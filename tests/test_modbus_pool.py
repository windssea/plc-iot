"""Real loopback socket ownership and unit/timeout isolation; no external I/O."""
import asyncio
from dataclasses import replace
import unittest

import test_modbus as fixtures
from plcnext_iot.drivers.modbus import ModbusConnectionPool, ModbusReader, ReadError
from plcnext_iot.devices.modbus import ModbusRunner
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.points.samples import SampleBuffer


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def handle(self, reader, writer):
        self.accepted += 1
        await fixtures.ModbusTests.handle(self, reader, writer)

    async def asyncSetUp(self):
        self.accepted = 0
        await fixtures.ModbusTests.asyncSetUp(self)
        self.reader.close()
        self.pool = ModbusConnectionPool()
        self.owners = []
        self.reader = self.owner()

    async def asyncTearDown(self):
        for reader in self.owners:
            reader.close()
        await fixtures.ModbusTests.asyncTearDown(self)
        self.assertFalse(self.pool._entries)

    def owner(self, **changes):
        reader = ModbusReader(replace(self.device.connection, **changes), self.pool)
        self.owners.append(reader)
        return reader

    async def wait_requests(self, count):
        async with asyncio.timeout(2):
            while len(self.requests) < count:
                await asyncio.sleep(.005)

    async def test_units_and_addresses_share_one_serial_connection(self):
        second = self.owner(unit_id=2)
        point = self.device.points[0]
        other = replace(point, modbus=replace(point.modbus, address=7))
        self.gate = asyncio.Event()
        one = asyncio.create_task(self.reader.read(point))
        await self.wait_requests(1)
        two = asyncio.create_task(second.read(other))
        await asyncio.sleep(.02)
        self.assertEqual(len(self.requests), 1)
        self.gate.set()
        self.assertEqual(await one, 230)
        self.assertEqual(await two, 230)
        self.assertEqual([u for u, _ in self.requests], [1, 2])
        self.assertEqual(self.requests[1][1][1:3], b'\x00\x07')
        self.assertEqual(self.accepted, 1)
        self.reader.close()
        self.reader.close()
        self.assertEqual(await second.read(other), 230)
        self.assertEqual(self.accepted, 1)

    async def test_owner_timeout_discards_socket_before_next_unit(self):
        self.reader.close()
        self.reader = self.owner(request_timeout_ms=40)
        second = self.owner(unit_id=2, request_timeout_ms=400)
        self.gate = asyncio.Event()
        with self.assertRaises(ReadError) as error:
            await self.reader.read(self.device.points[0])
        self.assertEqual(error.exception.quality, 'BAD_TIMEOUT')
        pending = asyncio.create_task(second.read(self.device.points[0]))
        await self.wait_requests(2)
        await asyncio.sleep(.08)  # Longer than the previous owner's deadline.
        self.reply = bytes.fromhex('03020064')
        self.gate.set()
        self.assertEqual(await pending, 10)
        self.assertEqual(self.accepted, 2)
        self.assertEqual([u for u, _ in self.requests], [1, 2])

    async def test_cancel_active_request_reconnects_for_sibling(self):
        second = self.owner(unit_id=2)
        self.gate = asyncio.Event()
        one = asyncio.create_task(self.reader.read(self.device.points[0]))
        await self.wait_requests(1)
        two = asyncio.create_task(second.read(self.device.points[0]))
        one.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await one
        await self.wait_requests(2)
        self.gate.set()
        self.assertEqual(await two, 230)
        self.assertEqual(self.accepted, 2)

    async def test_cancel_lock_waiter_preserves_active_socket(self):
        second = self.owner(unit_id=2)
        self.gate = asyncio.Event()
        one = asyncio.create_task(self.reader.read(self.device.points[0]))
        await self.wait_requests(1)
        two = asyncio.create_task(second.read(self.device.points[0]))
        await asyncio.sleep(.01)
        two.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await two
        second.close()
        self.assertTrue(self.reader._client.connected)
        self.gate.set()
        self.assertEqual(await one, 230)
        self.assertEqual(self.accepted, 1)

    async def test_released_waiter_cannot_reopen_or_read(self):
        second = self.owner(unit_id=2)
        self.gate = asyncio.Event()
        one = asyncio.create_task(self.reader.read(self.device.points[0]))
        await self.wait_requests(1)
        two = asyncio.create_task(second.read(self.device.points[0]))
        await asyncio.sleep(.01)
        second.close()
        self.gate.set()
        self.assertEqual(await one, 230)
        with self.assertRaises(ReadError):
            await two
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.accepted, 1)

    async def test_protocol_error_does_not_drop_sibling_socket(self):
        second = self.owner(unit_id=2)
        self.reply = bytes.fromhex('8302')
        with self.assertRaises(ReadError) as error:
            await self.reader.read(self.device.points[0])
        self.assertEqual(error.exception.exception_code, 2)
        self.reply = bytes.fromhex('030208fc')
        self.assertEqual(await second.read(self.device.points[0]), 230)
        self.assertEqual(self.accepted, 1)

    async def test_different_endpoints_and_runtimes_never_share_entries(self):
        other = self.owner(port=self.device.connection.port + 1)
        self.assertIsNot(self.reader._endpoint, other._endpoint)
        elsewhere = ModbusReader(self.device.connection, ModbusConnectionPool())
        try:
            self.assertIsNot(self.reader._endpoint, elsewhere._endpoint)
        finally:
            elsewhere.close()
        upper = self.owner(host=self.device.connection.host.upper())
        self.assertIs(self.reader._endpoint, upper._endpoint)

    async def test_pool_bound_and_last_release_discard_entries(self):
        pool = ModbusConnectionPool(1)
        a = ModbusReader(self.device.connection, pool)
        b = ModbusReader(replace(self.device.connection, unit_id=2), pool)
        with self.assertRaises(RuntimeError):
            ModbusReader(replace(self.device.connection, port=123), pool)
        a.close()
        self.assertEqual(len(pool._entries), 1)
        b.close()
        self.assertFalse(pool._entries)
        for capacity in (0, 51, True):
            with self.assertRaises(ValueError):
                ModbusConnectionPool(capacity)

    async def test_runtime_configuration_switch_rollback_and_shutdown_release(self):
        samples = SampleBuffer()
        runtime = DeviceRuntime(lambda d: ModbusRunner(d, runtime, samples))
        base = self.device
        second = replace(base, device_id='second', connection=replace(base.connection, unit_id=2))
        from plcnext_iot.config.models import ConfigSnapshot
        import json
        snapshot = ConfigSnapshot.parse(json.dumps(self.wire).encode(), 'PLC-01')
        snapshot = replace(snapshot, devices=(base, second))
        transition = runtime.prepare(snapshot)
        await transition.activate()
        transition.publish()
        try:
            await self.wait_requests(2)
            self.assertEqual(self.accepted, 1)
            new = replace(base, connection=replace(base.connection, unit_id=3))
            candidate = replace(snapshot, config_version=11, devices=(new, second))
            transition = runtime.prepare(candidate)
            await transition.activate()
            # Roll back before publication, as config persistence failure does.
            await transition.rollback()
            self.assertEqual(runtime.snapshot.config_version, snapshot.config_version)
            self.assertEqual(next(iter(runtime.modbus_connections._entries.values())).references, 2)
            transition = runtime.prepare(candidate)
            await transition.activate()
            transition.publish()
            await self.wait_requests(3)
            self.assertEqual(self.requests[-1][0], 3)
            self.assertEqual(self.accepted, 1)
            self.assertFalse(any(r._task.done() for r in runtime.runners.values()))
        finally:
            await runtime.shutdown(2)
        self.assertFalse(runtime.modbus_connections._entries)


if __name__ == '__main__':
    unittest.main()
