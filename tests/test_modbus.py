import asyncio
from dataclasses import replace
import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from plcnext_iot.config.models import ConfigSnapshot


class ModbusTests(unittest.IsolatedAsyncioTestCase):
    async def test_contiguous_points_share_one_request(self):
        from plcnext_iot.points.read_plan import plan_reads
        p = self.device.points[0]
        second = replace(p, point_id='second', modbus=replace(p.modbus, address=p.modbus.address+1))
        self.reply = bytes.fromhex('030408fc0906')
        results, extra = await self.reader.read_block(plan_reads([p, second])[0])
        self.assertEqual(results[p.point_id], (230.0, 'GOOD'))
        self.assertEqual(results['second'], (231.0, 'GOOD'))
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.requests[0][1][-2:], b'\x00\x02')
        self.assertEqual(extra, 0)

    async def test_illegal_address_fallback_is_bounded(self):
        from plcnext_iot.points.read_plan import plan_reads
        p = self.device.points[0]
        points = [replace(p, point_id='p'+str(i), modbus=replace(p.modbus, address=i)) for i in range(8)]
        self.reply = bytes.fromhex('8302')
        results, extra = await self.reader.read_block(plan_reads(points)[0])
        self.assertEqual(extra, 4)
        self.assertEqual(len(self.requests), 5)
        self.assertEqual(len(results), 8)
        self.assertTrue(all(q == 'BAD_PROTOCOL' for _, q in results.values()))

    async def test_short_block_is_protocol_error_without_fallback(self):
        from plcnext_iot.points.read_plan import plan_reads
        p = self.device.points[0]
        second = replace(p, point_id='second', modbus=replace(p.modbus, address=p.modbus.address+1))
        with self.assertRaises(self.ReadError) as error:
            await self.reader.read_block(plan_reads([p, second])[0])
        self.assertEqual(error.exception.quality, 'BAD_PROTOCOL')
        self.assertEqual(len(self.requests), 1)

    async def asyncSetUp(self):
        self.assertTrue((ROOT / 'src/plcnext_iot/drivers/modbus.py').exists(), 'Modbus reader is not implemented')
        from plcnext_iot.drivers.modbus import ModbusReader, ReadError, decode
        self.Reader, self.ReadError, self.decode = ModbusReader, ReadError, decode
        self.wire = json.loads((ROOT / 'contracts/examples/valid/config-set.json').read_bytes())
        self.requests, self.connections, self.handlers = [], set(), set()
        self.reply = bytes.fromhex('030208fc')  # 2300 * 0.1 = 230 V
        self.gate = None
        self.drop = False
        self.server = await asyncio.start_server(self.handle, '127.0.0.1', 0)
        port = self.server.sockets[0].getsockname()[1]
        self.wire['config']['devices'][0]['connection'].update(host='127.0.0.1', port=port, requestTimeoutMs=100, connectTimeoutMs=500)
        self.device = ConfigSnapshot.parse(json.dumps(self.wire).encode(), 'PLC-01').devices[0]
        self.reader = self.Reader(self.device.connection)

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        self.connections.add(writer)
        try:
            while True:
                header = await reader.readexactly(7)
                tid, protocol, length, unit = struct.unpack('>HHHB', header)
                request = await reader.readexactly(length - 1)
                self.requests.append((unit, request))
                if self.drop:
                    break
                if self.gate:
                    await self.gate.wait()
                response = self.reply
                writer.write(struct.pack('>HHHB', tid, 0, len(response) + 1, unit) + response)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self.connections.discard(writer)
            writer.close()
            await writer.wait_closed()
            self.handlers.discard(task)

    async def asyncTearDown(self):
        if not hasattr(self, 'reader'):
            return
        self.reader.close()
        self.server.close()
        for task in list(self.handlers):
            task.cancel()
        await asyncio.gather(*self.handlers, return_exceptions=True)
        await self.server.wait_closed()

    async def test_reads_correct_unit_address_and_applies_scaling(self):
        self.assertEqual(await self.reader.read(self.device.points[0]), 230)
        self.assertEqual(self.requests, [(1, bytes.fromhex('0300000001'))])

    async def test_all_v1_types_and_orders_decode_known_wire_values(self):
        p = self.device.points[0]
        fixtures = [('int16','AB',[0xfffe],-2), ('uint16','BA',[0x3412],0x1234),
                    ('int32','ABCD',[0xffff,0xfffe],-2), ('uint32','ABCD',[0x1234,0x5678],0x12345678),
                    ('float32','ABCD',[0x4148,0],12.5), ('float32','BADC',[0x4841,0],12.5),
                    ('float32','CDAB',[0,0x4148],12.5), ('float32','DCBA',[0,0x4841],12.5)]
        for kind, order, words, want in fixtures:
            point = replace(p, data_type=kind, scale=1, offset=0, modbus=replace(p.modbus, byte_order=order))
            self.assertEqual(self.decode(point, words), want)
        point = replace(p, data_type='bool', scale=1, offset=0, modbus=replace(p.modbus, area='coil', byte_order=None))
        self.assertIs(self.decode(point, [True]), True)

    async def test_nonfinite_and_short_values_are_bad_decode(self):
        p = replace(self.device.points[0], data_type='float32', modbus=replace(self.device.points[0].modbus, byte_order='ABCD'))
        for words in ([0x7fc0,0], [0x7f80,0], [0]):
            with self.assertRaises(self.ReadError) as ctx:
                self.decode(p, words)
            self.assertEqual(ctx.exception.quality, 'BAD_DECODE')

    async def test_all_read_function_codes(self):
        for area, fc in [('coil',1),('discrete_input',2),('holding_register',3),('input_register',4)]:
            p = self.device.points[0]
            p = replace(p, data_type='bool' if fc < 3 else 'uint16', scale=1, offset=0,
                        modbus=replace(p.modbus, area=area, address=17, byte_order=None if fc < 3 else 'AB'))
            self.reply = bytes([fc, 1, 1]) if fc < 3 else bytes([fc, 2, 0, 42])
            self.assertEqual(await self.reader.read(p), True if fc < 3 else 42)
            self.assertEqual(self.requests[-1], (1, struct.pack('>BHH',fc,17,1)))

    async def test_protocol_exception_keeps_connection_and_recovers(self):
        self.reply = bytes.fromhex('8302')
        with self.assertRaises(self.ReadError) as ctx:
            await self.reader.read(self.device.points[0])
        self.assertEqual(ctx.exception.quality, 'BAD_PROTOCOL')
        self.reply = bytes.fromhex('030208fc')
        self.assertEqual(await self.reader.read(self.device.points[0]), 230)
        self.assertEqual(len(self.connections), 1)

    async def test_timeout_then_reconnect_does_not_consume_late_response(self):
        self.gate = asyncio.Event()
        with self.assertRaises(self.ReadError) as ctx:
            await self.reader.read(self.device.points[0])
        self.assertEqual(ctx.exception.quality, 'BAD_TIMEOUT')
        self.gate.set()
        self.gate = None
        self.reply = bytes.fromhex('03020064')
        self.assertEqual(await self.reader.read(self.device.points[0]), 10)

    async def test_connection_refusal_is_bad_connection(self):
        self.server.close()
        await self.server.wait_closed()
        with self.assertRaises(self.ReadError) as ctx:
            await self.reader.read(self.device.points[0])
        self.assertEqual(ctx.exception.quality, 'BAD_CONNECTION')

    async def test_peer_disconnect_during_request_is_connection_failure(self):
        self.drop = True
        with self.assertRaises(self.ReadError) as ctx:
            await self.reader.read(self.device.points[0])
        self.assertEqual(ctx.exception.quality, 'BAD_CONNECTION')

    async def test_cancel_during_request_closes_socket_and_propagates(self):
        self.gate = asyncio.Event()
        pending = asyncio.create_task(self.reader.read(self.device.points[0]))
        async with asyncio.timeout(2):
            while not self.requests:
                await asyncio.sleep(0.01)
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await pending
        self.gate.set()
        await asyncio.sleep(0.02)
        self.assertFalse(self.connections)

    async def test_configured_retry_is_bounded(self):
        self.reader.close()
        self.reader = self.Reader(replace(self.device.connection, retry_count=1))
        self.gate = asyncio.Event()
        with self.assertRaises(self.ReadError):
            await self.reader.read(self.device.points[0])
        self.assertEqual(len(self.requests), 2)

    async def test_sample_buffer_discards_oldest_and_counts_loss(self):
        from plcnext_iot.points.samples import Sample, SampleBuffer
        buffer = SampleBuffer(2)
        for value in (1, 2, 3):
            buffer.put(Sample(10, 'a', 'p', 1, value, 'GOOD'))
        self.assertEqual(buffer.dropped, 1)
        self.assertEqual((await buffer.get()).value, 2)
        self.assertEqual((await buffer.get()).value, 3)
        for capacity in (0, -1, True, 10001):
            with self.assertRaises(ValueError):
                SampleBuffer(capacity)

    async def test_cli_modbus_flag_emits_real_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bootstrap = root / 'bootstrap.json'
            bootstrap.write_text(json.dumps({'gatewayId':'PLC-01','dataDirectory':'data',
                'operationTimeoutSeconds':1,'queueCapacity':2}))
            config = root / 'config.json'
            config.write_text(json.dumps(self.wire))
            process = await asyncio.create_subprocess_exec(sys.executable, '-m', 'tools.agent',
                '--bootstrap', str(bootstrap), '--config', str(config), '--driver', 'modbus-tcp',
                '--run-seconds', '1.5', cwd=ROOT, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            try:
                out, err = await asyncio.wait_for(process.communicate(), 6)
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            self.assertEqual(process.returncode, 0, err.decode())
            events = [json.loads(line) for line in out.decode().splitlines()]
            samples = [e for e in events if e['event'] == 'sample']
            self.assertTrue(samples)
            self.assertEqual(samples[-1]['sample']['value'], 230)
            self.assertEqual(samples[-1]['sample']['quality'], 'GOOD')
            self.assertEqual(events[-1]['state'], 'STOPPED')

    async def test_lifecycle_collects_versioned_samples_and_stops_connections(self):
        self.assertTrue((ROOT / 'src/plcnext_iot/devices/modbus.py').exists(), 'Modbus runner is not implemented')
        from plcnext_iot.devices.modbus import ModbusRunner
        from plcnext_iot.points.samples import SampleBuffer
        from plcnext_iot.devices.runtime import DeviceRuntime
        from plcnext_iot.core.lifecycle import AgentLifecycle
        from plcnext_iot.core.settings import AgentSettings
        buffer = SampleBuffer(2)
        runtime = DeviceRuntime(lambda device: ModbusRunner(device, runtime, buffer))
        with tempfile.TemporaryDirectory() as directory:
            agent = AgentLifecycle(AgentSettings('PLC-01', Path(directory)), runtime)
            try:
                await agent.start()
                result = await agent.submit(json.dumps(self.wire).encode())
                self.assertEqual(result.status, 'APPLIED')
                sample = await asyncio.wait_for(buffer.get(), 2)
                self.assertEqual((sample.config_version, sample.device_id, sample.point_id, sample.value, sample.quality),
                                 (10, 'meter-A', 'voltage', 230, 'GOOD'))
                self.assertGreater(sample.timestamp, 0)
                self.reply = bytes.fromhex('8302')
                bad = await asyncio.wait_for(buffer.get(), 2)
                self.assertEqual((bad.value, bad.quality), (None, 'BAD_PROTOCOL'))
            finally:
                await agent.close()
            await asyncio.sleep(0.02)
            self.assertFalse(self.connections)

    async def test_report_only_switch_drops_inflight_old_version_sample(self):
        from plcnext_iot.devices.modbus import ModbusRunner
        from plcnext_iot.points.samples import SampleBuffer
        from plcnext_iot.devices.runtime import DeviceRuntime
        from plcnext_iot.core.lifecycle import AgentLifecycle
        from plcnext_iot.core.settings import AgentSettings
        self.wire['config']['devices'][0]['points'][0]['pollIntervalMs'] = 100
        self.wire['config']['devices'][0]['connection']['requestTimeoutMs'] = 1000
        buffer = SampleBuffer(20)
        runtime = DeviceRuntime(lambda device: ModbusRunner(device, runtime, buffer))
        with tempfile.TemporaryDirectory() as directory:
            agent = AgentLifecycle(AgentSettings('PLC-01', Path(directory)), runtime)
            try:
                await agent.start()
                self.assertEqual((await agent.submit(json.dumps(self.wire).encode())).status, 'APPLIED')
                await asyncio.wait_for(buffer.get(), 2)
                old = runtime.runners['meter-A']
                self.gate = asyncio.Event()
                previous = len(self.requests)
                async with asyncio.timeout(2):
                    while len(self.requests) == previous:
                        await asyncio.sleep(0.005)
                self.wire.update(configVersion=11, messageId='report-11')
                self.wire['config']['devices'][0]['points'][0]['reportIntervalMs'] = 6000
                self.assertEqual((await agent.submit(json.dumps(self.wire).encode())).status, 'APPLIED')
                self.assertIs(runtime.runners['meter-A'], old)
                self.gate.set()
                self.gate = None
                sample = await asyncio.wait_for(buffer.get(), 2)
                self.assertEqual(sample.config_version, 11)
                self.assertGreater(len(self.requests), previous + 1)
            finally:
                await agent.close()


if __name__ == '__main__':
    unittest.main()
