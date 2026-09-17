import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.points.read_plan import plan_reads
from plcnext_iot.devices.budget import ReadBudget
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.devices.modbus import ModbusRunner
from plcnext_iot.points.samples import Sample, SampleBuffer
from plcnext_iot.messaging.health import HealthPublisher
from plcnext_iot.contracts import validate_message


def config():
    return ConfigSnapshot.parse((ROOT/'contracts/examples/valid/config-set.json').read_bytes(), 'PLC-01')


def point(base, address, **kw):
    return replace(base, point_id=kw.pop('point_id', 'p'+str(address)), modbus=replace(base.modbus, address=address), **kw)


class PlanTests(unittest.TestCase):
    def test_merge_overlap_and_no_holes(self):
        p = config().devices[0].points[0]
        blocks = plan_reads([point(p, 0), point(p, 1), point(p, 1, point_id='overlap')])
        self.assertEqual([(b.address, b.count) for b in blocks], [(0, 2)])
        self.assertEqual(len(blocks[0].points), 3)
        self.assertEqual(len(plan_reads([point(p, 0), point(p, 2)])), 2)

    def test_group_period_and_area(self):
        p = config().devices[0].points[0]
        self.assertEqual(len(plan_reads([point(p, 0), point(p, 1, poll_interval_ms=p.poll_interval_ms*2)])), 2)
        self.assertEqual(len(plan_reads([point(p, 0), replace(point(p, 1), modbus=replace(p.modbus, area='input_register'))])), 2)

    def test_limits_and_no_split(self):
        p = config().devices[0].points[0]
        points = [point(p, i) for i in range(124)] + [point(p, 124, data_type='float32')]
        self.assertEqual([b.count for b in plan_reads(points)], [124, 2])
        bit = replace(p, data_type='bool', modbus=replace(p.modbus, area='coil'))
        self.assertEqual([b.count for b in plan_reads([point(bit, i) for i in range(2001)])], [2000, 1])


class BudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_backoff_wait_does_not_postpone_reconnection(self):
        from unittest.mock import patch
        from plcnext_iot.drivers.modbus import ReadError
        snapshot = config()
        device = snapshot.devices[0]
        device = replace(device, points=tuple(replace(p, poll_interval_ms=20) for p in device.points))
        runtime = DeviceRuntime()
        runtime.snapshot = replace(snapshot, devices=(device,))
        samples = SampleBuffer()
        runner = ModbusRunner(device, runtime, samples)
        runtime._runners[device.device_id] = runner
        class Reader:
            calls = 0
            def __init__(self, connection): pass
            def close(self): pass
            async def read_block(self, block, budget):
                self.calls += 1
                if self.calls == 1:
                    raise ReadError('BAD_CONNECTION')
                return {p.point_id: (230, 'GOOD') for p in block.points}, 0
        with patch('plcnext_iot.devices.modbus.ModbusReader', Reader):
            await runner.start()
            try:
                async with asyncio.timeout(1.5):
                    while (await samples.get()).quality != 'GOOD':
                        pass
                self.assertEqual(runner._reader.calls, 2)
            finally:
                await runner.stop()

    async def test_same_endpoint_wait_does_not_block_other_endpoint(self):
        budget = ReadBudget(2)
        deadline = asyncio.get_running_loop().time()+1
        async with budget.acquire(('a', 1), deadline) as first:
            self.assertTrue(first)
            async def wait_same():
                async with budget.acquire(('a', 1), asyncio.get_running_loop().time()+0.04) as admitted:
                    return admitted
            task = asyncio.create_task(wait_same())
            await asyncio.sleep(0)
            async with budget.acquire(('b', 1), deadline) as other:
                self.assertTrue(other)
            self.assertFalse(await task)
        self.assertEqual(budget.skipped, 1)
        self.assertEqual(budget._endpoints, {})

    async def test_global_bound_and_cancel_releases(self):
        budget = ReadBudget(1)
        deadline = asyncio.get_running_loop().time()+1
        async with budget.acquire(('a', 1), deadline):
            async def wait():
                async with budget.acquire(('b', 1), deadline):
                    self.fail('global slot overcommitted')
            task = asyncio.create_task(wait())
            await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        async with budget.acquire(('b', 1), deadline) as admitted:
            self.assertTrue(admitted)
        self.assertEqual(budget._endpoints, {})


class HealthTests(unittest.TestCase):
    def setUp(self):
        self.config = config()
        self.device = self.config.devices[0]
        self.runtime = DeviceRuntime()
        self.runtime.snapshot = self.config
        self.runner = ModbusRunner(self.device, self.runtime, SampleBuffer())
        self.runtime._runners[self.device.device_id] = self.runner
        self.agent = SimpleNamespace(runtime=self.runtime, active_config_version=self.config.config_version,
            state='RUNNING', settings=SimpleNamespace(gateway_id='PLC-01'))
        self.health = HealthPublisher(self.agent)

    def test_states_and_heartbeat_contract(self):
        p = self.device.points[0]
        self.assertEqual(self.runner.diagnostics(self.device, 10)[0]['status'], 'CONNECTING')
        for quality, expected in [('GOOD', 'ONLINE'), ('BAD_PROTOCOL', 'DEGRADED'), ('BAD_TIMEOUT', 'OFFLINE')]:
            self.runner.latest[p.point_id] = Sample(10, self.device.device_id, p.point_id, 1000, 1 if quality=='GOOD' else None, quality, 10)
            self.assertEqual(self.runner.diagnostics(self.device, 10)[0]['status'], expected)
            hb = self.health.snapshot('session-test', dict(batches=0,payloadBytes=0,droppedBatches=0), now=10, timestamp=1000)
            self.assertEqual(validate_message('heartbeat', json.dumps(hb).encode()), [])
        self.runner.latest[p.point_id] = Sample(10, self.device.device_id, p.point_id, 1000, 1, 'GOOD', 0)
        self.assertEqual(self.runner.diagnostics(self.device, 100000)[0]['status'], 'OFFLINE')

    def test_transitional_and_unknown_commit_suppressed(self):
        self.agent.state = 'APPLYING'
        self.assertIsNone(self.health.snapshot('s', {}))
        self.agent.state = 'RUNNING'
        self.agent.active_config_version = None
        self.assertIsNone(self.health.snapshot('s', {}))

    def test_disabled_counts_excluded(self):
        self.runtime.snapshot = replace(self.config, enabled=False)
        self.agent.state = 'DISABLED'
        hb = self.health.snapshot('s', dict(batches=0,payloadBytes=0,droppedBatches=0))
        self.assertEqual(hb['points'], dict(total=0,good=0,bad=0))
        self.assertEqual(hb['devices'][0]['status'], 'DISABLED')

