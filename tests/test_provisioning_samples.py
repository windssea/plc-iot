"""Live DTO fences with real lifecycle transactions; no external services."""
import asyncio
from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))
from plcnext_iot.core.lifecycle import AgentLifecycle
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.points.samples import Sample
from plcnext_iot.provisioning.samples import read_samples
from plcnext_iot.provisioning.server import SetupServer
from plcnext_iot.provisioning.store import SetupStore
from tools import agent as agent_tool, appliance


def wire(version=10, address=0):
    value = json.loads((ROOT/'contracts/examples/valid/config-set.json').read_text())
    value.update(configVersion=version, messageId=f'config-{version}')
    value['config']['devices'][0]['points'][0]['modbus']['address'] = address
    return json.dumps(value).encode()


class Runner:
    def __init__(self, device):
        self.device, self.latest, self._task = device, {}, None
        self.gate = None

    async def start(self):
        self.latest.clear()
        self._task = asyncio.create_task(asyncio.Event().wait())
        if self.gate is not None:
            await self.gate.wait()

    async def stop(self):
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None


class LiveSamplesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.runners = []
        self.gate = None
        def factory(device):
            runner = Runner(device)
            runner.gate = self.gate
            self.runners.append(runner)
            return runner
        self.runtime = DeviceRuntime(factory)
        self.agent = AgentLifecycle(AgentSettings('PLC-01', self.directory), self.runtime)
        await self.agent.start()
        self.addAsyncCleanup(self.agent.close)
        await self.agent.submit(wire())
        self.server = SetupServer(SetupStore(self.directory), dict(gatewayId='PLC-01'))
        self.server.set_runtime(self.agent)

    def shown(self, version=10, device_id='meter-A', now=10):
        return read_samples(self.agent, 'PLC-01', device_id, version, now)

    def put(self, value=0, quality='GOOD', version=10, captured=9, timestamp=1000):
        self.runtime.runners['meter-A'].latest['voltage'] = Sample(
            version, 'meter-A', 'voltage', timestamp, value, quality, captured)

    async def test_zero_false_and_null_are_not_truthy_filtered(self):
        self.put(0)
        self.assertEqual(self.shown()[1]['points'][0],
                         dict(pointId='voltage', value=0, timestamp=1000, quality='GOOD'))
        config = self.runtime.snapshot
        device = replace(config.devices[0], points=(replace(config.devices[0].points[0], data_type='bool'),))
        self.runtime.snapshot = replace(config, devices=(device,))
        self.put(False)
        self.assertIs(self.shown()[1]['points'][0]['value'], False)
        self.put(None, 'BAD_TIMEOUT')
        self.assertEqual(self.shown()[1]['points'][0]['quality'], 'BAD_TIMEOUT')
        self.assertIsNone(self.shown()[1]['points'][0]['value'])

    async def test_stale_boundary_uses_monotonic_clock_and_never_fabricates_timestamp(self):
        self.put(42, captured=5, timestamp=1000)
        self.assertEqual(self.shown(now=9.999)[1]['points'][0]['quality'], 'GOOD')
        row = self.shown(now=10)[1]['points'][0]
        self.assertEqual(row, dict(pointId='voltage', value=None, timestamp=1000, quality='STALE'))
        self.put(42, captured=None)
        self.assertEqual(self.shown()[1]['points'][0]['quality'], 'STALE')

    async def test_empty_disabled_task_stopped_and_bad_quality_do_not_show_old_good(self):
        self.assertEqual(self.shown()[1]['points'][0]['quality'], 'UNKNOWN')
        self.put(12, 'BAD_CONNECTION')
        self.assertIsNone(self.shown()[1]['points'][0]['value'])
        self.put(12)
        await self.runtime.runners['meter-A'].stop()
        self.assertEqual(self.shown()[1]['points'][0]['quality'], 'BAD_INTERNAL_ERROR')
        value = json.loads(wire(11))
        value['config']['enabled'] = False
        await self.agent.submit(json.dumps(value).encode())
        self.assertEqual(self.shown(11)[1]['points'][0],
                         dict(pointId='voltage', value=None, timestamp=None, quality='DISABLED'))

    async def test_task_exited_cannot_show_good(self):
        self.put(12)
        task = self.runtime.runners['meter-A']._task
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(self.shown()[1]['points'][0]['quality'], 'BAD_INTERNAL_ERROR')

    async def test_invalid_good_values_never_emit_non_json_numbers_or_wrong_types(self):
        for value in (float('nan'), float('inf'), 'password-value', True, None):
            self.put(value)
            result = self.shown()[1]
            self.assertEqual(result['points'][0]['quality'], 'BAD_TYPE_MISMATCH')
            self.assertIsNone(result['points'][0]['value'])
            json.dumps(result, allow_nan=False)

    async def test_version_identity_and_unavailable_state_fences(self):
        self.assertEqual(self.shown(9)[0], 409)
        self.assertEqual(self.shown(device_id='missing')[0], 404)
        for state in ('NEW', 'STARTING', 'APPLYING', 'FAILED', 'STOPPING', 'STOPPED', 'WAITING_CONFIG'):
            self.agent.state = state
            self.assertEqual(self.shown()[0], 503)
        self.agent.state = 'RUNNING'
        self.agent._durable_version = None
        self.assertEqual(self.shown()[0], 503)
        self.agent._durable_version = 10
        self.assertEqual(read_samples(self.agent, 'OTHER', 'meter-A', 10, 10)[0], 503)
        self.assertEqual(read_samples(None, 'PLC-01', 'meter-A', 10, 10)[0], 503)

    async def test_report_only_reused_runner_does_not_mix_old_samples_with_new_version(self):
        self.put(12)
        old = self.runtime.runners['meter-A']
        self.assertEqual((await self.agent.submit(wire(11))).status, 'APPLIED')
        self.assertIs(self.runtime.runners['meter-A'], old)
        row = self.shown(11)[1]['points'][0]
        self.assertEqual(row, dict(pointId='voltage', value=None, timestamp=None, quality='UNKNOWN'))
        self.put(13, version=11)
        self.assertEqual(self.shown(11)[1]['points'][0]['value'], 13)

    async def test_candidate_activation_not_visible_and_replaced_runner_cache_not_exposed(self):
        self.put(12)
        old = self.runtime.runners['meter-A']
        self.gate = asyncio.Event()
        applying = asyncio.create_task(self.agent.submit(wire(11, 1)))
        try:
            async with asyncio.timeout(2):
                while len(self.runners) < 2 or self.runners[-1]._task is None:
                    await asyncio.sleep(0)
            candidate = self.runners[-1]
            candidate.latest['voltage'] = Sample(11, 'meter-A', 'voltage', 2000, 999, 'GOOD', 9)
            self.assertEqual(self.runtime.snapshot.config_version, 10)
            self.assertEqual(self.shown()[0], 503)
        finally:
            self.gate.set()
        self.assertEqual((await applying).status, 'APPLIED')
        self.assertIsNot(self.runtime.runners['meter-A'], old)
        self.assertEqual(self.shown(11)[1]['points'][0]['value'], 999)

    async def test_commit_failure_rolls_back_without_candidate_sample(self):
        from contextlib import closing
        import sqlite3
        with closing(sqlite3.connect(self.directory/'agent.db')) as conn:
            conn.execute("""CREATE TRIGGER reject_activation BEFORE UPDATE OF active_version ON metadata
                            BEGIN SELECT RAISE(ABORT,'write refused'); END""")
        result = await self.agent.submit(wire(11, 1))
        self.assertEqual(result.code, 'STORAGE_ERROR')
        self.assertEqual(self.shown(11)[0], 409)
        self.assertEqual(self.shown()[1]['points'][0]['quality'], 'UNKNOWN')

    async def test_request_only_projection_has_no_file_thread_protocol_work_or_cache_mutation(self):
        self.put(12)
        cached = dict(self.runtime.runners['meter-A'].latest)
        agent_state = dict(vars(self.agent))
        with patch('plcnext_iot.provisioning.server.read_active', side_effect=AssertionError('SQLite read')), \
                patch('asyncio.to_thread', side_effect=AssertionError('thread work')), \
                patch('pathlib.Path.open', side_effect=AssertionError('file work')):
            for _ in range(20):
                code, _, result = await self.server.route(
                    'GET', '/api/devices/meter-A/samples?configVersion=10', {}, b'')
                self.assertEqual(code, 200)
                self.assertEqual(result['deviceId'], 'meter-A')
        self.assertEqual(self.runtime.runners['meter-A'].latest, cached)
        self.assertEqual(vars(self.agent), agent_state)
        self.assertEqual(set(result), {'gatewayId', 'configVersion', 'deviceId', 'points'})

    async def test_strict_path_and_required_version_validation(self):
        for suffix in ('', '?configVersion=0', '?configVersion=true', '?configVersion=10&x=1',
                       '?configVersion=10&configVersion=10', '?configVersion=10#fragment'):
            code, _, _ = await self.server.route('GET', '/api/devices/meter-A/samples'+suffix, {}, b'')
            self.assertEqual(code, 400)
        for ident in ('%2F', '%XX', '%252F', '%FF', 'x/y'):
            code, _, _ = await self.server.route('GET', f'/api/devices/{ident}/samples?configVersion=10', {}, b'')
            self.assertEqual(code, 404)

    async def test_device_named_samples_keeps_existing_detail_and_has_live_subroute(self):
        value = json.loads(wire(11))
        value['config']['devices'][0]['deviceId'] = 'samples'
        self.assertEqual((await self.agent.submit(json.dumps(value).encode())).status, 'APPLIED')
        code, _, detail = await self.server.route('GET', '/api/devices/samples?configVersion=11', {}, b'')
        self.assertEqual(code, 200)
        self.assertEqual(detail['device']['deviceId'], 'samples')
        code, _, live = await self.server.route('GET', '/api/devices/samples/samples?configVersion=11', {}, b'')
        self.assertEqual(code, 200)
        self.assertEqual(live['deviceId'], 'samples')
        self.assertEqual(live['points'][0]['quality'], 'UNKNOWN')

    async def test_run_observer_cleared_before_any_cleanup_await(self):
        events = []
        class Lifecycle:
            state, last_error = 'RUNNING', None
            active_config_version = 0
            async def start(self):
                pass
            async def wait_failed(self):
                await asyncio.Event().wait()
            async def close(self):
                events.append('close')
                self.state = 'STOPPED'
        with patch.object(agent_tool, 'AgentLifecycle', return_value=Lifecycle()), redirect_stdout(io.StringIO()):
            await agent_tool.run(AgentSettings('PLC-01', self.directory), [], 0,
                                 stop_event=asyncio.Event(),
                                 runtime_observer=lambda value: events.append('attach' if value else 'clear'))
        self.assertEqual(events, ['attach', 'clear', 'close'])

    async def test_fatal_and_shutdown_blocked_stop_never_expose_existing_good(self):
        self.put(12)
        runner = self.runtime.runners['meter-A']
        original_stop = runner.stop
        entered, release = asyncio.Event(), asyncio.Event()
        async def blocked_stop():
            entered.set()
            await release.wait()
            await original_stop()
        runner.stop = blocked_stop
        fatal = asyncio.create_task(self.agent._fatal('TEST_FAILURE'))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            self.assertEqual(self.agent.state, 'FAILED')
            self.assertEqual(self.runtime.snapshot.config_version, 10)
            self.assertEqual(self.shown()[0], 503)
        finally:
            release.set()
            await fatal
        self.assertIsNone(self.runtime.snapshot)

    async def test_supervisor_clears_borrowed_pointer_even_if_run_raises(self):
        stop = asyncio.Event()
        server = SimpleNamespace(value={}, change_seq=0, applied_seq=0)
        received = []
        server.set_runtime = received.append
        store = SimpleNamespace(write_ca=lambda _: None,
                                settings=lambda _: (self.agent.settings, object()))
        async def run_raises(*args, runtime_observer=None, **kwargs):
            runtime_observer(self.agent)
            raise OSError('run failed')
        with patch.object(appliance, 'run', run_raises), redirect_stdout(io.StringIO()):
            with self.assertRaises(OSError):
                await appliance._supervise(store, server, stop)
        self.assertEqual(received, [self.agent, None])
