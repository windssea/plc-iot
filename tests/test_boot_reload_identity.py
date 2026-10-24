"""Process boot identity regression tests, without sockets or a SQLite store."""
import asyncio
from contextlib import redirect_stdout
from dataclasses import replace
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))
from tools import appliance
from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.contracts import validate_message
from plcnext_iot.messaging.health import HealthPublisher
from plcnext_iot.points.samples import Sample
from plcnext_iot.reporting.engine import ReportEngine
from plcnext_iot.core.boot import MAX_SEQUENCE, ProcessBootContext
from plcnext_iot.reporting.service import TelemetryPipeline
from plcnext_iot.points.samples import SampleBuffer
from plcnext_iot.core.sample_time import CaptureTime


def configured(engine, now=0, points=2, version=10):
    config = ConfigSnapshot.parse((ROOT/'contracts/examples/valid/config-set.json').read_bytes(), 'PLC-01')
    point = config.devices[0].points[0]
    device = replace(config.devices[0], points=tuple(replace(point, point_id='p'+str(i)) for i in range(points)))
    config = replace(config, config_version=version, devices=(device,),
                     report=replace(config.report, max_batch_points=1))
    engine.configure(config, now)
    samples=SampleBuffer(boot_context=engine.boot_context,gateway_id='PLC-01',
        time_provider=SimpleNamespace(read=lambda ms:CaptureTime(1000+ms,True)))
    for point in device.points:
        engine.accept(samples.capture(version,device.device_id,point.point_id,42.5,'GOOD',
                      completed=now,nominal=now,origin=now),now)
    return config


class ApplianceReloadTests(unittest.IsolatedAsyncioTestCase):
    async def supervise_three_runs(self):
        stop = asyncio.Event()
        server = SimpleNamespace(value={'configured': True}, change_seq=0, applied_seq=0)
        store = SimpleNamespace(write_ca=lambda value: None,
                                settings=lambda value: (SimpleNamespace(gateway_id='PLC-01'), object()))
        batches, heartbeats, starts, contexts = [], [], [], []
        clock = SimpleNamespace(now=100)

        async def fake_run(settings, configs, seconds, driver, mqtt, quiet, run_stop, boot_id, *, boot_context=None):
            index = len(heartbeats)
            clock.now = 100 + index*10
            kwargs = {'boot_context': boot_context} if boot_context is not None else {}
            engine = ReportEngine(settings.gateway_id, boot_id, **kwargs)
            config = configured(engine, index*10)
            batches.extend(engine.prepare(index*10, 1000 + index*10000, force=True))
            agent = SimpleNamespace(runtime=SimpleNamespace(snapshot=config, runners={}),
                                    active_config_version=config.config_version, state='RUNNING', settings=settings)
            health = HealthPublisher(agent, SimpleNamespace(engine=engine))
            starts.append(health.started)
            contexts.append(boot_context)
            heartbeat = health.snapshot('session-'+str(index), dict(batches=0, payloadBytes=0, droppedBatches=0),
                                        now=starts[0] + index*10 + 7, timestamp=1000 + index*10000)
            heartbeats.append(heartbeat)
            if index < 2:
                server.change_seq += 1
            else:
                stop.set()
            return 0

        with patch.object(appliance, 'run', fake_run), patch.object(appliance, 'RELOAD_SETTLE_SECONDS', 0), \
                patch('plcnext_iot.messaging.health.time', SimpleNamespace(monotonic=lambda: clock.now)), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(await appliance._supervise(store, server, stop), 0)
        for batch in batches:
            self.assertEqual(validate_message('data', batch.payload), [])
        for heartbeat in heartbeats:
            self.assertEqual(validate_message('heartbeat', json.dumps(heartbeat).encode()), [])
        return batches, heartbeats, starts, contexts

    async def test_same_appliance_boot_sequence_continues_across_two_reload_cycles(self):
        batches, heartbeats, _, contexts = await self.supervise_three_runs()
        wire = [json.loads(batch.payload) for batch in batches]
        self.assertEqual([batch['sequence'] for batch in wire], [1, 2, 3, 4, 5, 6])
        self.assertEqual(len({batch['bootId'] for batch in wire}), 1)
        self.assertEqual({batch['bootId'] for batch in wire}, {h['bootId'] for h in heartbeats})
        self.assertEqual(len({h['sessionId'] for h in heartbeats}), 3)
        self.assertIsNotNone(contexts[0])
        self.assertTrue(all(context is contexts[0] for context in contexts))

    async def test_same_appliance_boot_uptime_includes_elapsed_reload_time(self):
        _, heartbeats, starts, _ = await self.supervise_three_runs()
        self.assertEqual([h['uptimeSeconds'] for h in heartbeats], [7, 17, 27])
        self.assertEqual(starts, [starts[0]]*3)


class BootContextTests(unittest.TestCase):
    def test_new_process_has_new_boot_and_starts_its_own_sequence_at_one(self):
        first, second = ProcessBootContext(), ProcessBootContext()
        self.assertNotEqual(first.boot_id, second.boot_id)
        for context in (first, second):
            engine = ReportEngine('PLC-01', context.boot_id, boot_context=context)
            configured(engine)
            self.assertEqual(json.loads(engine.prepare(0, 1000, force=True)[0].payload)['sequence'], 1)

    def test_two_argument_engine_api_keeps_independent_default_contexts(self):
        for _ in range(2):
            engine = ReportEngine('PLC-01', 'existing-caller-boot')
            configured(engine)
            self.assertEqual(engine.sequence, 0)
            self.assertEqual(json.loads(engine.prepare(0, 1000, force=True)[0].payload)['sequence'], 1)

    def test_sequence_keeps_increasing_but_reload_policy_caches_are_fresh(self):
        context = ProcessBootContext('reload-policy', started_at=100)
        first = ReportEngine('PLC-01', context.boot_id, boot_context=context)
        configured(first, points=1)
        batch = first.prepare(0, 1000, force=True)[0]
        first.committed(batch, 0)
        second = ReportEngine('PLC-01', context.boot_id, boot_context=context)
        next_config = configured(second, now=1, points=1, version=11)
        self.assertEqual(second.baselines, {})
        self.assertEqual(first.sequence, 1)
        second.committed(batch, 1)
        self.assertEqual(second.baselines, {})
        self.assertFalse(second.accept(batch.samples[0], 1))
        next_batch = second.prepare(1, 2000, force=True)[0]
        wire = json.loads(next_batch.payload)
        self.assertEqual((wire['bootId'], wire['sequence'], wire['configVersion']),
                         (context.boot_id, 2, next_config.config_version))
        self.assertEqual(json.loads(batch.payload)['sequence'], 1)
        self.assertEqual(first.baselines[('meter-A', 'p0')][0], batch.samples[0])

    def test_exhaustion_does_not_emit_an_invalid_or_wrapped_sequence(self):
        context = ProcessBootContext('bounded-sequence')
        context._sequence = MAX_SEQUENCE-1
        engine = ReportEngine('PLC-01', context.boot_id, boot_context=context)
        configured(engine, points=1)
        last = engine.prepare(0, 1000, force=True)[0]
        self.assertEqual(json.loads(last.payload)['sequence'], MAX_SEQUENCE)
        self.assertEqual(validate_message('data', last.payload), [])
        self.assertIs(engine.prepare(1,2000,force=True)[0],last)
        engine.committed(last,1)
        configured(engine,now=1,points=1,version=11)
        with self.assertRaisesRegex(ValueError, 'sequence exhausted'):
            engine.prepare(1, 2000, force=True)
        self.assertEqual(context.sequence, MAX_SEQUENCE)
        with self.assertRaisesRegex(ValueError, 'sequence exhausted'):
            context.next_sequence()

    def test_explicit_mismatched_identity_is_rejected_before_storage_construction(self):
        context = ProcessBootContext('boot-a')
        with self.assertRaisesRegex(ValueError, 'boot identity'):
            ReportEngine('PLC-01', 'boot-b', boot_context=context)
        with patch('plcnext_iot.reporting.service.StoreWorker') as storage:
            with self.assertRaisesRegex(ValueError, 'boot identity'):
                TelemetryPipeline(None, None, Path('unused'), 'PLC-01', boot_id='boot-b', boot_context=context)
            storage.assert_not_called()

    def test_health_without_telemetry_reuses_the_explicit_context_and_monotonic_origin(self):
        clock = SimpleNamespace(monotonic=lambda: 100)
        with patch('plcnext_iot.core.boot.time', clock):
            context = ProcessBootContext('health-only')
        agent = SimpleNamespace(runtime=SimpleNamespace(snapshot=None), active_config_version=0,
                                state='WAITING_CONFIG', settings=SimpleNamespace(gateway_id='PLC-01'))
        values = []
        for now, session in ((107.9, 'session-a'), (127.1, 'session-b')):
            health = HealthPublisher(agent, boot_context=context)
            heartbeat = health.snapshot(session, dict(batches=0, payloadBytes=0, droppedBatches=0), now=now, timestamp=1000)
            self.assertEqual((heartbeat['bootId'], heartbeat['sessionId']), (context.boot_id, session))
            self.assertEqual(validate_message('heartbeat', json.dumps(heartbeat).encode()), [])
            values.append(heartbeat['uptimeSeconds'])
        self.assertEqual(values, [7, 27])


class FakeStorage:
    def __init__(self, outcomes=(True,)):
        self.outcomes = iter(outcomes)
        self.attempts, self.persisted = [], []

    async def call(self, method, *args):
        if method == 'enqueue':
            self.attempts.append(args[0])
            result = next(self.outcomes, True)
            if result:
                self.persisted.append(args[0])
            return result
        if method == 'stats':
            return {'pending': len(self.persisted), 'dead': 0}
        raise AssertionError('Unexpected storage operation: '+method)

    async def close(self):
        pass


class PipelineBootTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_enqueue_keeps_sequence_and_exact_frozen_payload(self):
        context = ProcessBootContext('durable-reload')
        storage = FakeStorage((False, True, True))
        runtime = SimpleNamespace(snapshot=None)
        clock = SimpleNamespace(now=0, time=lambda: clock.now)
        with patch('plcnext_iot.reporting.service.StoreWorker', return_value=storage):
            first = TelemetryPipeline(runtime, SampleBuffer(), Path('unused'), 'PLC-01', boot_context=context)
            configured(first.engine, points=1)
            with patch('plcnext_iot.reporting.service.asyncio.get_running_loop', return_value=clock):
                await first._flush(force=True)
                self.assertEqual(first.engine.baselines, {})
                clock.now = 1
                await first._flush(force=True)
            original = storage.persisted[0]
            self.assertEqual(json.loads(original)['sequence'], 1)
            self.assertEqual(len(first.engine.baselines), 1)
            second = TelemetryPipeline(runtime, SampleBuffer(), Path('unused'), 'PLC-01', boot_context=context)
            configured(second.engine, now=2, points=1, version=11)
            with patch('plcnext_iot.reporting.service.asyncio.get_running_loop', return_value=clock):
                clock.now = 2
                await second._flush(force=True)
        self.assertEqual([json.loads(payload)['sequence'] for payload in storage.attempts], [1, 1, 2])
        self.assertEqual([json.loads(payload)['sequence'] for payload in storage.persisted], [1, 2])
        self.assertIs(storage.persisted[0], original)
        self.assertEqual(json.loads(original)['configVersion'], 10)

    async def test_actual_agent_run_passes_one_context_to_pipeline_and_health_across_reload(self):
        from tools import agent as agent_tool
        context = ProcessBootContext('actual-run', started_at=100)
        pipelines, heartbeats = [], []

        class FakeAgent:
            def __init__(self, settings, runtime):
                self.settings, self.runtime = settings, runtime
                self.state, self.last_error = 'NEW', None
                self.active_config_version = 10
            async def start(self):
                self.state = 'RUNNING'
            async def wait_failed(self):
                await asyncio.Event().wait()
            async def close(self):
                self.state = 'STOPPED'

        class Pipeline(TelemetryPipeline):
            async def start(self):
                self.runtime.snapshot = configured(self.engine,now=len(pipelines)*10)
                pipelines.append(self)
                await self._flush(force=True)

        class FakeMqtt:
            def __init__(self, agent, settings, *, telemetry):
                self.health = HealthPublisher(agent, telemetry)
                self.connected, self.last_error = True, None
            async def start(self):
                index = len(heartbeats)
                heartbeats.append(self.health.snapshot('actual-session-'+str(index),
                    dict(batches=0, payloadBytes=0, droppedBatches=0), now=112+index*10, timestamp=1000))
            async def wait_failed(self):
                await asyncio.Event().wait()
            async def close(self):
                pass

        settings = SimpleNamespace(gateway_id='PLC-01', data_directory=Path('unused'))
        with patch.object(agent_tool, 'AgentLifecycle', FakeAgent), \
                patch('plcnext_iot.reporting.service.TelemetryPipeline', Pipeline), \
                patch('plcnext_iot.reporting.service.StoreWorker', side_effect=lambda *args: FakeStorage()), \
                patch('plcnext_iot.messaging.config_service.MqttConfigService', FakeMqtt), redirect_stdout(io.StringIO()):
            for _ in range(2):
                self.assertEqual(await agent_tool.run(settings, [], 0, mqtt_settings=object(),
                    stop_event=asyncio.Event(), boot_id=context.boot_id, boot_context=context), 0)
            self.assertEqual(await agent_tool.run(settings, [], 0, mqtt_settings=object(),
                stop_event=asyncio.Event()), 0)
        self.assertTrue(all(p.engine.boot_context is context for p in pipelines[:2]))
        self.assertEqual([json.loads(payload)['sequence'] for p in pipelines[:2] for payload in p.storage.persisted], [1, 2, 3, 4])
        self.assertEqual([h['uptimeSeconds'] for h in heartbeats[:2]], [12, 22])
        self.assertEqual(len({h['sessionId'] for h in heartbeats}), 3)
        self.assertTrue(all(h['bootId'] == context.boot_id for h in heartbeats[:2]))
        self.assertIsNot(pipelines[2].engine.boot_context, context)
        self.assertNotEqual(pipelines[2].engine.boot_id, context.boot_id)
        self.assertEqual([json.loads(payload)['sequence'] for payload in pipelines[2].storage.persisted], [1, 2])
