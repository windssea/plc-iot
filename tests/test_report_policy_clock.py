"""Pure reporting regression tests: no runtime, SQLite, sockets or MQTT."""
import asyncio
from dataclasses import FrozenInstanceError, replace
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.contracts import validate_message
from plcnext_iot.messaging.messages import encode
from plcnext_iot.points.samples import SampleBuffer
from plcnext_iot.core.sample_time import CaptureTime
from types import SimpleNamespace
from plcnext_iot.reporting.engine import ReportEngine
from plcnext_iot.reporting.service import TelemetryPipeline


def configured(points=1, max_batch_points=500):
    snapshot = ConfigSnapshot.parse((ROOT/'contracts/examples/valid/config-set.json').read_bytes(), 'PLC-01')
    point = replace(snapshot.devices[0].points[0], report_mode='cyclic',
                    report_interval_ms=1000, stale_after_ms=5000)
    device = replace(snapshot.devices[0], points=tuple(replace(point, point_id='p'+str(i)) for i in range(points)))
    snapshot = replace(snapshot, devices=(device,), report=replace(snapshot.report,
                        batch_interval_ms=1000, max_batch_points=max_batch_points))
    engine = ReportEngine('PLC-01', 'pure-regression')
    engine.test_samples = SampleBuffer(boot_context=engine.boot_context,gateway_id='PLC-01',
        time_provider=SimpleNamespace(read=lambda ms:CaptureTime(ms,True)))
    engine.configure(snapshot, 0)
    return engine, snapshot


def accept(engine, snapshot, tick):
    for point in snapshot.devices[0].points:
        sample=engine.test_samples.capture(snapshot.config_version,snapshot.devices[0].device_id,
            point.point_id,42.5,'GOOD',completed=float(tick),nominal=float(tick),origin=0)
        engine.accept(sample,float(tick))


class ReportPolicyClockTests(unittest.TestCase):
    def test_successful_enqueue_latency_does_not_skip_next_cycle(self):
        for delay in (.001, .016):
            with self.subTest(delay=delay):
                engine, snapshot = configured()
                observed = []
                for tick in range(1, 9):
                    accept(engine, snapshot, tick)
                    batches = engine.prepare(float(tick), tick*1000)
                    for batch in batches:
                        engine.committed(batch, tick+delay)
                    observed.extend(s.timestamp for b in batches for s in b.samples)
                self.assertEqual(observed, list(range(1000, 8001, 1000)))

    def test_split_batches_use_same_prepared_cycle_despite_serial_commits(self):
        engine, snapshot = configured(points=2, max_batch_points=1)
        accept(engine, snapshot, 1)
        batches = engine.prepare(1, 1000)
        self.assertEqual(len(batches), 2)
        for index, batch in enumerate(batches):
            engine.committed(batch, 1+.001+index*.015)
        accept(engine, snapshot, 2)
        self.assertEqual(sum(len(b.samples) for b in engine.prepare(2, 2000)), 2)

    def test_rejected_enqueue_does_not_advance_baseline(self):
        engine, snapshot = configured()
        accept(engine, snapshot, 1)
        self.assertEqual(len(engine.prepare(1, 1000)), 1)
        self.assertEqual(engine.baselines, {})
        accept(engine, snapshot, 2)
        batches = engine.prepare(2, 2000)
        self.assertEqual(len(batches), 2)
        self.assertEqual([b.samples[0].timestamp for b in batches], [1000,2000])

    def test_old_version_commit_does_not_advance_new_baseline(self):
        engine, snapshot = configured()
        accept(engine, snapshot, 1)
        batch = engine.prepare(1, 1000)[0]
        engine.configure(replace(snapshot, config_version=snapshot.config_version+1), 1.001)
        engine.committed(batch, 1.016)
        self.assertEqual(engine.baselines, {})

    def test_prepared_clock_is_immutable_and_absent_from_exact_wire_bytes(self):
        engine, snapshot = configured()
        accept(engine, snapshot, 1)
        batch = engine.prepare(1, 1000)[0]
        with self.assertRaises(FrozenInstanceError):
            batch.prepared_at = 999
        self.assertEqual(batch.prepared_at, 1)
        wire = json.loads(batch.payload)
        expected = {'schemaVersion': 1, 'gatewayId': 'PLC-01', 'timestamp': 1000,
                    'messageId': wire['messageId'], 'bootId': 'pure-regression', 'sequence': 1,
                    'configVersion': snapshot.config_version, 'values': [
                        {'deviceId': snapshot.devices[0].device_id, 'pointId': 'p0',
                         'timestamp': 1000, 'value': 42.5, 'quality': 'GOOD'}]}
        self.assertEqual(batch.payload, encode(expected))
        self.assertEqual(validate_message('data', batch.payload), [])

    def test_slow_commit_does_not_generate_catch_up_batches(self):
        engine, snapshot = configured()
        accept(engine, snapshot, 1)
        batch = engine.prepare(1, 1000)[0]
        engine.committed(batch, 3.5)
        accept(engine, snapshot, 3.5)
        batch=engine.prepare(3.5,3500)[0]
        self.assertEqual(engine.prepare(3.5,3500),[])
        engine.committed(batch,3.5)
        self.assertEqual(engine.prepare(3.5, 3500), [])
        self.assertEqual(engine.prepare(3.6, 3600), [])


class FakeClock:
    def __init__(self, now):
        self.now = now

    def time(self):
        return self.now


class FakeStorage:
    def __init__(self, engine, clock, outcome=True):
        self.engine, self.clock, self.outcome = engine, clock, outcome
        self.payloads = []
        self.baselines_before_completion = []

    async def call(self, method, payload, _wall):
        assert method == 'enqueue'
        self.payloads.append(payload)
        # Give the real _flush an actual suspension without any service I/O.
        await asyncio.sleep(0)
        self.baselines_before_completion.append(dict(self.engine.baselines))
        self.clock.now += .016
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


class PipelinePolicyClockTests(unittest.IsolatedAsyncioTestCase):
    def pipeline(self, outcome=True):
        # Exercise the production _flush without constructing StoreWorker or
        # starting the collection/reporting/send loops.
        pipeline = object.__new__(TelemetryPipeline)
        pipeline.engine, snapshot = configured()
        clock = FakeClock(1)
        pipeline.storage = FakeStorage(pipeline.engine, clock, outcome)
        return pipeline, snapshot, clock

    async def test_flush_commits_only_after_success_and_next_cycle_is_not_skipped(self):
        pipeline, snapshot, clock = self.pipeline()
        accept(pipeline.engine, snapshot, 1)
        with patch('plcnext_iot.reporting.service.asyncio.get_running_loop', return_value=clock):
            await pipeline._flush()
            self.assertEqual(pipeline.storage.baselines_before_completion, [{}])
            self.assertEqual(next(iter(pipeline.engine.baselines.values()))[1], 1)
            clock.now = 2
            accept(pipeline.engine, snapshot, 2)
            await pipeline._flush()
        self.assertEqual([json.loads(p)['values'][0]['timestamp'] for p in pipeline.storage.payloads],
                         [1000, 2000])

    async def test_flush_false_does_not_advance(self):
        pipeline, snapshot, clock = self.pipeline(False)
        accept(pipeline.engine, snapshot, 1)
        with patch('plcnext_iot.reporting.service.asyncio.get_running_loop', return_value=clock):
            await pipeline._flush()
        self.assertEqual(pipeline.engine.baselines, {})

    async def test_flush_exception_does_not_advance(self):
        pipeline, snapshot, clock = self.pipeline(RuntimeError('fake enqueue failed'))
        accept(pipeline.engine, snapshot, 1)
        with patch('plcnext_iot.reporting.service.asyncio.get_running_loop', return_value=clock):
            with self.assertRaisesRegex(RuntimeError, 'fake enqueue failed'):
                await pipeline._flush()
        self.assertEqual(pipeline.engine.baselines, {})


if __name__ == '__main__':
    unittest.main()
