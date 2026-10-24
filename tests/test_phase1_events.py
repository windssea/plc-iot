"""I01/I02 acceptance. Pure/injected I/O only: no DB, broker or device."""
import asyncio
from dataclasses import FrozenInstanceError, replace
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.core.boot import MAX_SEQUENCE, ProcessBootContext
from plcnext_iot.core.sample_time import CaptureTime
from plcnext_iot.contracts import validate_message
from plcnext_iot.devices.runtime import DeviceRuntime, SampleContext
from plcnext_iot.points.events import PointEvents
from plcnext_iot.points.samples import SampleBuffer
from plcnext_iot.reporting.engine import ReportEngine
from plcnext_iot.reporting.service import TelemetryPipeline
from plcnext_iot.points.slots import skipped_after


class Clock:
    valid = True
    generation = 0
    def read(self, mono_ms):
        return CaptureTime(100000+mono_ms if self.valid else None, self.valid, 3, self.generation,
                           generation_started_mono_ms=getattr(self,'generation_started',None))


def setup_engine(mode='cyclic', deadband=0, report=5000, *, points=1, capacity=32768):
    snapshot = ConfigSnapshot.parse((ROOT/'contracts/examples/valid/config-set.json').read_bytes(), 'PLC-01')
    p = replace(snapshot.devices[0].points[0], report_mode=mode, deadband=deadband,
                report_interval_ms=report, stale_after_ms=3000)
    d = replace(snapshot.devices[0], points=tuple(replace(p,point_id='p'+str(i)) for i in range(points)))
    snapshot = replace(snapshot, devices=(d,))
    boot = ProcessBootContext('event-test')
    clock = Clock()
    samples = SampleBuffer(boot_context=boot, time_provider=clock,gateway_id='PLC-01')
    engine = ReportEngine('PLC-01', boot.boot_id, boot_context=boot, capacity=capacity)
    engine.configure(snapshot, 0)
    return engine, snapshot, samples, clock


def event(samples, value=10, quality='GOOD', q=0, completed=None, origin=0, *, point='p0', version=10, generation=0):
    return samples.capture(version, 'meter-A', point, value if quality=='GOOD' else None, quality,
        nominal=None if q is None else q/1000, completed=(completed if completed is not None else (q or 0)+10)/1000,
        origin=origin/1000, generation=generation, kind='QUALITY_STATE' if q is None else 'PHYSICAL_RESULT')


class SelectionVectors(unittest.TestCase):
    def test_shared_numeric_goldens_R01_R02_R03_R04_R05_R07_R09_R10_R14(self):
        vectors = json.loads((ROOT.parent/'PLCnext-Hub-Backend/tools/phase1/golden-cases.json').read_text())['selection']
        for v in vectors:
            with self.subTest(vector=v['id']):
                engine, _, samples, _ = setup_engine(v.get('mode','cyclic'),v.get('deadband',0))
                count = len(v.get('nominal',v.get('values',[])))
                observed = []
                for i in range(count):
                    q = v.get('nominal',[j*1000 for j in range(count)])[i]
                    quality = v.get('qualities',['GOOD']*count)[i]
                    value = v.get('values',[10]*count)[i]
                    s = event(samples,value,quality,q,v.get('completed',[(q or 0)+100 for q in v.get('nominal',range(count))])[i]
                              if 'completed' in v else None)
                    refused = i+1 in v.get('refused',[])
                    with patch.object(engine,'_can_admit',return_value=not refused):
                        accepted = engine.accept(s, s.completed_mono_ms/1000)
                    if accepted or refused:
                        observed.append(s.sample_seq)
                    if refused:
                        self.assertFalse(engine.accept(s, 99))
                self.assertEqual(observed,v['selected'])
                self.assertEqual(engine.missed_report_slots,v.get('missedReportSlots',0))
                if 'nextDue' in v:
                    self.assertEqual(engine.next_report_due[('meter-A','p0')],v['nextDue'])

    def test_R06_selected_same_point_events_are_distinct_and_immutable(self):
        engine, _, samples, _ = setup_engine(report=1000)
        first, second = event(samples,q=0), event(samples,value=20,q=1000)
        engine.accept(first,0); engine.accept(second,1)
        batches=engine.prepare(1,1000,force=True)
        self.assertEqual([b.samples for b in batches],[(first,),(second,)])
        self.assertEqual(len({json.loads(b.payload)['messageId'] for b in batches}),2)
        for b in batches: self.assertEqual(validate_message('data',b.payload),[])
        self.assertEqual(engine.prepare(2,2000,force=True),batches)
        self.assertTrue(all(a is b for a,b in zip(batches,engine.prepare(3,3000,force=True))))
        with self.assertRaises(FrozenInstanceError): first.value=100
        with self.assertRaises(FrozenInstanceError): batches[0].payload=b'other'

    def test_R11_newest_committed_wins_over_old_shadow_and_old_commit(self):
        engine, _, samples, _ = setup_engine('change')
        first, second = event(samples,value=1,q=0),event(samples,value=2,q=1000)
        engine.accept(first,0);engine.accept(second,1)
        batches=engine.prepare(1,1000,force=True)
        engine.committed(batches[1],9)
        self.assertEqual(engine.shadows[('meter-A','p0')],first)
        self.assertFalse(engine.accept(event(samples,value=2,q=2000),2))
        engine.committed(batches[0],10)
        engine.committed(batches[0],11)
        self.assertEqual(engine.baselines[('meter-A','p0')][0],second)

    def test_R13_invalid_time_does_not_remove_history_or_retimestamp_it(self):
        engine, _, samples, clock = setup_engine()
        first=event(samples,q=0);self.assertTrue(engine.accept(first,0))
        clock.valid=False;clock.generation=1
        invalid=event(samples,q=5000,origin=5000)
        self.assertFalse(engine.accept(invalid,5));self.assertIsNone(invalid.timestamp)
        clock.valid=True;clock.generation=2
        current=event(samples,q=10000,origin=10000)
        self.assertTrue(engine.accept(current,10))
        self.assertEqual((engine.known_unsent_events,engine.time_invalid_events),(1,1))
        batches=engine.prepare(10,110000,force=True)
        self.assertEqual([b.samples[0] for b in batches],[first,current])
        self.assertEqual(batches[0].samples[0].timestamp,first.timestamp)
        self.assertFalse(engine.accept(invalid,11))

    def test_capacity_refusal_no_baseline_advance_and_no_repeat_loss(self):
        engine, _, samples, _ = setup_engine(report=1000,capacity=1)
        first=event(samples,q=0);engine.accept(first,0)
        second=event(samples,q=1000);self.assertFalse(engine.accept(second,1))
        self.assertFalse(engine.accept(second,1))
        self.assertEqual(engine.known_unsent_events,1)
        self.assertEqual(engine.shadows[('meter-A','p0')],first)
        self.assertEqual(engine.next_report_due[('meter-A','p0')],2000)

    def test_config_switch_keeps_accepted_old_history_and_blocks_stale_completion(self):
        engine, snapshot, samples, _ = setup_engine()
        first=event(samples,q=0);engine.accept(first,0)
        old=engine.prepare(0,100000,force=True)[0]
        engine.configure(replace(snapshot,config_version=11),1,generation=2)
        self.assertFalse(engine.accept(event(samples,q=1000),1))
        self.assertTrue(engine.accept(event(samples,version=11,generation=2,q=1000,origin=1000),1))
        engine.committed(old,2)
        self.assertEqual(engine.baselines,{})
        batch=engine.prepare(2,102000,force=True)[0]
        self.assertEqual(batch.version,11)
        self.assertEqual(engine.shadows[('meter-A','p0')],batch.samples[0])
        engine.configure(engine.snapshot,3,generation=2)
        self.assertEqual(engine.prepare(3,103000,force=True),[batch])

    def test_no_new_result_means_no_synthetic_cyclic_repeat(self):
        engine, _, samples, _ = setup_engine()
        engine.accept(event(samples,q=0),0)
        batch=engine.prepare(0,100000,force=True)[0];engine.committed(batch,0)
        self.assertEqual(engine.prepare(100,200000,force=True),[])
        self.assertEqual(engine.missed_report_slots,0)

    def test_same_process_reload_preserves_committed_baseline_and_grid(self):
        first,snapshot,samples,_=setup_engine()
        admitted=event(samples,q=0);first.accept(admitted,0)
        first.committed(first.prepare(0,100000,force=True)[0],0)
        second=ReportEngine('PLC-01',first.boot_id,boot_context=first.boot_context)
        second.configure(snapshot,1)
        self.assertEqual(second.baselines[('meter-A','p0')][0],admitted)
        self.assertEqual(second.next_report_due[('meter-A','p0')],5000)
        self.assertFalse(second.accept(event(samples,q=1000),1))
        self.assertTrue(second.accept(event(samples,q=5000),5))

    def test_limits_and_metadata_stay_off_legacy_wire(self):
        engine,snapshot,samples,_=setup_engine(points=7)
        engine.configure(replace(snapshot,report=replace(snapshot.report,max_batch_points=3)),0)
        for i in range(7): engine.accept(event(samples,point='p'+str(i)),0)
        batches=engine.prepare(1,101000,force=True)
        self.assertEqual([len(b.samples) for b in batches],[3,3,1])
        for b in batches:
            wire=json.loads(b.payload)
            self.assertEqual(set(wire['values'][0]),{'deviceId','pointId','timestamp','value','quality'})
            self.assertEqual(validate_message('data',b.payload),[])


class CaptureAndStateTests(unittest.TestCase):
    def setUp(self):
        self.engine,self.snapshot,self.samples,self.clock=setup_engine()
        self.latest={};self.events=PointEvents(self.samples,self.latest)
        self.context=SampleContext(10,self.snapshot.devices[0],1)
        self.point=self.context.device.points[0]
        self.events.activate(self.context,{'p0':0},.1)

    def test_R08_R09_R12_unknown_once_stale_once_bad_results_extend_liveness(self):
        unknown=self.samples.get_nowait()
        self.assertEqual((unknown.kind,unknown.quality,unknown.nominal_poll_due_mono_ms),('QUALITY_STATE','UNKNOWN',None))
        self.assertEqual((unknown.nominal_origin_mono_ms,unknown.completed_mono_ms),(0,100))
        self.events.activate(self.context,{'p0':9},9)
        for tick in range(7):
            sample=self.events.result(self.context,self.point,None,'BAD_TIMEOUT',tick,tick+.01)
            self.events.expire(tick+.02)
            self.assertEqual(self.samples.get_nowait(),sample)
        self.events.expire(9.01)
        stale=self.samples.get_nowait()
        self.assertEqual((stale.kind,stale.quality,stale.timestamp),('QUALITY_STATE','STALE',109010))
        for i in range(100):self.events.expire(10+i)
        self.assertEqual(self.samples._queue.qsize(),0)
        self.events.result(self.context,self.point,5,'GOOD',110,110.01)
        self.assertEqual(self.latest['p0'].quality,'GOOD')

    def test_overflow_rejects_new_identity_and_counts_undecided_loss(self):
        samples=SampleBuffer(1,boot_context=self.samples.boot_context,time_provider=self.clock,gateway_id='PLC-01')
        first=event(samples,q=0);second=event(samples,q=1000)
        self.assertTrue(samples.put(first));self.assertFalse(samples.put(second))
        self.assertEqual(samples.get_nowait(),first)
        self.assertEqual((samples.captured_events,samples.capture_overflow_events,samples.undecided_lost_events),(2,1,1))
        self.assertFalse(samples.accounting_complete)
        self.assertEqual(samples.last_overflow_identity,(second.capture_boot_id,second.sample_seq))

    def test_equal_utc_is_not_identity_or_deduplication(self):
        self.samples.time_provider=SimpleNamespace(read=lambda _: CaptureTime(1234,True))
        first=event(self.samples,q=0);second=event(self.samples,q=1000)
        self.assertEqual(first.timestamp,second.timestamp)
        self.assertNotEqual(first.sample_seq,second.sample_seq)
        self.assertEqual(self.samples.boot_context.sequence,0)

    def test_R13_through_actual_capture_source_resets_grid_from_real_poll_plan(self):
        self.engine.configure(self.snapshot,0,generation=1)
        self.samples.get_nowait() # UNKNOWN belongs to a separate cohort in this test.
        first=self.events.result(self.context,self.point,10,'GOOD',0,.01)
        self.assertTrue(self.engine.accept(first,.01))
        self.clock.valid=False;self.clock.generation=1;self.clock.generation_started=5000
        invalid=self.events.result(self.context,self.point,10,'GOOD',5,5.01)
        self.assertFalse(self.engine.accept(invalid,5.01))
        self.clock.valid=True;self.clock.generation=2;self.clock.generation_started=10000
        current=self.events.result(self.context,self.point,10,'GOOD',10,10.01)
        self.assertEqual(current.nominal_origin_mono_ms,10000)
        self.assertTrue(self.engine.accept(current,10.01))
        self.assertEqual(self.engine.missed_report_slots,0)
        self.assertEqual(self.engine.next_report_due[('meter-A','p0')],15000)
        self.assertEqual(first.nominal_origin_mono_ms,0)

    def test_inflight_old_context_is_captured_but_never_relabelled_or_queued(self):
        self.samples.get_nowait()
        old=self.events.result(self.context,self.point,10,'GOOD',0,1,publish=False)
        self.assertEqual((old.config_version,old.config_generation),(10,1))
        self.assertEqual(self.samples._queue.qsize(),0)
        self.assertEqual((self.samples.version_discarded_events,self.samples.undecided_lost_events),(1,1))
        self.assertFalse(self.samples.accounting_complete)

    def test_capture_validation_and_sequence_exhaustion(self):
        for value in (float('inf'),float('nan'),10**350,'5'):
            with self.assertRaises(ValueError):event(self.samples,value=value)
        with self.assertRaises(ValueError):event(self.samples,q=1000,completed=999)
        self.samples.boot_context._sample_sequence=MAX_SEQUENCE
        with self.assertRaisesRegex(ValueError,'Sample sequence exhausted'):event(self.samples)
        self.assertTrue(self.samples.counter_overflow)
        self.assertFalse(self.samples.accounting_complete)

    def test_skipped_slots_stop_before_config_retirement(self):
        self.assertEqual(skipped_after(0,10,1),10)
        self.assertEqual(skipped_after(0,10,1,retired_at=3),2)
        self.assertEqual(skipped_after(0,10,1,retired_at=3.1),3)
        self.assertEqual(skipped_after(5,10,1,retired_at=3),0)

    def test_explicit_system_compatibility_is_not_platform_time(self):
        samples=SampleBuffer(boot_context=self.samples.boot_context,gateway_id='PLC-01')
        s=event(samples)
        self.assertFalse(s.time_valid)
        self.assertEqual(s.time_source,'SYSTEM_RTC_COMPAT')
        self.assertGreater(s.timestamp,0)


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_reload_retains_capture_state_plan_and_sampling_generation(self):
        _,snapshot,samples,clock=setup_engine()
        boot=samples.boot_context
        class Runner:
            async def start(self):pass
            async def stop(self):pass
        first=DeviceRuntime(lambda _:Runner(),boot_context=boot)
        t=first.prepare(snapshot);await t.activate();t.publish()
        context=first.sample_context('meter-A',first.runners['meter-A'])
        source=PointEvents(samples,{})
        source.activate(context,{'p0':0},0)
        source.plan['modbus']=[1.0]
        source.result(context,snapshot.devices[0].points[0],10,'GOOD',0,.01)
        await first.shutdown(1)
        second=DeviceRuntime(lambda _:Runner(),boot_context=boot)
        t=second.prepare(snapshot);await t.activate();t.publish()
        self.assertEqual(second.sample_generation,first.sample_generation)
        new_context=second.sample_context('meter-A',second.runners['meter-A'])
        reloaded_samples=SampleBuffer(boot_context=boot,time_provider=clock,gateway_id='PLC-01')
        restored=PointEvents(reloaded_samples,{})
        restored.activate(new_context,{'p0':99},99)
        self.assertTrue(reloaded_samples._queue.empty())
        self.assertEqual(restored.latest['p0'].quality,'GOOD')
        self.assertEqual(restored.origins,{'p0':0})
        self.assertEqual(restored.plan['modbus'],[1.0])
        next_event=restored.result(new_context,snapshot.devices[0].points[0],11,'GOOD',1,1.01)
        self.assertEqual(next_event.sample_seq,3)
        await second.shutdown(1)

    async def test_slow_or_failed_enqueue_keeps_original_events_and_shadow(self):
        engine,_,samples,_=setup_engine(report=1000)
        pipeline=object.__new__(TelemetryPipeline);pipeline.engine=engine
        payloads=[];outcomes=iter((False,True,True))
        async def call(method,payload,wall):
            self.assertEqual(method,'enqueue');payloads.append(payload)
            await asyncio.sleep(0)
            return next(outcomes)
        pipeline.storage=SimpleNamespace(call=call)
        first=event(samples,q=0);second=event(samples,value=20,q=1000)
        engine.accept(first,0)
        await pipeline._flush(force=True)
        self.assertEqual(engine.baselines,{})
        self.assertEqual(engine.shadows[('meter-A','p0')],first)
        engine.accept(second,1)
        await pipeline._flush(force=True)
        self.assertIs(payloads[0],payloads[1])
        self.assertEqual(engine.baselines[('meter-A','p0')][0],second)
        self.assertEqual(engine._owned,{})


if __name__=='__main__':unittest.main()
