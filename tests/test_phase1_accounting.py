"""I05 production accounting, no database/SDK/device connections."""
import asyncio
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
import uuid

from test_phase1_events import setup_engine,event
from test_phase1_outbox import MemoryRepository,Clock,ack
from plcnext_iot.core.boot import ProcessBootContext,MAX_SEQUENCE
from plcnext_iot.points.samples import SampleBuffer
from plcnext_iot.reporting.accounting import Accounting,COUNTERS
from plcnext_iot.reporting.audit import AuditRecorder
from plcnext_iot.reporting.engine import ReportEngine
from plcnext_iot.reporting.service import TelemetryPipeline
from plcnext_iot.storage.outbox_session import OutboxSession
from plcnext_iot.storage.outbox import OutboxStore

ROOT=Path(__file__).resolve().parents[1]
MEASUREMENTS=[]


class AccountingTests(unittest.TestCase):
    def setUp(self):
        self.engine,self.config,self.samples,_=setup_engine(report=1000)
        self.accounting=self.samples.boot_context.accounting
        self.clock=Clock();self.repo=MemoryRepository()
        self.session=OutboxSession(clock=self.clock,accounting=self.accounting)
        self.store=OutboxStore(None,'PLC-01',repository=self.repo,session=self.session)

    def admit(self,q=0,value=10):
        sample=event(self.samples,q=q,value=value)
        self.assertTrue(self.engine.accept(sample,q/1000))
        return sample

    def commit(self):
        batch=self.engine.prepare(0,1000,force=True)[0]
        self.assertTrue(self.store.enqueue(batch.payload,0,dict(events=tuple(s.sample_seq for s in batch.samples),capture_boot=self.engine.boot_id)))
        self.engine.committed(batch,0)
        return json.loads(batch.payload)['messageId']

    def test_all_16_and_capture_decision_conservation_with_overflow(self):
        self.samples._queue=asyncio.Queue(1)
        first=event(self.samples);second=event(self.samples,q=1)
        self.samples.put(first);self.assertFalse(self.samples.put(second))
        self.engine.accept(self.samples.get_nowait(),0)
        s=self.accounting.snapshot();c=s['counters']
        self.assertEqual(set(c),set(COUNTERS));self.assertEqual(len(c),16)
        self.assertEqual((c['capturedEventsTotal'],c['eligibleEventsTotal'],c['captureOverflowEventsTotal'],c['undecidedLostEventsTotal']),(2,1,1,1))
        self.assertEqual(s['decisionPending'],0)
        self.assertFalse(s['accountingComplete'])
        self.assertEqual(s['buffer']['pendingEvents'],1)
        self.assertEqual(s['lastGap']['sampledFromUtcMs'],second.timestamp)

    def test_filter_time_invalid_and_capacity_are_different_facts(self):
        self.admit();same=event(self.samples,q=1)
        self.assertFalse(self.engine.accept(same,.001))
        self.samples.time_provider.valid=False
        self.assertFalse(self.engine.accept(event(self.samples,q=1000),1))
        self.samples.time_provider.valid=True;self.engine.capacity=1
        self.assertFalse(self.engine.accept(event(self.samples,q=2000),2))
        c=self.accounting.snapshot()['counters']
        self.assertEqual((c['eligibleEventsTotal'],c['policyFilteredEventsTotal'],c['timeInvalidEventsTotal'],c['knownUnsentEventsTotal']),(3,1,1,2))
        self.assertEqual(c['enqueueRejectedAttemptsTotal'],1)

    def test_pending_event_owner_does_not_double_at_commit_and_ack(self):
        self.admit();key=self.commit()
        s=self.accounting.snapshot()
        self.assertEqual((s['buffer']['pendingEvents'],s['buffer']['pendingBatches']),(1,1))
        self.store.reserve(0);self.assertTrue(self.store.acknowledge(ack(key)))
        self.assertFalse(self.store.acknowledge(ack(key)))
        s=self.accounting.snapshot()
        self.assertEqual((s['buffer']['pendingEvents'],s['buffer']['pendingBatches'],s['counters']['storedConfirmedEventsTotal']),(0,0,1))

    def test_frozen_uncommitted_batches_are_in_gauge_but_not_sqlite_payload_quota(self):
        self.admit();self.engine.prepare(0,1000,force=True)
        snap=self.accounting.snapshot()['buffer']
        self.assertEqual((snap['pendingEvents'],snap['pendingBatches'],snap['usedBytes']),(1,1,0))
        self.engine.abandon();snap=self.accounting.snapshot()['buffer']
        self.assertEqual((snap['pendingEvents'],snap['pendingBatches'],snap['usedBytes']),(0,0,0))

    def test_failed_enqueue_attempts_do_not_add_event_losses(self):
        self.admit();batch=self.engine.prepare(0,1000,force=True)[0]
        self.repo.fail_insert=True
        for _ in range(4):
            with self.assertRaises(RuntimeError):self.store.enqueue(batch.payload,0,dict(events=(1,),capture_boot=self.engine.boot_id))
        s=self.accounting.snapshot()
        self.assertEqual(s['counters']['enqueueRejectedAttemptsTotal'],4)
        self.assertEqual(s['counters']['knownUnsentEventsTotal'],0)
        self.assertEqual(s['buffer']['pendingEvents'],1)
        self.engine.abandon();s=self.accounting.snapshot()
        self.assertEqual(s['counters']['knownUnsentEventsTotal'],1)
        self.assertEqual(s['buffer']['pendingEvents'],0)

    def test_late_stored_changes_gauge_but_keeps_historical_uncertain(self):
        self.admit();key=self.commit();self.store.reserve(0)
        # Inject the terminal decision; trusted-age boundaries live in test_timed_outbox.
        self.clock.now=30;self.store._terminal(key,'EXPIRED_DATA','AGE_EXPIRED')
        s=self.accounting.snapshot()
        self.assertEqual((s['buffer']['pendingEvents'],s['buffer']['deadEvents']),(0,1))
        self.assertEqual(s['counters']['uncertainDeliveryEventsTotal'],1)
        self.store.acknowledge(ack(key));s=self.accounting.snapshot()
        self.assertEqual((s['buffer']['deadEvents'],s['counters']['storedConfirmedEventsTotal'],s['counters']['lateStoredEventsTotal'],s['counters']['uncertainDeliveryEventsTotal']),(0,1,1,1))

    def test_prune_does_not_recount_isolation_or_invent_unknown_ack_mapping(self):
        self.store.max_dead=1
        for i in range(2):
            self.admit(q=i*1000,value=i);key=self.commit()
            self.clock.now=i;self.store.reserve(0)
            self.store.acknowledge(ack(key,'REJECTED',('INVALID_TIMESTAMP',)))
        s=self.accounting.snapshot()
        self.assertEqual(s['counters']['isolatedEventsTotal'],2)
        self.assertEqual((s['buffer']['deadEvents'],s['buffer']['deadBatches']),(1,1))
        self.assertTrue(s['detailTruncated'])

    def test_same_boot_reload_preserves_counters_and_new_boot_starts_zero(self):
        self.admit();self.commit()
        boot=self.samples.boot_context
        before=self.accounting.snapshot()
        samples=SampleBuffer(boot_context=boot,time_provider=self.samples.time_provider,gateway_id='PLC-01')
        engine=ReportEngine('PLC-01',boot.boot_id,boot_context=boot);engine.configure(self.config,0)
        engine.accept(event(samples,q=1000,value=20),1)
        self.assertEqual(boot.accounting.snapshot()['counters']['capturedEventsTotal'],2)
        self.assertEqual(before['counterEpoch'],boot.accounting.snapshot()['counterEpoch'])
        fresh=ProcessBootContext();self.assertNotEqual(fresh.boot_id,boot.boot_id)
        self.assertTrue(all(v==0 for v in fresh.accounting.snapshot()['counters'].values()))

    def test_restored_legacy_gauges_are_null_and_epoch_totals_not_imported(self):
        self.admit();self.commit()
        fresh=Accounting('new-boot')
        store=OutboxStore(None,'PLC-01',repository=self.repo,session=OutboxSession(clock=self.clock,accounting=fresh))
        s=fresh.snapshot()
        self.assertIsNone(s['buffer']['pendingEvents'])
        self.assertEqual(s['buffer']['pendingBatches'],1)
        self.assertFalse(s['accountingComplete'])
        self.assertEqual(s['lastGap']['reason'],'VOLATILE_RESTART')
        self.assertIsNone(s['lastGap']['eventCount'])
        self.assertEqual(s['counters']['capturedEventsTotal'],0)
        self.assertEqual(store.stats()['pending'],1)

    def test_same_process_store_reopen_does_not_recount_restored_or_pending(self):
        self.admit();self.commit();before=self.accounting.snapshot()
        OutboxStore(None,'PLC-01',repository=self.repo,session=self.session)
        self.assertEqual(self.accounting.snapshot(),before)

    def test_corrupt_persisted_bytes_stop_complete_claim_without_inventing_a_loss(self):
        self.admit();key=self.commit()
        row,raw=self.repo.records[key];self.repo.records[key]=(row,raw+b' ')
        with self.assertRaises(RuntimeError):self.store.reserve(0)
        s=self.accounting.snapshot()
        self.assertFalse(s['accountingComplete'])
        self.assertEqual(s['buffer']['pendingEvents'],1)
        self.assertEqual(s['counters']['knownUnsentEventsTotal'],0)

    def test_invalid_capture_marks_incomplete_instead_of_claiming_zero_missing(self):
        with self.assertRaises(ValueError):event(self.samples,value=float('nan'))
        self.assertFalse(self.accounting.snapshot()['accountingComplete'])

    def test_source_repeat_only_adjacent_physical_original_utc_and_survives_buffer_reload(self):
        first=event(self.samples,q=0)
        self.samples.time_provider.read=lambda _:SimpleNamespace(utc_ms=first.timestamp,time_valid=True,generation=0,clock_revision=0,source='HUB_MQTT')
        event(self.samples,q=None,completed=2)
        samples=SampleBuffer(boot_context=self.samples.boot_context,gateway_id='PLC-01',time_provider=self.samples.time_provider)
        event(samples,q=1000)
        self.assertEqual(self.accounting.snapshot()['counters']['sourceTimestampRepeatEventsTotal'],1)

    def test_counter_saturation_and_completeness_latch(self):
        self.accounting.add('missedPollCyclesTotal',MAX_SEQUENCE)
        self.accounting.add('missedPollCyclesTotal',1)
        self.accounting.add('missedPollCyclesTotal',1)
        s=self.accounting.snapshot()
        self.assertEqual(s['counters']['missedPollCyclesTotal'],MAX_SEQUENCE)
        self.assertTrue(s['counterOverflow']);self.assertFalse(s['accountingComplete'])
        self.admit();self.assertFalse(self.accounting.snapshot()['accountingComplete'])

    def test_gap_detail_bound_does_not_decrease_total(self):
        self.accounting._gaps=__import__('collections').deque(maxlen=2)
        for i in range(5):
            self.engine.capacity=1
            sample=event(self.samples,q=i*1000,value=i)
            if i==0:self.engine.accept(sample,0)
            else:self.engine.accept(sample,i)
        s=self.accounting.snapshot()
        self.assertEqual(s['counters']['gapRecordsTotal'],4)
        self.assertEqual(len(self.accounting._gaps),2)
        self.assertTrue(s['detailTruncated'])

    def test_h05_counter_dictionary_and_offline_frame_shape_agree(self):
        spec=importlib.util.spec_from_file_location('h05_reference',ROOT.parent/'PLCnext-Hub-Backend/tools/phase1/diagnostics_reference.py')
        reference=importlib.util.module_from_spec(spec);spec.loader.exec_module(reference)
        self.assertEqual(tuple(reference.COUNTERS),COUNTERS)
        frames=json.loads((ROOT.parent/'PLCnext-Hub-Backend/tools/phase1/diagnostics-fixtures.json').read_text())
        frame=frames['sqlite-unknown'];snap=self.accounting.snapshot()
        for key in ('accountingComplete','counterOverflow','mode','durability','counters','buffer','lastGap','detailTruncated'):
            frame[key]=snap[key]
        frame['bootId']=frame['counterEpoch']=self.engine.boot_id
        reference.validate(json.dumps(frame).encode()) # Offline shape only, no publisher.

    def test_metadata_limit_is_hard_even_for_undecided_captures(self):
        account=self.accounting;account.max_metadata_bytes=16384
        for i in range(1000):event(self.samples,q=i)
        self.assertLessEqual(account.metadata_bytes,16384)
        self.assertFalse(account.snapshot()['accountingComplete'])
        self.assertIsNone(account.snapshot()['decisionPending'])

    def test_production_audit_has_values_decisions_and_exact_batch_association(self):
        path=ROOT/'.local'/('i05-complete-audit-'+uuid.uuid4().hex+'.jsonl')
        recorder=AuditRecorder(path)
        self.accounting.attach_recorder(recorder)
        try:
            self.admit(value=42.5);key=self.commit();self.store.reserve(0)
            self.store.acknowledge(ack(key))
        finally:summary=recorder.close()
        facts=[json.loads(line) for line in path.read_text().splitlines()]
        self.assertTrue(summary['accountingComplete'])
        self.assertEqual([f['kind'] for f in facts],['captured','decision','committed','reserved','stored'])
        self.assertEqual(facts[0]['value'],42.5)
        self.assertEqual(facts[0]['nominalPollDueMonoMs'],0)
        self.assertTrue(all(f['counterEpoch']==self.engine.boot_id for f in facts))
        self.assertTrue(all(f['sampleSeq']==1 for f in facts))
        self.assertEqual(facts[-1]['messageId'],key)
        MEASUREMENTS.append(dict(measurement='production_audit_injected_ack',summary=summary,facts=facts))

    def test_compact_audit_preserves_noncontiguous_maximum_width_identities(self):
        path=ROOT/'.local'/('i05-compact-audit-'+uuid.uuid4().hex+'.jsonl')
        recorder=AuditRecorder(path);self.accounting.attach_recorder(recorder)
        seqs=tuple(MAX_SEQUENCE-2*i for i in range(31))
        entry=SimpleNamespace(events=seqs,capture_boot=self.engine.boot_id,row=dict(message_id='compact',hash='a'*64))
        try:self.accounting.record_batch('reserved',entry)
        finally:summary=recorder.close()
        facts=[json.loads(line) for line in path.read_text().splitlines()]
        actual=[]
        for fact in facts:
            if 'sampleSeq' in fact:actual.append(fact['sampleSeq'])
            else:
                self.assertLessEqual(len(fact['sampleSeqs']),256)
                ids=[int(value) for value in fact['sampleSeqs'].split(',')]
                self.assertEqual(len(ids),fact['eventCount']);actual.extend(ids)
        self.assertEqual(tuple(actual),seqs)
        self.assertEqual(len(facts),3)
        self.assertTrue(summary['accountingComplete'])


class AtomicSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_ack_before_ram_transfer_is_one_logical_snapshot(self):
        engine,_,samples,_=setup_engine(report=1000)
        sample=event(samples);engine.accept(sample,0)
        accounting=samples.boot_context.accounting
        session=OutboxSession(accounting=accounting);store=OutboxStore(None,'PLC-01',repository=MemoryRepository(),session=session)
        committed=asyncio.Event();release=asyncio.Event()
        async def call(method,*args):
            accepted=store.enqueue(*args);committed.set();await release.wait();return accepted
        pipeline=object.__new__(TelemetryPipeline);pipeline.engine=engine;pipeline.session=session;pipeline.context=samples.boot_context
        pipeline.storage=SimpleNamespace(call=call)
        task=asyncio.create_task(pipeline._flush(force=True))
        await committed.wait()
        try:
            self.assertEqual(len(engine._owned),1)
            snap=accounting.snapshot()
            self.assertEqual((snap['buffer']['pendingEvents'],snap['buffer']['pendingBatches']),(1,1))
            key=next(iter(session.entries));store.reserve(None);store.acknowledge(ack(key))
            snap=accounting.snapshot()
            self.assertEqual((snap['buffer']['pendingEvents'],snap['counters']['storedConfirmedEventsTotal']),(0,1))
        finally:release.set();await task
        self.assertEqual(accounting.snapshot()['buffer']['pendingEvents'],0)

    async def test_local_api_returns_atomic_snapshot_without_database_calls(self):
        from plcnext_iot.provisioning.server import SetupServer
        server=object.__new__(SetupServer);server._runtime_agent=None
        self.assertEqual((await server.route('GET','/api/diagnostics/buffer',{},b''))[0],503)
        diagnostics=Accounting('test-api').snapshot()
        server._runtime_agent=SimpleNamespace(telemetry=SimpleNamespace(diagnostics_snapshot=lambda:deepcopy(diagnostics)))
        code,_,actual=await server.route('GET','/api/diagnostics/buffer',{},b'')
        self.assertEqual((code,actual),(200,diagnostics))


class AuditTests(unittest.TestCase):
    def test_bounded_production_recorder_records_then_refuses_without_blocking(self):
        path=ROOT/'.local'/('i05-audit-'+uuid.uuid4().hex+'.jsonl')
        recorder=AuditRecorder(path,max_records=1)
        account=Accounting('audit-boot');account.attach_recorder(recorder)
        try:
            account.record('one',sampleSeq=1);account.record('two',sampleSeq=2)
            self.assertFalse(account.snapshot()['accountingComplete'])
        finally:summary=recorder.close()
        self.assertEqual((summary['accepted'],summary['written'],summary['rejected']),(1,1,1))
        self.assertEqual(len(path.read_text().splitlines()),1)
        with self.assertRaises(FileExistsError):AuditRecorder(path)


if __name__=='__main__':unittest.main()
