"""Production policy and worker tests with injected repositories; no SQLite or device I/O."""
import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import importlib.util
import json
from pathlib import Path
import threading
import time
import tracemalloc
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_phase1_events import setup_engine, event
from plcnext_iot.config.store import StoreError
from plcnext_iot.core.storage_worker import StoreWorker,WorkerFull
from plcnext_iot.messaging.transport import PublishRejected
from plcnext_iot.reporting.service import TelemetryPipeline
from plcnext_iot.storage.outbox import OutboxStore
from plcnext_iot.storage.outbox_session import OutboxSession
from plcnext_iot.storage.batch_repository import SqlBatches

ROOT=Path(__file__).resolve().parents[1]
MEASUREMENTS=[]
EVIDENCE_OUTPUT=ROOT/'doc/verification/evidence/2026-10-24-i03-i04'


class Clock:
    def __init__(self):self.now=0
    def __call__(self):return self.now


class MemoryRepository:
    """Transactional map implementing only persistence, not scheduling/ACK policy."""
    def __init__(self):
        self.records={};self.legacy=dict(dropped=0,dead_pruned=0)
        self.fail_insert=False;self.fail_delete=False;self.fail_terminal=False
        self.before_insert=None
    def rows(self):return [deepcopy(row) for row,payload in self.records.values()]
    def payload(self,key):return self.records[key][1] if key in self.records else None
    def insert(self,row,payload):
        if self.before_insert:self.before_insert()
        if self.fail_insert:raise StoreError('Injected commit failure')
        if row['message_id'] in self.records:raise ValueError('Duplicate insert')
        self.records[row['message_id']]=(deepcopy(row),payload)
    def update(self,key,**fields):self.records[key][0].update(fields)
    def delete(self,key):
        if self.fail_delete:raise StoreError('Injected ACK commit failure')
        del self.records[key]
    def count_attempt(self,name):self.legacy[name]+=1
    def prune(self,key):self.delete(key);self.count_attempt('dead_pruned')
    def terminal(self,key,reason,pruned):
        if self.fail_terminal:raise StoreError('Injected terminal transaction failure')
        self.records[key][0].update(state='DEAD',reason=reason)
        for old in pruned:del self.records[old]
        self.legacy['dead_pruned']+=len(pruned)
    def counters(self):return dict(self.legacy)


def payload(key):
    wire=json.loads((ROOT/'contracts/examples/valid/data.json').read_bytes())
    wire['messageId']=key
    return json.dumps(wire,separators=(',',':')).encode()


def ack(key,status='STORED',codes=(),gateway='PLC-01'):
    return json.dumps(dict(schemaVersion=1,gatewayId=gateway,timestamp=1000,messageId=key,status=status,
        errors=[dict(code=c,path='',message='Rejected') for c in codes])).encode()


class OutboxPolicyTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.repo=MemoryRepository();self.facts=[]
        self.session=OutboxSession(clock=self.clock,on_fact=self.facts.append)
        self.store=OutboxStore(None,'PLC-01',repository=self.repo,session=self.session)
    def add(self,key,seq=1,now=0):
        data=payload(key)
        self.assertTrue(self.store.enqueue(data,now,dict(events=(seq,),capture_boot='capture')))
        return data
    def sent(self,wall=0):return self.store.reserve(wall)

    def test_failed_enqueue_is_pending_attempt_not_event_loss_and_exact_retry(self):
        self.repo.fail_insert=True
        data=payload('b1')
        for _ in range(3):
            with self.assertRaises(StoreError):self.store.enqueue(data,0,dict(events=(1,2),capture_boot='capture'))
        self.assertEqual(len(self.session.staged),1)
        self.assertEqual(sum(self.session.facts[k] for k in ('known_unsent','isolated','uncertain')),0)
        self.repo.fail_insert=False
        self.assertTrue(self.store.enqueue(data,0,dict(events=(1,2),capture_boot='capture')))
        self.assertEqual(self.sent(),data)
        self.assertEqual(self.session.entries['b1'].row['hash'],hashlib.sha256(data).hexdigest())
        self.assertEqual(tuple(self.session.entries['b1'].events),(1,2))

    def test_first30_then60_monotonic_and_reconnect_does_not_reset(self):
        data=self.add('b1');self.assertEqual(self.sent(),data)
        self.clock.now=29.999
        self.store.reset_inflight();self.store.open_gate(99)
        self.assertIsNone(self.sent(wall=5000))
        self.clock.now=30
        self.assertEqual(self.sent(wall=-1000),data)
        self.clock.now=89.999;self.assertIsNone(self.sent())
        self.clock.now=90;self.assertEqual(self.sent(),data)
        self.assertEqual(self.session.entries['b1'].row['attempts'],3)

    def test_new_process_retains_attempts_ignores_old_due_and_is_paced(self):
        data=self.add('b1');self.sent()
        self.repo.records['b1'][0]['next_due']=10**15
        self.clock.now=10000
        fresh=OutboxSession(clock=self.clock)
        store=OutboxStore(None,'PLC-01',repository=self.repo,session=fresh)
        self.assertFalse(fresh.accounting_complete)
        self.assertEqual(fresh.entries['b1'].category,'history')
        self.assertIsNone(store.reserve(0))
        self.clock.now+=60
        self.assertEqual(store.reserve(0),data)
        self.assertEqual(fresh.entries['b1'].row['attempts'],2)

    def test_local_reload_preserves_deadline_rate_and_terminal_source(self):
        self.add('b1');self.sent();self.clock.now=3
        other=OutboxStore(None,'PLC-01',repository=self.repo,session=self.session)
        self.assertEqual(other.session.entries['b1'].due,30)
        self.assertEqual(tuple(other.session.entries['b1'].events),(1,))
        self.assertIsNone(other.reserve(0))

    def test_watermark_old_zero_attempt_is_history_and_classes_alternate_fifo(self):
        self.add('h1',1);self.add('h2',2);self.store.open_gate(2)
        self.add('n1',3);self.add('n2',4)
        names=[]
        for tick in (0,.25,.5,.75):
            self.clock.now=tick
            names.append(json.loads(self.sent())['messageId'])
        self.assertEqual(names,['n1','h1','n2','h2'])

    def test_seven_history_slots_reserve_one_for_new_and_backoff_skips_head(self):
        for i in range(10):self.add('h'+str(i),i+1)
        self.store.open_gate(10)
        for i in range(7):
            self.clock.now=i*.25;self.assertIsNotNone(self.sent())
        self.clock.now=2;self.assertIsNone(self.sent())
        self.add('n1',11)
        self.assertEqual(json.loads(self.sent())['messageId'],'n1')
        self.assertEqual(sum(e.active for e in self.session.entries.values()),8)
        self.clock.now=2.25;self.assertIsNone(self.sent())
        self.store.acknowledge(ack('h0','REJECTED',('RESOURCE_LIMIT',)))
        self.clock.now=2.5
        self.assertEqual(json.loads(self.sent())['messageId'],'h7')

    def test_eight_restored_active_slots_new_progresses_after_first_release(self):
        for i in range(8):
            self.add('h'+str(i),i+1)
            self.session.entries['h'+str(i)].active=True
            self.session.entries['h'+str(i)].due=30
            self.session.entries['h'+str(i)].row['attempts']=1
        self.store.open_gate(8);self.add('new',9)
        self.assertIsNone(self.sent())
        self.assertTrue(self.store.acknowledge(ack('h0')))
        self.assertEqual(json.loads(self.sent())['messageId'],'new')

    def test_rate_count_and_bytes_retry_reconnect_cannot_burst(self):
        for i in range(20):self.add('b'+str(i),i+1)
        self.store.open_gate(20)
        for tick in (0,.25,.5,.75):
            self.clock.now=tick;self.assertIsNotNone(self.sent())
            self.assertIsNone(self.sent())
        self.store.reset_inflight();self.store.open_gate(20)
        self.clock.now=.999;self.assertIsNone(self.sent())
        self.clock.now=1;self.assertIsNotNone(self.sent())
        self.assertLessEqual(sum(n for _,n in self.session.rate),512*1024)

    def test_ack_before_puback_is_terminal_duplicates_never_resurrect(self):
        self.add('b1',7);self.sent()
        self.assertTrue(self.store.acknowledge(ack('b1')))
        self.assertFalse(self.store.acknowledge(ack('b1')))
        self.assertFalse(self.store.acknowledge(ack('b1','REJECTED',('INVALID_TIMESTAMP',))))
        self.assertEqual(self.session.facts['stored'],1)
        self.assertEqual(self.store.stats()['pending'],0)

    def test_ack_does_not_trigger_expiration_even_with_invalid_wall(self):
        self.add('b1');self.add('b2',2);self.sent()
        self.assertTrue(self.store.acknowledge(ack('b1'),10**15))
        self.assertEqual(self.session.entries['b2'].row['state'],'PENDING')

    def test_ack_failure_keeps_pending_and_repeated_stored_counts_once_after_commit(self):
        self.add('b1');self.sent();self.repo.fail_delete=True
        with self.assertRaises(StoreError):self.store.acknowledge(ack('b1'))
        self.assertEqual(self.session.facts['stored'],0)
        self.repo.fail_delete=False;self.assertTrue(self.store.acknowledge(ack('b1')))
        self.assertEqual(self.session.facts['stored'],1)

    def test_retryable_whitelist_and_mixed_permanent_conflict(self):
        for code in ('UNKNOWN_CONFIG_VERSION','RESOURCE_LIMIT','STORAGE_ERROR'):
            with self.subTest(code=code):
                self.add(code);self.clock.now+=1;self.sent()
                self.assertTrue(self.store.acknowledge(ack(code,'REJECTED',(code,))))
                self.assertEqual(self.session.entries[code].due,self.clock.now+60)
        self.clock.now+=1;self.add('bad');self.sent()
        self.assertTrue(self.store.acknowledge(ack('bad','REJECTED',('RESOURCE_LIMIT','INVALID_TIMESTAMP'))))
        self.assertEqual(self.session.facts['isolated'],1)
        self.assertFalse(self.store.acknowledge(ack('bad')))
        self.assertEqual(self.session.facts['conflicting_acks'],1)

    def test_local_expiry_late_stored_retains_historical_uncertain_fact(self):
        self.add('b1');self.sent()
        # Even a far-future wall value does not expire an active ACK wait.
        self.store._expire(7*86400);self.assertEqual(self.store.stats()['pending'],1)
        self.clock.now=30;self.store._expire(7*86400)
        self.assertEqual(self.session.facts['uncertain'],1)
        self.assertTrue(self.store.acknowledge(ack('b1')))
        self.assertEqual((self.session.facts['uncertain'],self.session.facts['stored'],self.session.facts['late_stored']),(1,1,1))

    def test_unsent_expiry_known_unsent_and_unknown_ack_no_reconstruction(self):
        self.add('b1');self.store._expire(7*86400)
        self.assertEqual(self.session.facts['known_unsent'],1)
        self.assertFalse(self.store.acknowledge(ack('b1')))

    def test_permanent_and_expired_dead_pruning_never_reconstructs_mapping(self):
        self.store.max_dead=1
        for i in range(3):
            self.add('b'+str(i),i+1);self.clock.now=i;self.sent()
            self.store.acknowledge(ack('b'+str(i),'REJECTED',('INVALID_TIMESTAMP',)))
        self.assertEqual((self.store.stats()['dead'],self.store.stats()['dead_pruned']),(1,2))
        self.assertEqual(self.session.facts['isolated'],3)
        self.assertFalse(self.store.acknowledge(ack('b0')))
        self.assertEqual(self.session.facts['stored'],0)
        self.assertTrue(self.session.detail_truncated)

    def test_terminal_and_pruning_failure_are_atomic_and_do_not_exceed_dead_quota(self):
        self.store.max_dead=1
        self.add('b1');self.sent()
        self.store.acknowledge(ack('b1','REJECTED',('INVALID_TIMESTAMP',)))
        self.add('b2',2);self.clock.now=1;self.sent()
        self.repo.fail_terminal=True
        with self.assertRaises(StoreError):self.store.acknowledge(ack('b2','REJECTED',('INVALID_TIMESTAMP',)))
        self.assertEqual(self.store.stats()['dead'],1)
        self.assertEqual(self.session.entries['b2'].row['state'],'PENDING')
        self.assertEqual(self.session.facts['isolated'],1)
        self.repo.fail_terminal=False
        self.assertTrue(self.store.acknowledge(ack('b2','REJECTED',('INVALID_TIMESTAMP',))))
        self.assertEqual((self.store.stats()['dead'],self.store.stats()['dead_pruned']),(1,1))
        self.assertNotIn('b1',self.session.entries)

    def test_legacy_dead_without_source_does_not_invent_late_stored(self):
        self.add('b1');self.sent();self.clock.now=30;self.store._expire(7*86400)
        fresh=OutboxSession(clock=self.clock)
        store=OutboxStore(None,'PLC-01',repository=self.repo,session=fresh)
        self.assertFalse(store.acknowledge(ack('b1')))
        self.assertEqual(store.stats()['dead'],1)

    def test_sdk_definite_rejection_retains_backoff_but_is_known_unsent_on_expiry(self):
        self.add('b1');self.sent();self.store.submission_failed('b1')
        self.assertEqual(self.session.entries['b1'].due,30)
        self.assertIsNone(self.sent())
        self.clock.now=30;self.store._expire(7*86400)
        self.assertEqual((self.session.facts['known_unsent'],self.session.facts['uncertain']),(1,0))

    def test_wrong_gateway_unsent_unknown_and_invalid_ack_are_noops(self):
        self.add('b1')
        for data in (ack('b1'),ack('other'),ack('b1',gateway='OTHER'),b'{}'):
            self.assertFalse(self.store.acknowledge(data))
        self.assertEqual(self.store.stats()['pending'],1)

    def test_payload_count_byte_and_metadata_limits_keep_existing(self):
        self.store.max_batches=1;self.add('b1')
        self.assertFalse(self.store.enqueue(payload('b2'),0))
        self.assertEqual(self.store.stats()['dropped'],1)
        self.store.max_batches=100;self.store.max_bytes=len(payload('b1'))
        self.assertFalse(self.store.enqueue(payload('b2'),0))
        self.store.max_bytes=100000;self.session.max_bytes=self.session.used_bytes
        self.assertFalse(self.store.enqueue(payload('b2'),0))
        self.assertEqual(len(self.session.entries),1)

    def test_recorder_refusal_marks_incomplete_without_undoing_commit(self):
        self.session.on_fact=lambda _:False
        self.add('b1')
        self.assertFalse(self.session.accounting_complete)
        self.assertTrue(self.session.detail_truncated)
        self.assertEqual(self.store.stats()['pending'],1)

    def test_missing_same_process_records_fail_closed_without_erasing_mapping(self):
        self.add('b1');self.repo.records.clear()
        with self.assertRaisesRegex(StoreError,'disappeared'):
            OutboxStore(None,'PLC-01',repository=self.repo,session=self.session)
        self.assertIn('b1',self.session.entries)
        self.assertFalse(self.session.accounting_complete)

    def test_metadata_exhaustion_on_open_preserves_original_payload(self):
        data=self.add('b1')
        fresh=OutboxSession(clock=self.clock,max_bytes=1)
        with self.assertRaisesRegex(StoreError,'metadata quota'):
            OutboxStore(None,'PLC-01',repository=self.repo,session=fresh)
        self.assertEqual(self.repo.payload('b1'),data)

    def test_same_run_G01_G02_external_recorder_preserves_overlapping_facts(self):
        path=ROOT.parent/'PLCnext-Hub-Backend/tools/phase1/acceptance.py'
        spec=importlib.util.spec_from_file_location('phase1_independent_oracle',path)
        oracle=importlib.util.module_from_spec(spec);spec.loader.exec_module(oracle)
        self.add('s',1);self.sent();self.store.acknowledge(ack('s'))
        self.add('p',2)
        self.add('l',3,now=-7*86400)
        self.add('u',4,now=-7*86400)
        self.session.entries['p'].due=100
        self.session.entries['l'].due=100
        self.clock.now=.25;self.assertEqual(json.loads(self.sent(wall=-7*86400))['messageId'],'u')
        self.clock.now=31;self.store._expire(0)
        g1=oracle.partition((1,2,3,4),stored=(1,),pending=(2,),lost=(3,),uncertain=(4,))
        self.assertEqual(g1,dict(S=1,P=1,L=1,I=0,U=1))
        self.assertEqual([self.session.facts[k] for k in ('stored','known_unsent','uncertain')],[1,1,1])
        self.assertTrue(self.store.acknowledge(ack('u')))
        g2=oracle.partition((1,2,3,4),stored=(1,4),pending=(2,),lost=(3,),uncertain=(4,))
        self.assertEqual(g2,dict(S=2,P=1,L=1,I=0,U=0))
        out=EVIDENCE_OUTPUT
        out.mkdir(parents=True,exist_ok=True)
        trace=out/'same-run-correlation.jsonl'
        # Recorder opens exclusively; remove only this test's prior evidence.
        if trace.exists():trace.unlink()
        recorder=oracle.Recorder(trace)
        try:
            for fact in self.facts:
                for seq in fact['sample_seqs']:
                    row={k:v for k,v in fact.items() if k!='sample_seqs'}
                    self.assertTrue(recorder.record(dict(row,sample_seq=seq)))
        finally:result=recorder.close()
        self.assertTrue(result['accountingComplete'])
        MEASUREMENTS.append(dict(measurement='G01_G02_injected_ACK_oracle',G01=g1,G02=g2,
            historicalFacts=dict(self.session.facts),recorder=result,trace=str(trace.relative_to(ROOT))))


class MemoryOwnershipTests(unittest.TestCase):
    def test_capture_byte_quota_rejects_new_and_releases_on_get(self):
        _,_,samples,_=setup_engine()
        first=event(samples);samples.max_bytes=1
        self.assertFalse(samples.put(first));self.assertEqual(samples.used_bytes,0)
        samples.max_bytes=10000;second=event(samples,q=1)
        self.assertTrue(samples.put(second));self.assertGreater(samples.used_bytes,0)
        self.assertIs(samples.get_nowait(),second);self.assertEqual(samples.used_bytes,0)

    def test_selection_byte_bound_preserves_owned_and_no_double_loss(self):
        engine,_,samples,_=setup_engine(report=1000)
        engine.max_bytes=2000
        self.assertTrue(engine.accept(event(samples),0))
        self.assertFalse(engine.accept(event(samples,q=1000,value=20),1))
        self.assertEqual(engine.known_unsent_events,1)
        self.assertLessEqual(engine.used_bytes,engine.max_bytes)

    def test_materialization_bounded_and_reserves_new_slot_with_historical_backlog(self):
        engine,_,samples,_=setup_engine(report=1000)
        for i in range(20):engine.accept(event(samples,q=i*1000,value=i),i)
        engine.open_gate(20)
        old=engine.prepare(20,20000,force=True)
        self.assertEqual(len(old),7)
        fresh=event(samples,q=21000,value=21);engine.accept(fresh,21)
        batches=engine.prepare(21,21000,force=True)
        self.assertEqual(len(batches),8)
        self.assertEqual(batches[-1].samples,(fresh,))
        self.assertEqual(len(engine._owned),21)

    def test_materialization_alternates_and_never_mixes_watermark_categories(self):
        engine,_,samples,_=setup_engine(report=1000)
        for i in range(3):engine.accept(event(samples,q=i*1000,value=i),i)
        engine.open_gate(3)
        for i in range(3,6):engine.accept(event(samples,q=i*1000,value=i),i)
        batches=engine.prepare(10,10000,force=True)
        self.assertEqual([b.samples[0].sample_seq for b in batches],[4,1,5,2,6,3])

    def test_fixed_profile_22500_events_fits_hard_selection_byte_budget(self):
        engine,_,samples,_=setup_engine(report=5000,points=250)
        tracemalloc.start()
        try:
            for cycle in range(90):
                for point in range(250):
                    self.assertTrue(engine.accept(event(samples,q=cycle*5000,point='p'+str(point),value=cycle),cycle*5))
            current,peak=tracemalloc.get_traced_memory()
        finally:tracemalloc.stop()
        self.assertEqual(len(engine._owned),22500)
        self.assertLessEqual(engine.used_bytes,32*1024*1024)
        self.assertLessEqual(peak,32*1024*1024)
        MEASUREMENTS.append(dict(measurement='selection_22500_injected',charged=engine.used_bytes,
                                 traced_current=current,traced_peak=peak))


async def until(predicate,timeout=2):
    async with asyncio.timeout(timeout):
        while not predicate():await asyncio.sleep(.005)


class PipelineIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def make_pipeline(self,*,timeout=.5):
        _,snapshot,samples,_=setup_engine(report=1000,points=2)
        snapshot=replace(snapshot,report=replace(snapshot.report,batch_interval_ms=10))
        runtime=SimpleNamespace(snapshot=snapshot,sample_generation=0)
        repo=MemoryRepository()
        pipeline=TelemetryPipeline(runtime,samples,Path('never-created'),'PLC-01',close_timeout=timeout,
            store_factory=lambda p,g:OutboxStore(None,g,repository=repo,session=pipeline.session))
        await pipeline.start()
        return pipeline,samples,repo

    async def test_slow_threaded_commit_does_not_block_capture_selection_or_health(self):
        pipeline,samples,repo=await self.make_pipeline()
        entered=threading.Event();release=threading.Event()
        repo.before_insert=lambda:(entered.set(),release.wait(2))
        try:
            samples.put(event(samples))
            await until(entered.is_set)
            for i in range(1,30):samples.put(event(samples,q=i*1000,value=i))
            await until(lambda:pipeline.engine.eligible_events==30)
            stats=await asyncio.wait_for(pipeline.stats(),.1)
            self.assertEqual(stats['selected_pending_events'],30)
            self.assertEqual(stats['diagnostics']['buffer']['pendingEvents'],30)
            self.assertEqual(stats['diagnostics']['counters']['eligibleEventsTotal'],30)
            self.assertEqual(stats['diagnostics']['decisionPending'],0)
            self.assertEqual(pipeline.engine.known_unsent_events,0)
            self.assertEqual(pipeline.storage.jobs,1)
        finally:
            release.set();await pipeline.close()
        self.assertEqual(pipeline.shutdown_volatile_events,0)
        self.assertEqual(sum(len(e.events) for e in pipeline.session.entries.values()),30)

    async def test_finite_close_keeps_in_progress_owner_and_blocks_second_writer_until_settled(self):
        pipeline,samples,repo=await self.make_pipeline(timeout=.05)
        entered=threading.Event();release=threading.Event()
        repo.before_insert=lambda:(entered.set(),release.wait(2))
        try:
            samples.put(event(samples));await until(entered.is_set)
            started=time.monotonic();await pipeline.close()
            self.assertLess(time.monotonic()-started,.3)
            self.assertEqual(pipeline.last_error,'TELEMETRY_CLOSE_UNSETTLED')
            self.assertEqual(len(pipeline.engine._owned),1)
            self.assertEqual(pipeline.session.facts['known_unsent'],0)
            second=TelemetryPipeline(pipeline.runtime,samples,Path('never-created'),'PLC-01',
                boot_context=pipeline.context,store_factory=lambda p,g:None)
            try:
                with self.assertRaisesRegex(RuntimeError,'still owns storage'):await second.start()
            finally:await second.storage.close()
        finally:
            release.set();await asyncio.wait_for(pipeline.context.telemetry_closing,1)
        self.assertEqual(len(pipeline.engine._owned),0)
        self.assertEqual(len(repo.records),1)

    async def test_close_full_outbox_marks_remaining_ram_known_unsent_once(self):
        pipeline,samples,repo=await self.make_pipeline(timeout=.08)
        repo.fail_insert=True
        samples.put(event(samples))
        await until(lambda:pipeline.engine.eligible_events==1)
        await pipeline.close()
        await asyncio.wait_for(pipeline.context.telemetry_closing,1)
        self.assertEqual(pipeline.shutdown_volatile_events,1)
        self.assertEqual(pipeline.session.facts['known_unsent'],1)
        await pipeline.close()
        self.assertEqual(pipeline.session.facts['known_unsent'],1)
        self.assertEqual(repo.records,{})

    async def test_slow_puback_does_not_block_new_send_or_ack_before_puback(self):
        pipeline,samples,repo=await self.make_pipeline()
        published=[];never=asyncio.Event()
        class Connection:
            connected=True
            def submit_publish(self,topic,data):published.append(data);return object()
            async def wait_puback(self,info):await never.wait()
        connection=Connection()
        for point in ('p0','p1'):samples.put(event(samples,point=point))
        await until(lambda:len(repo.records)>0)
        sender=asyncio.create_task(pipeline.send_loop(connection,'unused'))
        try:
            await until(lambda:len(published)==1)
            key=json.loads(published[0])['messageId']
            self.assertTrue(await pipeline.acknowledge(ack(key)))
            samples.put(event(samples,q=1000,value=20))
            await until(lambda:len(published)==2)
            self.assertEqual(pipeline.session.facts['stored'],2)
        finally:
            connection.connected=False;sender.cancel()
            await asyncio.gather(sender,return_exceptions=True);await pipeline.close()

    async def test_sdk_synchronous_rejection_keeps_original_pending_backoff(self):
        pipeline,samples,repo=await self.make_pipeline()
        class Connection:
            connected=True
            def submit_publish(self,*args):raise PublishRejected('Injected refusal')
        samples.put(event(samples));await until(lambda:bool(repo.records))
        try:
            with self.assertRaises(PublishRejected):await pipeline.send_loop(Connection(),'unused')
            entry=next(iter(pipeline.session.entries.values()))
            self.assertFalse(entry.ever_sent)
            self.assertGreater(entry.due,time.monotonic())
            self.assertEqual(entry.row['state'],'PENDING')
        finally:await pipeline.close()

    async def test_identity_change_cannot_silently_clear_existing_event_associations(self):
        pipeline,samples,repo=await self.make_pipeline()
        samples.put(event(samples));await until(lambda:bool(repo.records))
        await pipeline.close()
        with self.assertRaisesRegex(StoreError,'previous gateway/directory'):
            TelemetryPipeline(pipeline.runtime,samples,Path('other-never-created'),'OTHER',boot_context=pipeline.context)
        self.assertEqual(sum(len(e.events) for e in pipeline.session.entries.values()),1)

    async def test_observer_failure_does_not_strand_other_input_events(self):
        pipeline,samples,repo=await self.make_pipeline()
        def broken(_):raise OSError('Injected log failure')
        pipeline.on_sample=broken
        for i in range(5):samples.put(event(samples,q=i*1000,value=i))
        await until(lambda:pipeline.engine.eligible_events==5)
        await pipeline.close()
        self.assertEqual(sum(len(e.events) for e in pipeline.session.entries.values()),5)
        self.assertTrue(pipeline.session.detail_truncated)

    async def test_handoff_reserves_new_commit_capacity_under_one_slot_backpressure(self):
        engine,_,samples,_=setup_engine(report=1000)
        for i in range(4):engine.accept(event(samples,q=i*1000,value=i),i)
        engine.open_gate(4)
        for i in range(4,8):engine.accept(event(samples,q=i*1000,value=i),i)
        pipeline=object.__new__(TelemetryPipeline);pipeline.engine=engine
        available=True;commits=[]
        async def call(method,data,wall):
            nonlocal available
            if available:
                available=False;commits.append(json.loads(data)['values'][0]['value']);return True
            return False
        pipeline.storage=SimpleNamespace(call=call)
        for _ in range(4):
            available=True;await pipeline._flush(force=True)
        self.assertEqual(commits,[4.,0.,5.,1.])


class PhysicalGuardTests(unittest.TestCase):
    def test_low_disk_preflight_rejects_before_open_or_creating_database(self):
        path=ROOT/'.local/not-created-by-outbox-test.db'
        self.assertFalse(path.exists())
        with patch('plcnext_iot.storage.batch_repository.shutil.disk_usage',return_value=SimpleNamespace(free=1)):
            with self.assertRaisesRegex(StoreError,'before open'):SqlBatches.preflight(path)
        self.assertFalse(path.exists())

    def make(self,*,used=0,free=100*1024*1024,busy=0):
        class File:
            name='telemetry.db';parent='injected'
            def exists(self):return True
            def stat(self):return SimpleNamespace(st_size=used//3)
            def with_name(self,name):return self
        calls=[]
        class Connection:
            def execute(self,sql):
                calls.append(sql);return SimpleNamespace(fetchone=lambda:(busy,0,0))
        repo=object.__new__(SqlBatches)
        repo.db=SimpleNamespace(path=File(),c=Connection())
        repo.max_physical_bytes=256*1024*1024;repo.min_free_bytes=64*1024*1024
        return repo,calls,patch('plcnext_iot.storage.batch_repository.shutil.disk_usage',return_value=SimpleNamespace(free=free))

    def test_physical_pool_and_free_reserve_close_admission_before_sql_mutation(self):
        for options in (dict(used=256*1024*1024),dict(free=64*1024*1024)):
            with self.subTest(options=options):
                repo,calls,guard=self.make(**options)
                with guard,self.assertRaises(StoreError):repo.guard()
                self.assertEqual(calls,[])

    def test_busy_wal_reader_closes_admission_instead_of_growing_without_bound(self):
        repo,calls,guard=self.make(busy=1)
        with guard,self.assertRaises(StoreError):repo.guard()
        self.assertEqual(calls,['PRAGMA wal_checkpoint(TRUNCATE)'])

    def test_safe_disk_runs_checkpoint_before_mutation(self):
        repo,calls,guard=self.make()
        with guard:repo.guard()
        self.assertEqual(calls,['PRAGMA wal_checkpoint(TRUNCATE)'])


class WorkerBoundTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_admission_count_and_bytes_are_bounded_and_release(self):
        entered=threading.Event();release=threading.Event()
        class Store:
            def slow(self,data):entered.set();release.wait(2);return len(data)
            def close(self):pass
        worker=StoreWorker(None,None,lambda *args:Store(),max_jobs=1,max_bytes=2048)
        await worker.open()
        first=asyncio.create_task(worker.call('slow',b'x'))
        try:
            await until(entered.is_set)
            with self.assertRaises(WorkerFull):await worker.call('slow',b'y')
            self.assertEqual(worker.jobs,1)
        finally:
            release.set();await first
        with self.assertRaises(WorkerFull):await worker.call('slow',b'z'*3000)
        self.assertEqual((worker.jobs,worker.used_bytes),(0,0))
        await worker.close()


if __name__=='__main__':unittest.main()
