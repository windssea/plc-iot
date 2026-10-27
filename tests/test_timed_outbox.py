"""Trusted-age and restart policy using a transactional repository double; no database."""
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'tests')]
from test_platform_time import provider,sync,ready,BASE
from test_phase1_outbox import MemoryRepository,payload,ack
from plcnext_iot.storage.outbox import OutboxStore
from plcnext_iot.storage.outbox_session import OutboxSession
from plcnext_iot.core.platform_time import TimeUnavailable
from plcnext_iot.config.store import StoreError,StoreFormatError


class DurableRepository(MemoryRepository):
    def __init__(self):
        super().__init__();self.order=1;self.guard=None
    def next_order(self):return self.order
    def insert(self,row,raw):
        if self.before_insert:self.before_insert()
        if self.guard:self.guard()
        hook=self.before_insert;self.before_insert=None
        try:super().insert(row,raw)
        finally:self.before_insert=hook
        self.order=row['local_order']+1
    def terminal_with_source(self,key,reason,pruned,source):
        super().terminal(key,reason,pruned)
        if key in self.records:self.records[key][0]['terminal_source']=source


def wire(key,stamp=BASE):
    value=json.loads(payload(key));value['timestamp']=stamp
    for item in value['values']:item['timestamp']=min(item['timestamp'],stamp)
    return json.dumps(value,separators=(',',':')).encode()


class TimedOutboxTests(unittest.TestCase):
    def setUp(self):
        self.c,self.time=provider();self.repo=DurableRepository()
        self.session=OutboxSession(clock=lambda:self.c.ms/1000)
        self.store=OutboxStore(None,'PLC-01',repository=self.repo,session=self.session,time_provider=self.time)
        self.repo.guard=lambda:self.store._commit_guard() if self.store._commit_guard else None

    def admitted(self,key='batch-one',seq=1):
        raw=wire(key)
        self.assertTrue(self.store.enqueue(raw,association=dict(events=(seq,),capture_boot=self.time.boot_id),time_token=self.time.token()))
        return raw

    def start(self):sync(self.time,self.c);ready(self.time)

    def test_invalid_time_and_stale_worker_context_never_commit(self):
        with self.assertRaises(TimeUnavailable):self.admitted()
        self.start();token=self.time.token();self.c.advance(2000)
        with self.assertRaises(TimeUnavailable):self.store.enqueue(wire('stale'),time_token=token)
        self.assertEqual(self.repo.records,{})
        self.repo.before_insert=self.time.require_maintenance
        with self.assertRaises(TimeUnavailable):self.admitted('raced')
        self.assertEqual(self.repo.records,{})

    def test_current_token_and_trusted_enqueue_metadata_do_not_change_wire(self):
        self.start();raw=self.admitted()
        row=self.repo.records['batch-one'][0]
        self.assertEqual(row['time_trust'],'HUB_TRUSTED')
        self.assertEqual(row['time_basis_ref'],'site-hub-1')
        self.assertEqual(row['enqueued_utc_ms'],self.time.snapshot().utc_ms)
        sent=self.store.reserve(time_token=self.time.token())
        self.assertEqual(sent,raw)
        old_due=self.session.entries['batch-one'].due
        self.c.wall+=10*86400*1000
        self.assertIsNone(self.store.reserve(time_token=self.time.token()))
        self.assertEqual(old_due,self.session.entries['batch-one'].due)
        self.assertTrue(self.store.acknowledge(ack('batch-one')))

    def test_time_loss_does_not_block_ack_or_trigger_age_scan(self):
        self.start();self.admitted();self.store.reserve(time_token=self.time.token())
        self.time.require_maintenance()
        self.store._expire(10**15)
        self.assertEqual(self.store.stats()['pending'],1)
        with self.assertRaises(TimeUnavailable):self.store.reserve(time_token=self.time.token())
        self.assertTrue(self.store.acknowledge(ack('batch-one')))
        self.assertEqual(self.repo.records,{})
        self.assertFalse(self.store.acknowledge(ack('batch-one','REJECTED',('INVALID_TIMESTAMP',))))

    def test_unknown_legacy_age_survives_upgrade_sync_and_wall_jumps(self):
        self.start();self.admitted()
        row=self.repo.records['batch-one'][0]
        row.update(time_trust='LEGACY_UNVERIFIED',created=-10**12,enqueued_utc_ms=None,uncertainty_ms=None,time_basis_ref=None)
        fresh=OutboxStore(None,'PLC-01',repository=self.repo,session=OutboxSession(clock=lambda:self.c.ms/1000),time_provider=self.time)
        fresh._expire(10**15)
        self.assertEqual(fresh.stats()['age_unknown_batches'],1)
        self.assertEqual(fresh.reserve(time_token=self.time.token()),self.repo.payload('batch-one'))

    def test_conservative_age_boundary_and_active_ack_opportunity(self):
        self.start();self.admitted()
        self.c.advance(7*86400*1000-10000)
        sync(self.time,self.c);ready(self.time)
        self.assertIsNotNone(self.store.reserve(time_token=self.time.token()))
        self.c.advance(20000)
        self.store._expire()
        self.assertEqual(self.store.stats()['pending'],1)
        self.c.advance(11000)
        self.store._expire()
        self.assertEqual(self.store.stats()['dead'],1)
        self.assertEqual(self.repo.records['batch-one'][0]['terminal_source'],'AGE_EXPIRED')
        self.assertTrue(self.store.acknowledge(ack('batch-one')))
        self.assertEqual(self.session.facts['late_stored'],1)

    def test_expiry_requires_lower_bound_not_midpoint_and_source_must_match(self):
        self.start();self.admitted()
        at=self.repo.records['batch-one'][0]['enqueued_utc_ms']
        self.c.advance(7*86400*1000)
        sync(self.time,self.c);ready(self.time)
        row=self.repo.records['batch-one'][0]
        self.session.entries['batch-one'].row.update(enqueued_utc_ms=self.time.snapshot().utc_ms-7*86400*1000)
        self.store._expire();self.assertEqual(self.store.stats()['pending'],1)
        self.c.advance(100)
        self.store._expire();self.assertEqual(self.store.stats()['dead'],1)

    def test_restart_retains_bytes_attempts_and_durable_terminal_source(self):
        self.start();raw=self.admitted();self.store.reserve(time_token=self.time.token())
        self.c.advance(7*86400*1000+100)
        sync(self.time,self.c);ready(self.time);self.store._expire()
        c,p=provider(self.c,boot='next-process');sync(p,c);ready(p)
        fresh=OutboxStore(None,'PLC-01',repository=self.repo,session=OutboxSession(clock=lambda:c.ms/1000),time_provider=p)
        self.assertEqual(self.repo.payload('batch-one'),raw)
        self.assertEqual(fresh.session.entries['batch-one'].row['attempts'],1)
        self.assertEqual(fresh.session.entries['batch-one'].source,'AGE_EXPIRED')
        self.assertTrue(fresh.acknowledge(ack('batch-one')))

    def test_new_process_never_uses_old_next_due_or_resets_attempts(self):
        self.start();raw=self.admitted();self.store.reserve(time_token=self.time.token())
        self.repo.records['batch-one'][0]['next_due']=10**15
        fresh_clock,fresh_time=provider(boot='next-process');sync(fresh_time,fresh_clock);ready(fresh_time)
        fresh_session=OutboxSession(clock=lambda:fresh_clock.ms/1000)
        fresh=OutboxStore(None,'PLC-01',repository=self.repo,session=fresh_session,time_provider=fresh_time)
        self.assertIsNone(fresh.reserve(time_token=fresh_time.token()))
        fresh_clock.advance(60000)
        self.assertEqual(fresh.reserve(time_token=fresh_time.token()),raw)
        self.assertEqual(fresh_session.entries['batch-one'].row['attempts'],2)
        self.assertEqual(fresh_session.entries['batch-one'].due,fresh_clock.ms/1000+60)

    def test_binding_basis_and_known_future_pause_without_deletion(self):
        self.start();self.admitted()
        row=self.session.entries['batch-one'].row
        row['time_basis_ref']='other-hub'
        self.assertIsNone(self.store.reserve(time_token=self.time.token()))
        row['time_basis_ref']='site-hub-1';row['wire_timestamp']=self.time.snapshot().utc_ms+400000
        self.assertIsNone(self.store.reserve(time_token=self.time.token()))
        self.assertEqual(self.store.stats()['pending'],1)

    def test_stored_delete_failure_and_legacy_unknown_dead_do_not_fabricate_terminal(self):
        self.start();self.admitted();self.store.reserve(time_token=self.time.token())
        self.repo.fail_delete=True
        with self.assertRaises(StoreError):self.store.acknowledge(ack('batch-one'))
        self.assertEqual(self.store.stats()['pending'],1)
        self.repo.fail_delete=False
        self.repo.records['batch-one'][0].update(state='DEAD',terminal_source=None,reason='EXPIRED_DATA')
        fresh=OutboxStore(None,'PLC-01',repository=self.repo,session=OutboxSession(clock=lambda:self.c.ms/1000),time_provider=self.time)
        self.assertFalse(fresh.acknowledge(ack('batch-one')))
        self.assertEqual(fresh.stats()['dead'],1)

    def test_hash_corruption_cannot_be_deleted_by_plausible_ack(self):
        self.start();self.admitted();self.store.reserve(time_token=self.time.token())
        row,raw=self.repo.records['batch-one'];self.repo.records['batch-one']=(row,raw+b' ')
        with self.assertRaises(StoreFormatError):self.store.acknowledge(ack('batch-one'))
        with self.assertRaises(StoreFormatError):OutboxStore(None,'PLC-01',repository=self.repo)

    def test_persistent_order_is_not_reused_after_deleting_highest_batch(self):
        self.start();self.admitted();self.store.reserve(time_token=self.time.token());self.store.acknowledge(ack('batch-one'))
        self.admitted('two',2)
        self.assertEqual(self.repo.records['two'][0]['local_order'],2)


if __name__=='__main__':unittest.main()
