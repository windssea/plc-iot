import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
import subprocess

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))


class OutboxTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((ROOT/'src/plcnext_iot/storage/outbox.py').exists(),'Durable outbox missing')
        from plcnext_iot.storage.outbox import OutboxStore
        self.Store=OutboxStore
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'telemetry.db'
        self.store=self.Store(self.path,'PLC-01',max_batches=3,max_bytes=10000,max_dead=1)
        self.wire=json.loads((ROOT/'contracts/examples/valid/data.json').read_bytes())

    def tearDown(self):
        if hasattr(self,'store'):
            self.store.close()
            self.temp.cleanup()

    def payload(self,message='batch-1',version=10):
        return json.dumps(dict(self.wire,messageId=message,configVersion=version)).encode()

    def ack(self,message='batch-1',status='STORED',code=None,gateway='PLC-01'):
        return json.dumps({'schemaVersion':1,'gatewayId':gateway,'timestamp':1000,'messageId':message,
            'status':status,'errors':[] if code is None else [{'code':code,'path':'','message':'Rejected'}]}).encode()

    def test_restart_retains_exact_payload_and_only_stored_deletes(self):
        original=self.payload()
        self.assertTrue(self.store.enqueue(original,0))
        sent=self.store.reserve(0,False)
        self.assertEqual(sent,original)
        self.assertEqual(self.store.stats()['pending'],1) # PUBACK cannot delete.
        self.store.close()
        self.store=self.Store(self.path,'PLC-01')
        self.assertEqual(self.store.reserve(1,False),original)
        self.assertFalse(self.store.acknowledge(self.ack(gateway='OTHER'),1))
        self.assertEqual(self.store.stats()['pending'],1)
        self.assertTrue(self.store.acknowledge(self.ack(),1))
        self.assertFalse(self.store.acknowledge(self.ack(),1))
        self.assertEqual(self.store.stats()['pending'],0)

    def test_unsent_or_unknown_ack_cannot_delete(self):
        self.store.enqueue(self.payload(),0)
        self.assertFalse(self.store.acknowledge(self.ack(),0))
        self.assertFalse(self.store.acknowledge(self.ack('unknown'),0))
        self.assertEqual(self.store.stats()['pending'],1)

    def test_retryable_rejection_retains_and_permanent_rejection_deadletters(self):
        self.store.enqueue(self.payload(),0)
        self.store.reserve(0,False)
        self.store.acknowledge(self.ack(status='REJECTED',code='UNKNOWN_CONFIG_VERSION'),1)
        self.assertEqual(self.store.stats()['pending'],1)
        self.assertIsNone(self.store.reserve(2,False))
        self.assertEqual(self.store.reserve(100,False),self.payload())
        self.store.acknowledge(self.ack(status='REJECTED',code='INVALID_TIMESTAMP'),101)
        self.assertEqual((self.store.stats()['pending'],self.store.stats()['dead']),(0,1))

    def test_capacity_refuses_new_and_preserves_existing_batches(self):
        for i in range(3):
            self.assertTrue(self.store.enqueue(self.payload('b'+str(i)),0))
        self.assertFalse(self.store.enqueue(self.payload('overflow'),0))
        self.assertEqual(self.store.stats()['dropped'],1)
        self.assertEqual(self.store.stats()['pending'],3)

    def test_expiry_and_dead_letter_retention_are_bounded(self):
        for i in range(3):
            self.store.enqueue(self.payload('b'+str(i)),0)
        self.assertIsNone(self.store.reserve(7*86400+1,False))
        stats=self.store.stats()
        self.assertEqual((stats['pending'],stats['dead'],stats['dead_pruned']),(0,1,2))

    def test_selection_can_alternate_new_and_due_retry(self):
        self.store.enqueue(self.payload('old'),0)
        self.store.reserve(0,False)
        self.store.enqueue(self.payload('new'),31)
        self.assertEqual(json.loads(self.store.reserve(31,True))['messageId'],'new')
        self.assertEqual(json.loads(self.store.reserve(31,False))['messageId'],'old')

    def test_commit_error_does_not_leave_partially_enqueued_batch(self):
        c=sqlite3.connect(self.path)
        c.execute("CREATE TRIGGER deny_insert BEFORE INSERT ON batches BEGIN SELECT RAISE(ABORT,'test failure'); END")
        c.commit()
        c.close()
        with self.assertRaises(Exception):
            self.store.enqueue(self.payload(),0)
        self.assertEqual(self.store.stats()['pending'],0)

    def test_existing_id_with_different_content_is_not_overwritten(self):
        self.store.enqueue(self.payload(),0)
        with self.assertRaises(ValueError):
            self.store.enqueue(self.payload(version=11),0)
        self.assertEqual(self.store.reserve(0,True),self.payload())

    def test_byte_quota_refuses_batch_even_when_row_capacity_remains(self):
        self.store.close()
        self.store=self.Store(self.path,'PLC-01',max_bytes=len(self.payload())-1)
        self.assertFalse(self.store.enqueue(self.payload(),0))
        self.assertEqual(self.store.stats()['pending'],0)

    def test_inflight_credit_is_limited_to_eight(self):
        self.store.close()
        self.store=self.Store(self.path,'PLC-01',max_batches=20)
        for i in range(10):
            self.store.enqueue(self.payload('b'+str(i)),0)
        for i in range(8):
            self.assertIsNotNone(self.store.reserve(0,True))
        self.assertIsNone(self.store.reserve(0,True))
        self.assertIsNotNone(self.store.reserve(31,True))

    def test_process_exit_after_enqueue_preserves_committed_bytes(self):
        self.store.close()
        code=(f"import sys,os; sys.path.insert(0,{str(ROOT/'src')!r}); "
              "from plcnext_iot.storage.outbox import OutboxStore; "
              f"s=OutboxStore({str(self.path)!r},'PLC-01'); s.enqueue({self.payload()!r},0); os._exit(23)")
        completed=subprocess.run([sys.executable,'-c',code],cwd=ROOT,capture_output=True,timeout=5)
        self.assertEqual(completed.returncode,23,completed.stderr.decode())
        self.store=self.Store(self.path,'PLC-01')
        self.assertEqual(self.store.reserve(0,True),self.payload())
