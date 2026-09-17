import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from plcnext_iot.contracts import validate_message


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((ROOT/'src/plcnext_iot/storage/receiver.py').exists(),'Telemetry receiver store missing')
        from plcnext_iot.storage.receiver import ReceiverStore
        self.Store=ReceiverStore
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'receiver.db'
        self.store=self.Store(self.path,'PLC-01')
        self.data=(ROOT/'contracts/examples/valid/data.json').read_bytes()
        self.config=(ROOT/'contracts/examples/valid/config-set.json').read_bytes()
        self.now=json.loads(self.data)['timestamp']

    def tearDown(self):
        if hasattr(self,'store'):
            self.store.close()
            self.temp.cleanup()

    def test_unknown_version_retry_then_durable_duplicate_confirmation(self):
        rejected=self.store.receive(self.data,self.now)
        self.assertEqual(json.loads(rejected)['errors'][0]['code'],'UNKNOWN_CONFIG_VERSION')
        self.store.install_config(self.config)
        ack=self.store.receive(self.data,self.now)
        self.assertEqual(validate_message('dataAck',ack),[])
        self.assertEqual(json.loads(ack)['status'],'STORED')
        self.store.close()
        self.store=self.Store(self.path,'PLC-01')
        self.assertEqual(json.loads(self.store.receive(self.data,self.now))['status'],'STORED')
        self.assertEqual(self.store.stats()['stored'],1)

    def test_duplicate_id_different_payload_is_rejected(self):
        self.store.install_config(self.config)
        self.store.receive(self.data,self.now)
        value=json.loads(self.data)
        value['values'][0]['value']=99
        ack=self.store.receive(json.dumps(value).encode(),self.now)
        self.assertEqual(json.loads(ack)['errors'][0]['code'],'VERSION_CONFLICT')
        self.assertEqual(self.store.stats()['stored'],1)

    def test_unknown_point_and_future_timestamp_are_not_stored(self):
        self.store.install_config(self.config)
        for change,code in [({'pointId':'missing'},'UNKNOWN_POINT'),({'timestamp':self.now+600000},'INVALID_TIMESTAMP')]:
            value=json.loads(self.data)
            value['values'][0].update(change)
            ack=self.store.receive(json.dumps(value).encode(),self.now)
            self.assertEqual(json.loads(ack)['errors'][0]['code'],code)
        self.assertEqual(self.store.stats()['stored'],0)

    def test_write_failure_never_returns_stored(self):
        self.store.install_config(self.config)
        c=sqlite3.connect(self.path)
        c.execute("CREATE TRIGGER reject_insert BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'injected'); END")
        c.commit()
        c.close()
        ack=self.store.receive(self.data,self.now)
        self.assertEqual(json.loads(ack)['errors'][0]['code'],'STORAGE_ERROR')
        self.assertEqual(self.store.stats()['stored'],0)
