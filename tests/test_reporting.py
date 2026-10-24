"""Reporting integration across intake, materialization and local commit, no DB."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tests'))
from test_phase1_events import setup_engine,event
from plcnext_iot.contracts import validate_message


class ReportingTests(unittest.TestCase):
    def test_deadband_uses_newest_admitted_shadow_before_durable_commit(self):
        engine,_,samples,_=setup_engine('change',1)
        first=event(samples,value=10);engine.accept(first,0)
        pending=engine.prepare(0,100000,force=True)[0]
        self.assertFalse(engine.accept(event(samples,value=11,q=1000),1))
        self.assertTrue(engine.accept(event(samples,value=11.1,q=2000),2))
        engine.committed(pending,10)
        self.assertFalse(engine.accept(event(samples,value=11.2,q=3000),3))

    def test_quality_change_is_selected_for_all_modes_and_keeps_null(self):
        for mode in ('cyclic','change','change_or_cyclic'):
            engine,_,samples,_=setup_engine(mode,100)
            for q,value,quality in ((0,10,'GOOD'),(1000,None,'BAD_TIMEOUT'),(2000,10,'GOOD')):
                self.assertTrue(engine.accept(event(samples,value,quality,q),q/1000))
            batches=engine.prepare(3,103000,force=True)
            values=[json.loads(b.payload)['values'][0] for b in batches]
            self.assertEqual([v['quality'] for v in values],['GOOD','BAD_TIMEOUT','GOOD'])
            self.assertIsNone(values[1]['value'])

    def test_cyclic_never_rechooses_latest_when_batch_deadline_wakes(self):
        engine,_,samples,_=setup_engine()
        first=event(samples,value=10,q=0);engine.accept(first,0)
        self.assertFalse(engine.accept(event(samples,value=20,q=4000),4))
        batch=engine.prepare(5,105000,force=True)[0]
        self.assertEqual(batch.samples,(first,))
        engine.committed(batch,5)
        self.assertEqual(engine.prepare(6,106000,force=True),[])
        self.assertTrue(engine.accept(event(samples,value=30,q=5000,completed=6001),6.001))

    def test_old_capture_keeps_its_quality_and_timestamp_after_ingress_delay(self):
        engine,_,samples,_=setup_engine()
        first=event(samples,q=0,completed=10)
        self.assertTrue(engine.accept(first,10))
        wire=json.loads(engine.prepare(10,110000,force=True)[0].payload)['values'][0]
        self.assertEqual((wire['quality'],wire['timestamp']),('GOOD',100010))

    def test_finite_engineering_numbers_and_limit_are_valid_old_contract(self):
        engine,snapshot,samples,_=setup_engine(points=500)
        for i in range(500):engine.accept(event(samples,1.7976931348623157e308,point='p'+str(i)),0)
        batches=engine.prepare(1,101000,force=True)
        self.assertEqual(sum(len(b.samples) for b in batches),500)
        for batch in batches:
            self.assertLessEqual(len(batch.payload),131072)
            self.assertEqual(validate_message('data',batch.payload),[])
        with self.assertRaises(ValueError):event(samples,10**350)

    def test_disabled_config_retains_previously_accepted_history(self):
        engine,snapshot,samples,_=setup_engine()
        first=event(samples);engine.accept(first,0)
        engine.configure(replace(snapshot,config_version=11,enabled=False),1)
        self.assertFalse(engine.accept(event(samples,version=11,q=1000),1))
        batch=engine.prepare(2,102000,force=True)[0]
        self.assertEqual((batch.version,batch.samples),(10,(first,)))
        engine.committed(batch,2)
        self.assertEqual(engine.baselines,{})


if __name__=='__main__':unittest.main()
