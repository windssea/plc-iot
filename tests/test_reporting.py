from dataclasses import replace
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.points.samples import Sample
from plcnext_iot.contracts import validate_message


class ReportingTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((ROOT/'src/plcnext_iot/reporting/engine.py').exists(),'Report engine missing')
        from plcnext_iot.reporting.engine import ReportEngine
        self.Engine=ReportEngine
        wire=json.loads((ROOT/'contracts/examples/valid/config-set.json').read_bytes())
        self.snapshot=ConfigSnapshot.parse(json.dumps(wire).encode(),'PLC-01')
        self.engine=self.Engine('PLC-01','boot-test')
        self.engine.configure(self.snapshot,0)

    def sample(self,value=10,quality='GOOD',timestamp=1000):
        return Sample(10,'meter-A','voltage',timestamp,value,quality)

    def commit(self,now):
        batches=self.engine.prepare(now,100000+int(now*1000),force=True)
        for batch in batches:
            self.assertEqual(validate_message('data',batch.payload),[])
            self.engine.committed(batch,now)
        return batches

    def test_deadband_compares_last_durable_value_and_is_strict(self):
        p=replace(self.snapshot.devices[0].points[0],deadband=1,report_mode='change')
        d=replace(self.snapshot.devices[0],points=(p,))
        self.engine.configure(replace(self.snapshot,devices=(d,)),0)
        self.engine.accept(self.sample(10),0)
        candidate=self.engine.prepare(0,1000,force=True)
        self.assertEqual(len(candidate),1)
        # Failed persistence does not advance baseline.
        self.assertEqual(len(self.engine.prepare(0.1,1100,force=True)),1)
        self.engine.committed(candidate[0],0.1)
        self.engine.accept(self.sample(11),0.2)
        self.assertEqual(self.commit(0.2),[])
        self.engine.accept(self.sample(11.1),0.3)
        self.assertEqual(len(self.commit(0.3)),1)

    def test_quality_changes_report_null_even_when_deadband_would_suppress(self):
        self.engine.accept(self.sample(),0)
        self.commit(0)
        self.engine.accept(self.sample(None,'BAD_TIMEOUT'),0.1)
        batch=self.commit(0.1)[0]
        value=json.loads(batch.payload)['values'][0]
        self.assertEqual((value['quality'],value['value']),('BAD_TIMEOUT',None))
        self.engine.accept(self.sample(10),0.2)
        self.assertEqual(len(self.commit(0.2)),1)

    def test_cyclic_uses_latest_sample_timestamp_without_fabricating_fresh_read(self):
        self.engine.accept(self.sample(timestamp=1234),0)
        self.commit(0)
        self.assertEqual(self.commit(4),[])
        batches=self.commit(5)
        self.assertEqual(json.loads(batches[0].payload)['values'][0]['quality'],'STALE')
        self.assertEqual(json.loads(batches[0].payload)['values'][0]['timestamp'],1234)

    def test_disabled_or_new_version_clears_cache_and_rejects_old_samples(self):
        self.engine.accept(self.sample(),0)
        self.engine.configure(replace(self.snapshot,config_version=11),0.1)
        self.assertFalse(self.engine.accept(self.sample(),0.2))
        first=self.commit(0.2)[0]
        self.assertEqual(json.loads(first.payload)['configVersion'],11)
        self.assertEqual(json.loads(first.payload)['values'][0]['quality'],'UNKNOWN')
        self.engine.configure(replace(self.snapshot,config_version=12,enabled=False),0.3)
        self.assertEqual(self.commit(0.3),[])

    def test_batch_point_limit_and_no_duplicate_point_values(self):
        p=self.snapshot.devices[0].points[0]
        d=replace(self.snapshot.devices[0],points=tuple(replace(p,point_id='p'+str(i)) for i in range(7)))
        self.engine.configure(replace(self.snapshot,devices=(d,),report=replace(self.snapshot.report,max_batch_points=3)),0)
        batches=self.commit(1)
        self.assertEqual([len(json.loads(b.payload)['values']) for b in batches],[3,3,1])
        self.assertEqual(len({json.loads(b.payload)['messageId'] for b in batches}),3)

    def test_cyclic_waits_for_interval_and_uses_newest_sample(self):
        p=replace(self.snapshot.devices[0].points[0],report_mode='cyclic')
        self.engine.configure(replace(self.snapshot,devices=(replace(self.snapshot.devices[0],points=(p,)),)),0)
        self.engine.accept(self.sample(10,timestamp=1000),0)
        self.commit(0)
        self.engine.accept(self.sample(20,timestamp=3000),4)
        self.assertEqual(self.commit(4),[])
        value=json.loads(self.commit(5)[0].payload)['values'][0]
        self.assertEqual((value['value'],value['timestamp']),(20,3000))

    def test_byte_limit_splits_large_numeric_values_before_500_points(self):
        p=self.snapshot.devices[0].points[0]
        points=tuple(replace(p,point_id='p'+str(i)) for i in range(500))
        self.engine.configure(replace(self.snapshot,devices=(replace(self.snapshot.devices[0],points=points),)),0)
        for point in points:
            self.engine.accept(replace(self.sample(10**350),point_id=point.point_id),0)
        batches=self.commit(0)
        self.assertGreater(len(batches),1)
        self.assertTrue(all(len(b.payload)<=131072 for b in batches))
        self.assertEqual(sum(len(json.loads(b.payload)['values']) for b in batches),500)

    def test_queued_old_sample_does_not_reset_freshness_to_processing_time(self):
        self.assertIn('monotonic_time',Sample.__dataclass_fields__,'Sample capture time is missing')
        self.engine.accept(replace(self.sample(),monotonic_time=0),10)
        value=json.loads(self.commit(10)[0].payload)['values'][0]
        self.assertEqual(value['quality'],'STALE')
