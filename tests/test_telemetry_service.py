import asyncio
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.points.samples import Sample,SampleBuffer
from plcnext_iot.devices.runtime import DeviceRuntime


class TelemetryServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_flushes_pending_sample_and_restart_preserves_batch(self):
        self.assertTrue((ROOT/'src/plcnext_iot/reporting/service.py').exists(),'Telemetry pipeline missing')
        from plcnext_iot.reporting.service import TelemetryPipeline
        runtime=DeviceRuntime()
        runtime.snapshot=ConfigSnapshot.parse((ROOT/'contracts/examples/valid/config-set.json').read_bytes(),'PLC-01')
        samples=SampleBuffer()
        with tempfile.TemporaryDirectory() as directory:
            pipeline=TelemetryPipeline(runtime,samples,Path(directory),'PLC-01')
            await pipeline.start()
            samples.put(Sample(10,'meter-A','voltage',1000,230,'GOOD'))
            await pipeline.close()
            second=TelemetryPipeline(runtime,SampleBuffer(),Path(directory),'PLC-01')
            await second.start()
            try:
                stats=await second.stats()
                self.assertEqual(stats['pending'],1)
                original=await second.storage.call('reserve',0,False)
                self.assertEqual(json.loads(original)['values'][0]['value'],230)
                self.assertEqual(json.loads(original)['values'][0]['timestamp'],1000)
            finally:
                await second.close()
