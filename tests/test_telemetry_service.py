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
        from plcnext_iot.core.sample_time import CaptureTime
        from types import SimpleNamespace
        import time
        runtime=DeviceRuntime()
        runtime.snapshot=ConfigSnapshot.parse((ROOT/'contracts/examples/valid/config-set.json').read_bytes(),'PLC-01')
        samples=SampleBuffer(time_provider=SimpleNamespace(
            read=lambda mono_ms:CaptureTime(time.time_ns()//1_000_000,True,1)))
        with tempfile.TemporaryDirectory() as directory:
            pipeline=TelemetryPipeline(runtime,samples,Path(directory),'PLC-01')
            await pipeline.start()
            tick=asyncio.get_running_loop().time()
            samples.put(samples.capture(10,'meter-A','voltage',230,'GOOD',completed=tick,
                nominal=tick,origin=tick,gateway_id='PLC-01'))
            await pipeline.close()
            second=TelemetryPipeline(runtime,SampleBuffer(),Path(directory),'PLC-01')
            await second.start()
            try:
                # The public diagnostics cache refreshes asynchronously. This
                # test asserts durable restart contents, not a just-started cache.
                stats=await second.storage.call('stats')
                self.assertEqual(stats['pending'],1)
                original=await second.storage.call('reserve',0,False)
                self.assertEqual(json.loads(original)['values'][0]['value'],230)
                self.assertGreater(json.loads(original)['values'][0]['timestamp'],0)
            finally:
                await second.close()
