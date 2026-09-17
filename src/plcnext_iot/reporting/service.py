"""Owned sample consumer and durable transport adapter; one DB worker thread."""
import asyncio
import time
import uuid

from plcnext_iot.core.storage_worker import StoreWorker
from plcnext_iot.reporting.engine import ReportEngine
from plcnext_iot.storage.outbox import OutboxStore


class TelemetryPipeline:
    def __init__(self,runtime,samples,directory,gateway,*,on_sample=None,store_factory=None,boot_id=None):
        self.runtime,self.samples=runtime,samples
        # A reload inside one process keeps the boot identity: receivers treat a
        # new bootId as a controller restart, and the sequence would restart too.
        self.engine=ReportEngine(gateway,boot_id or 'boot-'+uuid.uuid4().hex)
        self.storage=StoreWorker(directory/'telemetry.db',gateway,store_factory or OutboxStore)
        self.on_sample=on_sample
        self.last_error=None
        self.final_stats=None
        self._failed=asyncio.Event()
        self._stop=asyncio.Event()
        self._task=None
        self._close_task=None

    async def start(self):
        if self._task is not None or self._stop.is_set():
            raise RuntimeError('Telemetry pipeline starts once')
        try:
            await self.storage.open()
        except BaseException:
            await self.storage.close()
            raise
        self.engine.configure(self.runtime.snapshot,asyncio.get_running_loop().time())
        self._task=asyncio.create_task(self._run(),name='iot-reporting')

    def _accept(self,sample):
        self.engine.accept(sample,asyncio.get_running_loop().time())
        if self.on_sample is not None:
            self.on_sample(sample)

    async def _flush(self,force=False):
        now=asyncio.get_running_loop().time()
        for batch in self.engine.prepare(now,time.time_ns()//1_000_000,force=force):
            if await self.storage.call('enqueue',batch.payload,time.time()):
                self.engine.committed(batch,asyncio.get_running_loop().time())

    async def _run(self):
        try:
            while not self._stop.is_set():
                # Runtime clears its snapshot on shutdown; keep the last mapping
                # until the stopped producers' final queued samples are flushed.
                if self.runtime.snapshot is not None:
                    self.engine.configure(self.runtime.snapshot,asyncio.get_running_loop().time())
                try:
                    sample=await asyncio.wait_for(self.samples.get(),0.05)
                except TimeoutError:
                    sample=None
                if sample is not None:
                    # A config transaction may have committed during the await.
                    if self.runtime.snapshot is not None:
                        self.engine.configure(self.runtime.snapshot,asyncio.get_running_loop().time())
                    self._accept(sample)
                await self._flush()
            while True:
                try:
                    self._accept(self.samples.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await self._flush(force=True)
        except Exception:
            self.last_error='TELEMETRY_REPORT_FAILED'
            self._failed.set()

    async def send_loop(self,connection,topic):
        await self.storage.call('reset_inflight')
        prefer_new=True
        while not self._stop.is_set() and connection.connected:
            payload=await self.storage.call('reserve',time.time(),prefer_new)
            if payload is not None:
                await connection.publish(topic,payload)
                prefer_new=not prefer_new
            # <=4 batches/s, <=512 KiB/s; one PUBACK wait never blocks ingress.
            await asyncio.sleep(0.25)

    async def acknowledge(self,payload):
        return await self.storage.call('acknowledge',payload,time.time())

    async def stats(self):
        return dict(await self.storage.call('stats'),sample_dropped=self.samples.dropped,
                    version_discarded=self.engine.discarded,cache_reset_points=self.engine.cache_reset_points)

    async def wait_failed(self):
        await self._failed.wait()

    async def close(self):
        if self._close_task is None:
            self._stop.set()
            self._close_task=asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self):
        try:
            if self._task is not None:
                await self._task
                self.final_stats=await self.stats()
        finally:
            await self.storage.close()
