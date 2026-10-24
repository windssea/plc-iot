"""SQLite ownership never leaves one dedicated worker thread."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.config.store import ConfigStore
from plcnext_iot.core.bounds import retained_bytes


class WorkerFull(RuntimeError):
    pass


class StoreWorker:
    def __init__(self, path, gateway_id, factory=None, *, max_jobs=8, max_bytes=4*1024*1024, detachable=False):
        self._path, self._gateway_id = path, gateway_id
        self._factory = factory or ConfigStore
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="iot-db")
        self._store = None
        self._closed = False
        self.max_jobs, self.max_bytes = max_jobs, max_bytes
        self.jobs = self.used_bytes = 0
        self.detachable = detachable
        self._pending = set()

    async def _run(self, function, *args):
        if self._closed:
            raise RuntimeError('Storage worker closed')
        charge = retained_bytes(args)+512
        if self.jobs >= self.max_jobs or self.used_bytes+charge > self.max_bytes:
            raise WorkerFull('Storage work queue capacity reached')
        self.jobs += 1; self.used_bytes += charge
        future = asyncio.get_running_loop().run_in_executor(self._executor, partial(function, *args))
        self._pending.add(future)
        def settled(done):
            self.jobs -= 1; self.used_bytes -= charge
            self._pending.discard(done)
            # A detached operation may fail after its caller has stopped.
            if not done.cancelled():
                done.exception()
        future.add_done_callback(settled)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            if self.detachable:
                raise
            # A thread cannot be cancelled. Settle its transaction before unwind.
            try:
                await asyncio.shield(future)
            except Exception:
                pass
            raise

    def _open(self):
        self._store = self._factory(self._path, self._gateway_id)

    async def open(self):
        await self._run(self._open)

    async def call(self, method, *args):
        return await self._run(self._call, method, *args)

    def _call(self, method, *args):
        return getattr(self._store, method)(*args)

    async def parse(self, payload):
        return await self._run(ConfigSnapshot.parse, payload, self._gateway_id)

    def _close(self):
        if self._store is not None:
            self._store.close()
            self._store = None

    async def close(self, timeout=None):
        if self._closed:
            return
        # Schedule closure behind already admitted transactions on the same
        # thread, retaining the OS lock until they actually settle.
        self._closed = True
        future = asyncio.get_running_loop().run_in_executor(self._executor, self._close)
        try:
            if timeout is None:
                await asyncio.shield(future)
            else:
                await asyncio.wait_for(asyncio.shield(future), timeout)
        finally:
            self._executor.shutdown(wait=False, cancel_futures=False)
