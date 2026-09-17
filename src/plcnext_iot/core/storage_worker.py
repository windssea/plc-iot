"""SQLite ownership never leaves one dedicated worker thread."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.config.store import ConfigStore


class StoreWorker:
    def __init__(self, path, gateway_id, factory=None):
        self._path, self._gateway_id = path, gateway_id
        self._factory = factory or ConfigStore
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="iot-db")
        self._store = None
        self._closed = False

    async def _run(self, function, *args):
        future = asyncio.get_running_loop().run_in_executor(self._executor, partial(function, *args))
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
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
        return await self._run(lambda: getattr(self._store, method)(*args))

    async def parse(self, payload):
        return await self._run(ConfigSnapshot.parse, payload, self._gateway_id)

    def _close(self):
        if self._store is not None:
            self._store.close()
            self._store = None

    async def close(self):
        if self._closed:
            return
        try:
            await self._run(self._close)
        finally:
            self._executor.shutdown(wait=False, cancel_futures=False)
            self._closed = True
