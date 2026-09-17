"""Bounded endpoint-first admission; waiters never consume global slots."""
import asyncio
from contextlib import asynccontextmanager


class ReadBudget:
    def __init__(self, capacity=16):
        if type(capacity) is not int or not 1 <= capacity <= 16:
            raise ValueError('Read capacity must be 1..16')
        self._global = asyncio.Semaphore(capacity)
        self._endpoints = {}
        self.skipped = 0

    @asynccontextmanager
    async def acquire(self, endpoint, deadline):
        entry = self._endpoints.setdefault(endpoint, [asyncio.Lock(), 0])
        entry[1] += 1
        local = global_slot = False
        try:
            try:
                async with asyncio.timeout_at(deadline):
                    await entry[0].acquire()
                    local = True
                    await self._global.acquire()
                    global_slot = True
            except TimeoutError:
                self.skipped += 1
            yield global_slot
        finally:
            if global_slot:
                self._global.release()
            if local:
                entry[0].release()
            entry[1] -= 1
            if not entry[1]:
                del self._endpoints[endpoint]
