import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class Sample:
    config_version: int
    device_id: str
    point_id: str
    timestamp: int
    value: bool | int | float | None
    quality: str
    monotonic_time: float | None = None


class SampleBuffer:
    """Bounded diagnostic stream, not a durable telemetry outbox."""
    def __init__(self, capacity=1024):
        if type(capacity) is not int or not 1 <= capacity <= 10000:
            raise ValueError('Sample capacity must be in 1..10000')
        self._queue = asyncio.Queue(capacity)
        self.dropped = 0

    def put(self, sample):
        if self._queue.full():
            self._queue.get_nowait()
            self.dropped += 1
        self._queue.put_nowait(sample)

    async def get(self):
        return await self._queue.get()

    def get_nowait(self):
        return self._queue.get_nowait()
