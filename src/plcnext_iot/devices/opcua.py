"""Restartable OPC UA subscription runner with the same diagnostic contract as Modbus."""
import asyncio
import time

from plcnext_iot.drivers.opcua import OpcUaReader
from plcnext_iot.points.samples import Sample


class OpcUaRunner:
    def __init__(self, device, runtime, samples):
        self.device, self.runtime, self.samples = device, runtime, samples
        self._task = None
        self._reader = None
        self.last_error = None
        self.latest = {}
        self.last_good_timestamp = None
        self.skipped_polls = 0

    async def start(self):
        if self._task is not None and not self._task.done():
            return
        self.last_error = None
        self.latest.clear()
        self._task = asyncio.create_task(self._run(), name='opcua:' + self.device.device_id)

    async def stop(self):
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self._reader is not None:
            await self._reader.close()
            self._reader = None

    def diagnostics(self, device, now):
        qualities = []
        for point in device.points:
            if not point.enabled:
                continue
            sample = self.latest.get(point.point_id)
            quality = sample.quality if sample else 'UNKNOWN'
            if sample and quality == 'GOOD' and now - sample.monotonic_time >= point.stale_after_ms / 1000:
                quality = 'STALE'
            qualities.append(quality)
        good = qualities.count('GOOD')
        if not self.latest:
            state = 'CONNECTING'
        elif good == len(qualities):
            state = 'ONLINE'
        elif not good and all(q in ('BAD_CONNECTION', 'BAD_TIMEOUT', 'STALE') for q in qualities):
            state = 'OFFLINE'
        else:
            state = 'DEGRADED'
        error = next((q for q in qualities if q != 'GOOD'), None)
        if self._task is not None and self._task.done():
            state, error, good = 'OFFLINE', 'POLL_TASK_FAILED', 0
        return dict(deviceId=device.device_id, status=state, lastError=error,
                    lastGoodTimestamp=self.last_good_timestamp), good, len(qualities)

    def _publish(self, point, value, quality):
        context = self.runtime.sample_context(self.device.device_id, self)
        if context is None:
            return
        captured = asyncio.get_running_loop().time()
        sample = Sample(context.config_version, self.device.device_id, point.point_id,
                        time.time_ns() // 1_000_000, value, quality, captured)
        self.latest[point.point_id] = sample
        if quality == 'GOOD':
            self.last_good_timestamp = sample.timestamp
            self.last_error = None
        else:
            self.last_error = quality
        self.samples.put(sample)

    def _publish_all(self, value, quality):
        for point in self.device.points:
            if point.enabled:
                self._publish(point, value, quality)

    async def _run(self):
        backoff = 0.5
        try:
            while True:
                reader = OpcUaReader(self.device.connection, self.device.points)
                self._reader = reader
                try:
                    await reader.run(self._publish)
                    quality = self.last_error or 'BAD_CONNECTION'
                except asyncio.CancelledError:
                    raise
                except RuntimeError as exc:
                    quality = str(exc) if str(exc).startswith('BAD_') else 'BAD_CONFIGURATION'
                    self.last_error = quality
                    self._publish_all(None, quality)
                    await asyncio.Event().wait()
                    return
                except ConnectionError as exc:
                    quality = str(exc) if str(exc) in ('BAD_CONNECTION', 'BAD_TIMEOUT') else 'BAD_CONNECTION'
                self.last_error = quality
                self._publish_all(None, quality)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        finally:
            if self._reader is not None:
                await self._reader.close()
                self._reader = None
