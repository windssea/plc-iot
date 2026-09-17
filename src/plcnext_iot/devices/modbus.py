"""Restartable block polling with bounded admission and point diagnostics."""
import asyncio
import time

from plcnext_iot.drivers.modbus import ModbusReader, ReadError
from plcnext_iot.points.read_plan import plan_reads
from plcnext_iot.points.samples import Sample


class ModbusRunner:
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
        self._reader = ModbusReader(self.device.connection)
        self.last_error = None
        self.latest.clear()
        self._task = asyncio.create_task(self._poll(), name='modbus:' + self.device.device_id)

    async def stop(self):
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self._reader is not None:
            self._reader.close()
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

    async def _poll(self):
        loop = asyncio.get_running_loop()
        blocks = plan_reads(self.device.points)
        deadlines = [loop.time()] * len(blocks)
        reconnect_at, backoff = 0, 0.5
        connection = self.device.connection
        endpoint = (connection.host.lower(), connection.port)
        try:
            while True:
                fallback_budget = 4
                for index, block in enumerate(blocks):
                    if loop.time() < deadlines[index]:
                        continue
                    interval = block.poll_interval_ms / 1000
                    context = self.runtime.sample_context(self.device.device_id, self)
                    results = None
                    attempted = False
                    if loop.time() < reconnect_at:
                        results = {p.point_id: (None, self.last_error or 'BAD_CONNECTION') for p in block.points}
                    else:
                        async with self.runtime.read_budget.acquire(endpoint, deadlines[index] + interval) as admitted:
                            if admitted:
                                attempted = True
                                try:
                                    results, used = await self._reader.read_block(block, fallback_budget)
                                    fallback_budget -= used
                                except ReadError as exc:
                                    results = {p.point_id: (None, exc.quality) for p in block.points}
                            else:
                                self.skipped_polls += 1
                    if results is not None:
                        errors = [quality for _, quality in results.values() if quality != 'GOOD']
                        transport = next((q for q in errors if q in ('BAD_CONNECTION', 'BAD_TIMEOUT')), None)
                        self.last_error = transport or next(iter(errors), None)
                        if transport and attempted:
                            reconnect_at = loop.time() + backoff
                            backoff = min(backoff * 2, 30)
                        elif not errors:
                            backoff, reconnect_at = 0.5, 0
                        current = self.runtime.sample_context(self.device.device_id, self)
                        if context is not None and current is not None and context.config_version == current.config_version:
                            timestamp, captured = time.time_ns() // 1_000_000, loop.time()
                            for point in block.points:
                                value, quality = results[point.point_id]
                                sample = Sample(context.config_version, self.device.device_id,
                                    point.point_id, timestamp, value, quality, captured)
                                self.latest[point.point_id] = sample
                                if quality == 'GOOD':
                                    self.last_good_timestamp = timestamp
                                self.samples.put(sample)
                    previous = deadlines[index]
                    missed = int((loop.time() - previous) / interval)
                    self.skipped_polls += missed
                    deadlines[index] = previous + (missed + 1) * interval
                await asyncio.sleep(max(0, min(deadlines) - loop.time()))
        finally:
            self._reader.close()
