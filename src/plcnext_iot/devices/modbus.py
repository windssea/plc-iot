"""Restartable block polling with bounded admission and point diagnostics."""
import asyncio

from plcnext_iot.drivers.modbus import ModbusReader, ReadError
from plcnext_iot.points.read_plan import plan_reads
from plcnext_iot.points.events import PointEvents
from plcnext_iot.points.slots import skipped_after


class ModbusRunner:
    def __init__(self, device, runtime, samples):
        self.device, self.runtime, self.samples = device, runtime, samples
        self._task = None
        self._reader = None
        self.last_error = None
        self.latest = {}
        self.last_good_timestamp = None
        self.skipped_polls = 0
        self.events = PointEvents(samples, self.latest)

    async def start(self):
        if self._task is not None and not self._task.done():
            return
        self._reader = ModbusReader(self.device.connection)
        self.last_error = None
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
            if sample and now - self.events.last_result.get(point.point_id, sample.monotonic_time) >= point.stale_after_ms / 1000:
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
        error = self.last_error or next((q for q in qualities if q != 'GOOD'), None)
        if self._task is not None and self._task.done():
            state, error, good = 'OFFLINE', 'POLL_TASK_FAILED', 0
        return dict(deviceId=device.device_id, status=state, lastError=error,
                    lastGoodTimestamp=self.last_good_timestamp), good, len(qualities)

    async def _poll(self):
        loop = asyncio.get_running_loop()
        blocks, deadlines, active = (), [], None
        reconnect_at, backoff = 0, 0.5
        connection = self.device.connection
        endpoint = (connection.host.lower(), connection.port)
        monitor = asyncio.create_task(self._states(), name='modbus-states:' + self.device.device_id)
        try:
            while True:
                context = self.runtime.sample_context(self.device.device_id, self)
                if context is None:
                    await asyncio.sleep(0.01)
                    continue
                if context != active:
                    active = context
                    blocks = plan_reads(context.device.points)
                    origin = loop.time()
                    deadlines = [origin] * len(blocks)
                    self.events.activate(context, {p.point_id: origin for b in blocks for p in b.points}, origin)
                    deadlines = self.events.plan.setdefault('modbus',deadlines)
                    self.events.plan.setdefault('next_nominal',{p.point_id:deadlines[i] for i,b in enumerate(blocks) for p in b.points})
                fallback_budget = 4
                for index, block in enumerate(blocks):
                    if loop.time() < deadlines[index]:
                        continue
                    interval = block.poll_interval_ms / 1000
                    context = self.runtime.sample_context(self.device.device_id, self)
                    if context != active:
                        break
                    nominal = deadlines[index]
                    results = None
                    attempted = False
                    if loop.time() < reconnect_at:
                        self._missed(len(block.points))
                    else:
                        async with self.runtime.read_budget.acquire(endpoint, deadlines[index] + interval) as admitted:
                            if admitted and self.runtime.sample_context(self.device.device_id,self) == context:
                                attempted = True
                                try:
                                    results, used = await self._reader.read_block(block, fallback_budget)
                                    fallback_budget -= used
                                except ReadError as exc:
                                    results = {p.point_id: (None, exc.quality) for p in block.points}
                            else:
                                self._missed(len(block.points))
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
                        captured = loop.time()
                        for point in block.points:
                            value, quality = results[point.point_id]
                            sample = self.events.result(context, point, value, quality, nominal, captured,
                                                        publish=context == current)
                            if context == current and quality == 'GOOD':
                                self.last_good_timestamp = sample.timestamp
                    previous = deadlines[index]
                    retired = (self.runtime.sample_published_at
                               if self.runtime.sample_context(self.device.device_id,self) != context else None)
                    missed = skipped_after(previous,loop.time(),interval,retired_at=retired)
                    self._missed(missed * len(block.points))
                    deadlines[index] = previous + (missed + 1) * interval
                    for point in block.points:
                        self.events.plan['next_nominal'][point.point_id]=deadlines[index]
                if self.runtime.sample_context(self.device.device_id, self) == active:
                    self.events.expire(loop.time())
                await asyncio.sleep(max(0, min(min(deadlines, default=loop.time()+0.05), self.events.next_expiry()) - loop.time()))
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            self._reader.close()

    async def _states(self):
        while True:
            if self.runtime.sample_context(self.device.device_id, self) == self.events.context:
                self.events.expire(asyncio.get_running_loop().time())
            await asyncio.sleep(0.05)

    def _missed(self, count):
        self.skipped_polls += count
        self.samples.missed_poll_cycles += count
        self.samples.boot_context.accounting.add('missedPollCyclesTotal',count)
