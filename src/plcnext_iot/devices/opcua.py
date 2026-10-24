"""Restartable scheduled OPC UA runner with the Modbus event/diagnostic contract."""
import asyncio

from plcnext_iot.drivers.opcua import OpcUaReader
from plcnext_iot.points.events import PointEvents
from plcnext_iot.points.slots import skipped_before


class OpcUaRunner:
    def __init__(self, device, runtime, samples):
        self.device, self.runtime, self.samples = device, runtime, samples
        self._task = None
        self._reader = None
        self.last_error = None
        self.latest = {}
        self.last_good_timestamp = None
        self.skipped_polls = 0
        self.events = PointEvents(samples, self.latest)
        self._deadlines = {}

    async def start(self):
        if self._task is not None and not self._task.done():
            return
        self.last_error = None
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

    def _before_read(self, point, nominal, origins):
        context = self.runtime.sample_context(self.device.device_id, self)
        if context is not None:
            self.events.activate(context, origins, asyncio.get_running_loop().time())
        return context

    def _refresh_plan(self, deadlines):
        context=self.runtime.sample_context(self.device.device_id,self)
        if context is None:
            return False
        if context!=self.events.context:
            retired=self.runtime.sample_published_at
            if self.events.context is not None:
                for point in self.events.context.device.points:
                    if point.enabled:
                        self._missed(skipped_before(deadlines[point.point_id],retired,point.poll_interval_ms/1000))
            origin=asyncio.get_running_loop().time()
            deadlines.update({p.point_id:origin for p in context.device.points if p.enabled})
            self.events.activate(context,dict(deadlines),origin)
            self.events.plan['opcua']=deadlines
            self.events.plan['next_nominal']=deadlines
        return True

    def _missed(self, count):
        self.skipped_polls += count
        self.samples.missed_poll_cycles += count
        self.samples.boot_context.accounting.add('missedPollCyclesTotal',count)

    def _retired_at(self, context):
        if context != self.runtime.sample_context(self.device.device_id,self):
            return self.runtime.sample_published_at
        return None

    def _publish(self, point, value, quality, nominal, context):
        if context is None:
            return
        captured = asyncio.get_running_loop().time()
        current = self.runtime.sample_context(self.device.device_id, self)
        sample = self.events.result(context, point, value, quality, nominal, captured,
                                    publish=context == current)
        if context != current or sample is None:
            return
        if quality == 'GOOD':
            self.last_good_timestamp = sample.timestamp
            self.last_error = None
        else:
            self.last_error = quality

    async def _states(self):
        while True:
            if self.runtime.sample_context(self.device.device_id, self) == self.events.context:
                self.events.expire(asyncio.get_running_loop().time())
            await asyncio.sleep(0.05)

    async def _run(self):
        backoff = 0.5
        monitor = asyncio.create_task(self._states(), name='opcua-states:' + self.device.device_id)
        try:
            while True:
                context = self.runtime.sample_context(self.device.device_id, self)
                if context is None:
                    await asyncio.sleep(0.01)
                    continue
                now = asyncio.get_running_loop().time()
                if context != self.events.context:
                    self._deadlines = {p.point_id: now for p in context.device.points if p.enabled}
                    self.events.activate(context, self._deadlines, now)
                    self._deadlines=self.events.plan.setdefault('opcua',self._deadlines)
                    self.events.plan['next_nominal']=self._deadlines
                reader = OpcUaReader(context.device.connection, context.device.points)
                self._reader = reader
                try:
                    await reader.run(self._publish, before_read=self._before_read, skipped=self._missed,
                                     deadlines=self._deadlines,retired_at=self._retired_at,
                                     refresh_plan=self._refresh_plan)
                    quality = self.last_error or 'BAD_CONNECTION'
                except asyncio.CancelledError:
                    raise
                except RuntimeError as exc:
                    quality = str(exc) if str(exc).startswith('BAD_') else 'BAD_CONFIGURATION'
                    self.last_error = quality
                    # Failed configuration has no fabricated physical results.
                    await asyncio.Event().wait()
                    return
                except ConnectionError as exc:
                    quality = str(exc) if str(exc) in ('BAD_CONNECTION', 'BAD_TIMEOUT') else 'BAD_CONNECTION'
                self.last_error = quality
                # No-result reconnect waits are represented by STALE and missed
                # logical slots, never repeated synthetic BAD physical outcomes.
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            if self._reader is not None:
                await self._reader.close()
                self._reader = None
