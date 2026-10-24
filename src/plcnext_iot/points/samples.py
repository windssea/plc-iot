import asyncio
from dataclasses import dataclass
import math

from plcnext_iot.core.boot import MAX_SEQUENCE, ProcessBootContext
from plcnext_iot.core.sample_time import SystemRtcCompatibility
from plcnext_iot.core.bounds import event_charge


@dataclass(frozen=True, slots=True)
class Sample:
    config_version: int
    device_id: str
    point_id: str
    timestamp: int | None
    value: bool | int | float | None
    quality: str
    monotonic_time: float | None = None
    capture_boot_id: str | None = None
    sample_seq: int | None = None
    nominal_poll_due_mono_ms: int | None = None
    completed_mono_ms: int | None = None
    kind: str = 'PHYSICAL_RESULT'
    nominal_origin_mono_ms: int | None = None
    config_generation: int = 0
    time_generation: int = 0
    time_valid: bool = False
    clock_revision: int = 0
    time_source: str = 'SYSTEM_RTC_COMPAT'
    gateway_id: str | None = None

    def __post_init__(self):
        if self.timestamp is not None and (type(self.timestamp) is not int or not 0 <= self.timestamp <= MAX_SEQUENCE):
            raise ValueError('Unsafe sample UTC')
        if self.capture_boot_id is not None:
            for value, minimum in ((self.sample_seq, 1), (self.completed_mono_ms, 0),
                                   (self.nominal_origin_mono_ms, 0), (self.config_generation, 0),
                                   (self.time_generation, 0), (self.clock_revision, 0)):
                if type(value) is not int or not minimum <= value <= MAX_SEQUENCE:
                    raise ValueError('Unsafe event metadata')
            if self.kind not in ('PHYSICAL_RESULT','QUALITY_STATE') or (self.kind=='PHYSICAL_RESULT') != (self.nominal_poll_due_mono_ms is not None):
                raise ValueError('Invalid event kind/slot')
            if self.nominal_poll_due_mono_ms is not None and (type(self.nominal_poll_due_mono_ms) is not int
                    or not 0 <= self.nominal_poll_due_mono_ms <= self.completed_mono_ms):
                raise ValueError('Invalid physical slot')
        if self.quality == 'GOOD':
            if type(self.value) is not bool:
                try:
                    value = float(self.value) if type(self.value) in (int, float) else float('nan')
                except (ValueError, OverflowError):
                    value = float('nan')
                if not math.isfinite(value):
                    raise ValueError('GOOD requires a finite binary64 value or bool')
                object.__setattr__(self, 'value', value)
        elif self.value is not None:
            raise ValueError('Non-GOOD event value must be null')


class SampleBuffer:
    """Bounded diagnostic stream, not a durable telemetry outbox."""
    def __init__(self, capacity=4096, *, boot_context=None, time_provider=None, gateway_id=None, max_bytes=4*1024*1024):
        if type(capacity) is not int or not 1 <= capacity <= 10000:
            raise ValueError('Sample capacity must be in 1..10000')
        self._queue = asyncio.Queue(capacity)
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError('Ingress byte limit must be positive')
        self.max_bytes, self.used_bytes = max_bytes, 0
        self.dropped = 0
        self.boot_context = boot_context or ProcessBootContext()
        self.time_provider = time_provider or SystemRtcCompatibility()
        self.gateway_id = gateway_id
        self.captured_events = 0
        self.capture_overflow_events = 0
        self.undecided_lost_events = 0
        self.version_discarded_events = 0
        self.missed_poll_cycles = 0
        self.accounting_complete = True
        self.counter_overflow = False
        self.last_overflow_identity = None
        self._last_put_sequence = 0

    def capture(self, version, device, point, value, quality, *, completed, nominal,
                origin, generation=0, kind='PHYSICAL_RESULT', gateway_id=None, clock_reading=None):
        completed_ms = round(completed * 1000)
        nominal_ms = None if nominal is None else round(nominal * 1000)
        if kind not in ('PHYSICAL_RESULT', 'QUALITY_STATE') or (kind == 'PHYSICAL_RESULT') != (nominal is not None):
            raise ValueError('Event kind and physical slot disagree')
        if nominal_ms is not None and completed_ms < nominal_ms:
            raise ValueError('Physical result precedes its nominal slot')
        clock = clock_reading or self.time_provider.read(completed_ms)
        try:
            sequence = self.boot_context.next_sample_sequence()
        except ValueError:
            self.counter_overflow = True
            self.accounting_complete = False
            raise
        sample = Sample(version, device, point, clock.utc_ms, value, quality, completed,
                        self.boot_context.boot_id, sequence,
                        nominal_ms, completed_ms, kind, round(origin * 1000), generation,
                        clock.generation, clock.time_valid, clock.clock_revision, clock.source,
                        gateway_id or self.gateway_id)
        self.captured_events += 1
        return sample

    def put(self, sample):
        if sample.capture_boot_id != self.boot_context.boot_id or sample.sample_seq is None:
            raise ValueError('Ingress requires this process captured-event identity')
        if sample.sample_seq <= self._last_put_sequence:
            raise ValueError('Ingress identity must be submitted once in capture order')
        self._last_put_sequence = sample.sample_seq
        charge = event_charge(sample)
        if self._queue.full() or self.used_bytes+charge > self.max_bytes:
            self.dropped += 1
            self.capture_overflow_events += 1
            self.undecided_lost_events += 1
            self.accounting_complete = False
            self.last_overflow_identity = (sample.capture_boot_id, sample.sample_seq)
            return False
        self._queue.put_nowait(sample)
        self.used_bytes += charge
        return True

    async def get(self):
        sample = await self._queue.get()
        self.used_bytes -= event_charge(sample)
        return sample

    def get_nowait(self):
        sample = self._queue.get_nowait()
        self.used_bytes -= event_charge(sample)
        return sample
