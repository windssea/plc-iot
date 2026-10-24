"""P0/2 event-entry selection. Materialization never chooses from latest."""
from collections import deque
from dataclasses import dataclass
from itertools import islice
import uuid

from plcnext_iot.messaging.messages import encode
from plcnext_iot.core.boot import ProcessBootContext
from plcnext_iot.points.samples import Sample
from plcnext_iot.core.bounds import event_charge


@dataclass(frozen=True)
class ReportBatch:
    payload: bytes
    version: int
    samples: tuple[Sample, ...]
    prepared_at: float
    sequence: int


class ReportEngine:
    def __init__(self, gateway, boot_id, *, boot_context=None, capacity=32768, max_bytes=32*1024*1024):
        if type(capacity) is not int or not 1 <= capacity <= 32768:
            raise ValueError('Selected-event capacity must be 1..32768')
        self.gateway, self.boot_id = gateway, boot_id
        self.boot_context = boot_context or ProcessBootContext(boot_id)
        if self.boot_context.boot_id != boot_id:
            raise ValueError('Report engine boot identity does not match its process context')
        self.capacity = capacity
        if type(max_bytes) is not int or max_bytes < 1:
            raise ValueError('Selected-event byte limit must be positive')
        self.max_bytes, self.used_bytes = max_bytes, 0
        self._refs = {}
        self.snapshot = None
        self.generation = None
        self.points, self.latest, self.baselines, self.shadows = {}, {}, {}, {}
        self._epochs, self._seen, self.next_report_due = {}, {}, {}
        self._owned, self._queue, self._frozen, self._owners = {}, deque(), {}, {}
        self._history = deque()
        self._classes = {}
        self._prefer_new = True
        self.watermark = 0
        self.next_batch = 0
        self.discarded = self.cache_reset_points = 0
        self.eligible_events = self.policy_filtered_events = 0
        self.known_unsent_events = self.time_invalid_events = self.missed_report_slots = 0
        self.admission_rejected_events = 0

    @property
    def sequence(self):
        return self.boot_context.sequence

    @staticmethod
    def _identity(sample):
        return sample.capture_boot_id, sample.sample_seq

    @staticmethod
    def _epoch(sample):
        return sample.config_version, sample.config_generation, sample.time_generation

    @staticmethod
    def _key(sample):
        return sample.device_id, sample.point_id

    def _retain(self,sample):
        key=self._identity(sample)
        if key not in self._refs:
            self._refs[key]=[sample,0]
            self.used_bytes+=event_charge(sample)
        self._refs[key][1]+=1

    def _forget(self,sample):
        key=self._identity(sample)
        self._refs[key][1]-=1
        if not self._refs[key][1]:
            self.used_bytes-=event_charge(sample)
            del self._refs[key]

    def configure(self, snapshot, now, *, generation=None):
        if snapshot == self.snapshot and generation == self.generation:
            return
        self.cache_reset_points += len(self.latest)
        self.snapshot, self.generation = snapshot, generation
        self.points = {(d.device_id, p.point_id): p for d in snapshot.devices for p in d.points
                       if snapshot.enabled and d.enabled and p.enabled} if snapshot else {}
        for sample in self.latest.values():
            self._forget(sample)
        for sample,_ in self.baselines.values():
            self._forget(sample)
        self.latest.clear(); self.shadows.clear()
        checkpoint=self.boot_context.report_checkpoint
        if checkpoint is None or checkpoint['identity']!=(snapshot,generation):
            checkpoint=dict(identity=(snapshot,generation),baselines={},epochs={},seen={},next_due={})
            self.boot_context.report_checkpoint=checkpoint
        self.baselines=checkpoint['baselines']
        for sample,_ in self.baselines.values():
            self._retain(sample)
        self._epochs=checkpoint['epochs'];self._seen=checkpoint['seen']
        self.next_report_due=checkpoint['next_due']
        self.next_batch = now + (snapshot.report.batch_interval_ms / 1000 if snapshot else 1)
        # Already admitted histories/frozen bytes retain their owner and captured config.

    def _baseline(self, key):
        candidates = [s for s in (self.shadows.get(key), self.baselines.get(key, (None,))[0]) if s is not None]
        return max(candidates, key=lambda s: s.sample_seq) if candidates else None

    def _can_admit(self, sample):
        reserve = min(2*1024*1024,self.max_bytes//8)
        charge=0 if self._identity(sample) in self._refs else event_charge(sample)
        return len(self._owned) < self.capacity and self.used_bytes+charge <= self.max_bytes-reserve

    def open_gate(self, watermark):
        self.watermark = watermark
        self._history.extend(self._queue)
        self._queue.clear()
        self._classes = {seq: 'history' for seq in self._frozen}

    def accept(self, sample, now):
        key = self._key(sample)
        if (sample.gateway_id!=self.gateway or not self.snapshot or sample.config_version != self.snapshot.config_version
                or key not in self.points or (self.generation is not None and sample.config_generation != self.generation)):
            self.discarded += 1
            self.boot_context.accounting.undecided(sample,'VERSION_DISCARD')
            return False
        if (sample.capture_boot_id is None or sample.sample_seq is None
                or sample.completed_mono_ms is None or sample.nominal_origin_mono_ms is None):
            raise ValueError('Reporting requires a captured event identity and scheduler origin')
        if sample.capture_boot_id != self.boot_id:
            raise ValueError('Capture boot differs from reporting process boot')
        if sample.sample_seq <= self._seen.get(key, 0):
            return False
        self._seen[key] = sample.sample_seq
        epoch = self._epoch(sample)
        if self._epochs.get(key) != epoch:
            self._epochs[key] = epoch
            old=self.baselines.pop(key, None)
            if old is not None:
                self._forget(old[0])
            self.shadows.pop(key, None)
            self.next_report_due[key] = sample.nominal_origin_mono_ms
        old=self.latest.pop(key,None)
        if old is not None:
            self._forget(old)
        # This optional view never consumes reserved materialization headroom.
        if self.used_bytes+event_charge(sample)<=self.max_bytes-min(2*1024*1024,self.max_bytes//8):
            self.latest[key] = sample
            self._retain(sample)
        point = self.points[key]
        baseline = self._baseline(key)
        selected = baseline is None or baseline.quality != sample.quality
        if (baseline is not None and sample.quality == baseline.quality == 'GOOD'
                and point.report_mode in ('change', 'change_or_cyclic')):
            if type(sample.value) is bool or type(baseline.value) is bool:
                selected |= type(sample.value) != type(baseline.value) or sample.value != baseline.value
            else:
                selected |= abs(sample.value - baseline.value) > point.deadband
        if point.report_mode != 'change' and sample.kind == 'PHYSICAL_RESULT':
            q = sample.nominal_poll_due_mono_ms
            if q is None or q > sample.completed_mono_ms:
                raise ValueError('Invalid physical slot')
            due = self.next_report_due[key]
            if q >= due:
                count = (q - due) // point.report_interval_ms + 1
                self.missed_report_slots += count - 1
                self.boot_context.accounting.add('missedReportSlotsTotal',count-1)
                self.next_report_due[key] = due + count * point.report_interval_ms
                selected = True
        if not selected:
            self.policy_filtered_events += 1
            self.boot_context.accounting.decided(sample,selected=False)
            return False
        self.eligible_events += 1
        if sample.timestamp is None or not (sample.time_valid or sample.time_source == 'SYSTEM_RTC_COMPAT'):
            self.time_invalid_events += 1; self.known_unsent_events += 1
            self.boot_context.accounting.decided(sample,selected=True,reason='TIME_INVALID')
            return False
        if not self._can_admit(sample):
            self.admission_rejected_events += 1; self.known_unsent_events += 1
            self.boot_context.accounting.decided(sample,selected=True,reason='CAPACITY_EVICTION')
            return False
        if not self.boot_context.accounting.decided(sample,selected=True,admitted=True):
            return False
        self._owned[self._identity(sample)] = sample
        self._retain(sample)
        self._owners.setdefault((epoch, key), {})[sample.sample_seq] = sample
        queue = self._history if sample.sample_seq <= self.watermark else self._queue
        queue.append((sample, min(500, self.snapshot.report.max_batch_points)))
        self.shadows[key] = sample
        return True

    def prepare(self, now, wall_ms, *, force=False):
        if not force and now < self.next_batch:
            return []
        self.next_batch = now + (self.snapshot.report.batch_interval_ms / 1000 if self.snapshot else 1)
        batches = list(self._frozen.values())
        # Bound work and encoded copies even when SQLite is unavailable. Each
        # class can use seven slots, reserving one for the other class.
        while len(self._frozen) < 8 and (self._queue or self._history):
            available = {c:q for c,q in (('new',self._queue),('history',self._history))
                         if q and sum(v==c for v in self._classes.values()) < 7}
            if not available:
                break
            category = ('new' if self._prefer_new else 'history')
            if category not in available:
                category = next(iter(available))
            queue = available[category]
            pending = []; seen = set(); size = 512; limit = 500
            for sample, sample_limit in islice(queue, 500):
                key = self._key(sample)
                value = dict(deviceId=sample.device_id, pointId=sample.point_id,
                             timestamp=sample.timestamp, value=sample.value, quality=sample.quality)
                length = len(encode(value)) + 1
                if pending and (key in seen or sample.config_version != pending[0].config_version
                                or len(pending) >= min(limit, sample_limit) or size + length > 131072):
                    break
                pending.append(sample); seen.add(key); size += length; limit = min(limit, sample_limit)
            sequence = self.boot_context.next_sequence()
            message_id='batch-'+uuid.uuid4().hex
            values = [dict(deviceId=s.device_id, pointId=s.point_id, timestamp=s.timestamp,
                           value=s.value, quality=s.quality) for s in pending]
            payload = encode(dict(schemaVersion=1, gatewayId=self.gateway, timestamp=wall_ms,
                messageId=message_id, bootId=self.boot_id, sequence=sequence,
                configVersion=pending[0].config_version, values=values))
            if len(payload) > 131072:
                raise ValueError('Telemetry batch exceeds byte limit')
            charge = len(payload)+256+8*len(pending)
            if self.used_bytes+charge > self.max_bytes:
                break
            batch = ReportBatch(payload, pending[0].config_version, tuple(pending), now, sequence)
            self._frozen[sequence] = batch
            self.boot_context.accounting.materialized(sequence,message_id)
            self._classes[sequence] = category
            self.used_bytes += charge
            self._prefer_new = category != 'new'
            batches.append(batch)
            for _ in pending:
                queue.popleft()
        return batches

    def _release(self, sample):
        self._owned.pop(self._identity(sample), None)
        self._forget(sample)
        epoch, key = self._epoch(sample), self._key(sample)
        owners = self._owners[(epoch, key)]
        owners.pop(sample.sample_seq, None)
        if not owners:
            del self._owners[(epoch, key)]
        if self._epochs.get(key) == epoch:
            if owners:
                self.shadows[key] = owners[max(owners)]
            else:
                self.shadows.pop(key, None)

    def committed(self, batch, now):
        sequence=batch.sequence
        if self._frozen.get(sequence) is not batch:
            return
        del self._frozen[sequence]
        self._classes.pop(sequence, None)
        self.used_bytes -= len(batch.payload)+256+8*len(batch.samples)
        for sample in batch.samples:
            key = self._key(sample)
            if self._epochs.get(key) == self._epoch(sample):
                old = self.baselines.get(key)
                if old is None or sample.sample_seq > old[0].sample_seq:
                    if old is not None:
                        self._forget(old[0])
                    self.baselines[key] = (sample, sample.completed_mono_ms / 1000)
                    self._retain(sample)
            self._release(sample)

    def abandon(self):
        """Producer stopped, no enqueue in progress: remaining RAM is known unsent."""
        count = len(self._owned)
        self.boot_context.accounting.abandon(list(self._owned.values()),tuple(self._frozen))
        self.known_unsent_events += count
        for sample in self._owned.values():
            self._forget(sample)
        for batch in self._frozen.values():
            self.used_bytes-=len(batch.payload)+256+8*len(batch.samples)
        self._queue.clear(); self._history.clear(); self._frozen.clear(); self._classes.clear()
        self._owned.clear(); self._owners.clear(); self.shadows.clear()
        return count
