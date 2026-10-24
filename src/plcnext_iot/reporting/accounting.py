"""One boot's bounded counters and logical ownership publication, across threads."""
from collections import OrderedDict, deque
from copy import deepcopy
import threading
import sys
import uuid

from plcnext_iot.core.bounds import retained_bytes

MAX=9007199254740991
COUNTERS=tuple(('capturedEventsTotal eligibleEventsTotal policyFilteredEventsTotal timeInvalidEventsTotal '
    'captureOverflowEventsTotal undecidedLostEventsTotal knownUnsentEventsTotal isolatedEventsTotal '
    'uncertainDeliveryEventsTotal storedConfirmedEventsTotal lateStoredEventsTotal enqueueRejectedAttemptsTotal '
    'missedPollCyclesTotal missedReportSlotsTotal sourceTimestampRepeatEventsTotal gapRecordsTotal').split())


class Accounting:
    """Aggregates describe logical owners, never FIFO + a stale DB cache.

    Publication follows a successful DB transaction. During the transaction RAM
    remains the owner; commit changes the batch gauge, not the event gauge.
    No disk operation is performed while this lock is held.
    """
    def __init__(self,boot_id,*,max_gaps=64,max_metadata_bytes=1024*1024):
        if type(max_gaps) is not int or max_gaps<1 or type(max_metadata_bytes) is not int or max_metadata_bytes<16384:
            raise ValueError('Accounting bounds must be positive and metadata at least 16 KiB')
        self.boot_id=boot_id
        self.lock=threading.RLock()
        self.counters=dict.fromkeys(COUNTERS,0)
        self.complete=True;self.overflow=False;self.truncated=False
        self.revision=0;self.recorder=None
        self._pending_decisions=set();self._decision_unknown=False
        self._frozen_batches={}
        self._physical=OrderedDict()
        self._gaps=deque(maxlen=max_gaps)
        self._physical_bytes=self._gap_bytes=0
        self.max_metadata_bytes=max_metadata_bytes
        self.pending=self.dead=self.pending_batches=self.dead_batches=self.payload_bytes=0
        self.unknown_pending=self.unknown_dead=0
        self.recovery_noted=False

    def _add(self,name,count=1):
        if type(count) is not int or count<0:
            raise ValueError('Counter increment must be a nonnegative integer')
        value=self.counters[name]+count
        if value>MAX:self.overflow=True;self.complete=False
        self.counters[name]=min(MAX,value)
        self.revision+=1

    def add(self,name,count=1):
        with self.lock:self._add(name,count)

    def incomplete(self,*,truncated=False,overflow=False):
        with self.lock:
            changed=self.complete or (truncated and not self.truncated) or (overflow and not self.overflow)
            self.complete=False;self.truncated|=truncated;self.overflow|=overflow;self.revision+=bool(changed)

    def attach_recorder(self,recorder):
        with self.lock:self.recorder=recorder

    def record(self,kind,**fields):
        recorder=self.recorder
        if recorder is not None:
            try:ok=recorder.record(dict(kind=kind,counterEpoch=self.boot_id,**fields))
            except Exception:ok=False
            if not ok:self.incomplete(truncated=True)

    def _bound(self):
        # Reserve 1 MiB of the 4 MiB metadata pool; Outbox uses the other 3 MiB.
        while self._physical and (len(self._physical)>2000 or self.metadata_bytes>self.max_metadata_bytes):
            _,(_,charge)=self._physical.popitem(last=False);self._physical_bytes-=charge
            self.complete=False;self.truncated=True
        while self._gaps and self.metadata_bytes>self.max_metadata_bytes:
            self._gap_bytes-=retained_bytes(self._gaps.popleft())+64
            self.complete=False;self.truncated=True
        if self.metadata_bytes>self.max_metadata_bytes:
            self._pending_decisions.clear();self._decision_unknown=True
            self.complete=False;self.truncated=True

    @property
    def metadata_bytes(self):
        # Fixed allowance covers counters, aggregate gauges and locks.
        return (8192+sys.getsizeof(self._pending_decisions)+len(self._pending_decisions)*32+
                sys.getsizeof(self._frozen_batches)+len(self._frozen_batches)*256+self._physical_bytes+self._gap_bytes)

    def captured(self,sample):
        with self.lock:
            self._add('capturedEventsTotal')
            if len(self._pending_decisions)<8192:
                self._pending_decisions.add(sample.sample_seq)
            else:
                self._decision_unknown=True;self.complete=False;self.truncated=True
            if sample.kind=='PHYSICAL_RESULT':
                key=(sample.gateway_id,sample.device_id,sample.point_id)
                previous=self._physical.pop(key,None)
                if previous is not None:self._physical_bytes-=previous[1]
                if sample.timestamp is not None and previous is not None and previous[0]==sample.timestamp:
                    self._add('sourceTimestampRepeatEventsTotal')
                charge=retained_bytes((key,sample.timestamp))+128
                self._physical[key]=(sample.timestamp,charge);self._physical_bytes+=charge
            self._bound()
        self.record('captured',sampleSeq=sample.sample_seq,captureBootId=sample.capture_boot_id,
            gatewayId=sample.gateway_id,configVersion=sample.config_version,deviceId=sample.device_id,
            pointId=sample.point_id,eventKind=sample.kind,nominalPollDueMonoMs=sample.nominal_poll_due_mono_ms,
            completedMonoMs=sample.completed_mono_ms,originalUtcMs=sample.timestamp,value=sample.value,quality=sample.quality,
            nominalOriginMonoMs=sample.nominal_origin_mono_ms,configGeneration=sample.config_generation,
            timeGeneration=sample.time_generation,timeValid=sample.time_valid,clockRevision=sample.clock_revision,timeSource=sample.time_source)

    def _decision(self,sample):
        if sample.capture_boot_id!=self.boot_id or sample.sample_seq not in self._pending_decisions:
            return False
        self._pending_decisions.remove(sample.sample_seq)
        return True

    def undecided(self,sample,reason='VERSION_DISCARD',*,overflow=False):
        with self.lock:
            if not self._decision(sample):return False
            if overflow:self._add('captureOverflowEventsTotal')
            self._add('undecidedLostEventsTotal');self.complete=False
            self._gap(reason,'UNCERTAIN',samples=(sample,))
        self.record('undecided_lost',sampleSeq=sample.sample_seq,reason=reason)
        return True

    def decided(self,sample,*,selected,admitted=False,reason=None):
        with self.lock:
            if not self._decision(sample):return False
            self._add('eligibleEventsTotal' if selected else 'policyFilteredEventsTotal')
            if admitted:self.pending+=1
            elif selected:
                self._add('knownUnsentEventsTotal')
                if reason=='TIME_INVALID':self._add('timeInvalidEventsTotal')
                if reason=='CAPACITY_EVICTION':self._add('enqueueRejectedAttemptsTotal')
                self._gap(reason or 'STORAGE_ERROR','KNOWN_UNSENT',samples=(sample,))
        self.record('decision',sampleSeq=sample.sample_seq,selected=selected,admitted=admitted,reason=reason)
        return True

    def _gap(self,reason,outcome,*,samples=(),entry=None,count=None):
        self._add('gapRecordsTotal')
        if len(self._gaps)==self._gaps.maxlen:
            self._gap_bytes-=retained_bytes(self._gaps.popleft())+64;self.truncated=True
        seqs=[s.sample_seq for s in samples] if samples else list(entry.events) if entry else []
        stamps=[s.timestamp for s in samples if s.timestamp is not None]
        devices={s.device_id for s in samples};points={s.point_id for s in samples}
        versions={s.config_version for s in samples}
        gap=dict(gapId='gap-'+uuid.uuid4().hex,reason=reason,outcome=outcome,
            eventCount=len(seqs) if seqs else count,captureBootId=samples[0].capture_boot_id if samples else entry.capture_boot if entry else None,
            configVersion=next(iter(versions)) if len(versions)==1 else None,
            deviceId=next(iter(devices)) if len(devices)==1 else None,
            pointId=next(iter(points)) if len(devices)==len(points)==1 else None,
            sampleSeqFrom=min(seqs) if seqs else None,sampleSeqTo=max(seqs) if seqs else None,
            sampledFromUtcMs=min(stamps) if stamps else None,sampledToUtcMs=max(stamps) if stamps else None)
        self._gaps.append(gap);self._gap_bytes+=retained_bytes(gap)+64;self._bound()
        self.record('gap',**gap)

    def recovery(self):
        with self.lock:
            if not self.recovery_noted:
                self.recovery_noted=True;self.complete=False
                self._gap('VOLATILE_RESTART','UNCERTAIN')

    def restored(self,entry):
        with self.lock:
            self.recovery()
            if entry.row['state']=='PENDING':
                self.pending_batches+=1;self.unknown_pending+=1
            else:self.dead_batches+=1;self.unknown_dead+=1
            self.payload_bytes+=entry.row['size'];self.revision+=1
        self.record('restored_batch',messageId=entry.row['message_id'],payloadHash=entry.row['hash'],
                    state=entry.row['state'],eventCount=None,captureBootId=None)

    def materialized(self,sequence,message_id):
        with self.lock:
            # Normal pipeline has at most eight. The larger accounting guard
            # also bounds diagnostic-only engines sharing a test boot context.
            if len(self._frozen_batches)>=64:
                self.complete=False;self.truncated=True
                raise RuntimeError('Frozen-batch accounting capacity exhausted')
            self._frozen_batches[sequence]=message_id
            self.pending_batches+=1;self.revision+=1;self._bound()

    def committed(self,entry):
        with self.lock:
            frozen=next((seq for seq,key in self._frozen_batches.items() if key==entry.row['message_id']),None)
            if frozen is not None:del self._frozen_batches[frozen]
            else:self.pending_batches+=1
            self.payload_bytes+=entry.row['size'];self.revision+=1
            if entry.capture_boot!=self.boot_id:
                self.unknown_pending+=1;self.complete=False
        self.record_batch('committed',entry)

    def terminal(self,entry,source):
        with self.lock:
            n=len(entry.events)
            self.pending_batches-=1;self.dead_batches+=1
            if entry.capture_boot==self.boot_id:
                self.pending-=n;self.dead+=n
                outcome='ISOLATED' if source=='PERMANENT_REJECT' else 'UNCERTAIN' if entry.ever_sent else 'KNOWN_UNSENT'
                counter={'ISOLATED':'isolatedEventsTotal','UNCERTAIN':'uncertainDeliveryEventsTotal','KNOWN_UNSENT':'knownUnsentEventsTotal'}[outcome]
                self._add(counter,n)
            else:
                self.unknown_pending-=1;self.unknown_dead+=1;outcome='UNCERTAIN';self.complete=False
            self._gap(source,outcome,entry=entry)
        self.record_batch('terminal',entry,source=source,outcome=outcome)

    def removed(self,entry,*,stored=False,pruned=False):
        with self.lock:
            dead=entry.row['state']=='DEAD';n=len(entry.events)
            if dead:self.dead_batches-=1
            else:self.pending_batches-=1
            if entry.capture_boot==self.boot_id:
                if dead:self.dead-=n
                else:self.pending-=n
                if stored:
                    self._add('storedConfirmedEventsTotal',n)
                    if entry.source=='AGE_EXPIRED':self._add('lateStoredEventsTotal',n)
            elif dead:self.unknown_dead-=1
            else:self.unknown_pending-=1
            self.payload_bytes-=entry.row['size'];self.revision+=1
            if pruned:self.truncated=True
        self.record_batch('stored' if stored else 'pruned',entry)

    def abandon(self,samples,sequences=()):
        with self.lock:
            self.pending-=len(samples);self._add('knownUnsentEventsTotal',len(samples))
            for seq in sequences:
                if seq in self._frozen_batches:
                    del self._frozen_batches[seq];self.pending_batches-=1
            for start in range(0,len(samples),256):
                self._gap('VOLATILE_RESTART','KNOWN_UNSENT',samples=samples[start:start+256])
        for sample in samples:self.record('ram_terminal',sampleSeq=sample.sample_seq,outcome='KNOWN_UNSENT',reason='VOLATILE_RESTART')

    def record_batch(self,kind,entry,**extra):
        # Lossless compact association, not an inclusive range. Fifteen maximum
        # safe integers plus commas fit the recorder's 256-character string cap.
        # This avoids one audit record per value for every batch transition.
        for start in range(0,len(entry.events),15):
            seqs=entry.events[start:start+15]
            identity=(dict(sampleSeq=seqs[0]) if len(seqs)==1 else
                      dict(sampleSeqs=','.join(str(seq) for seq in seqs),eventCount=len(seqs)))
            self.record(kind,messageId=entry.row['message_id'],payloadHash=entry.row['hash'],
                        captureBootId=entry.capture_boot,**identity,**extra)

    def snapshot(self,*,max_payload_bytes=64*1024*1024):
        with self.lock:
            if self.recorder is not None and (self.recorder.failed or self.recorder.rejected):
                self.complete=False;self.truncated=True
            return dict(counterEpoch=self.boot_id,revision=self.revision,
                mode='SQLITE',durability='BATCH_PERSISTENT',
                accountingComplete=self.complete,counterOverflow=self.overflow,detailTruncated=self.truncated,
                counters=dict(self.counters),buffer=dict(pendingEvents=None if self.unknown_pending else self.pending,
                    deadEvents=None if self.unknown_dead else self.dead,pendingBatches=self.pending_batches,
                    deadBatches=self.dead_batches,quotaKind='PAYLOAD_TOTAL',usedBytes=self.payload_bytes,
                    maxBytes=max_payload_bytes,oldestPendingAgeMs=None,ageUnknownBatches=self.pending_batches),
                decisionPending=None if self._decision_unknown else len(self._pending_decisions),
                lastGap=deepcopy(self._gaps[-1]) if self._gaps else None)
