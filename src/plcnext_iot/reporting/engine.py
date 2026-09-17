"""Reporting policy uses monotonic time; only durable batches advance baselines."""
from dataclasses import dataclass, replace
import uuid

from plcnext_iot.messaging.messages import encode
from plcnext_iot.points.samples import Sample


@dataclass(frozen=True)
class ReportBatch:
    payload: bytes
    version: int
    samples: tuple[Sample, ...]


class ReportEngine:
    def __init__(self,gateway,boot_id):
        self.gateway,self.boot_id=gateway,boot_id
        self.sequence=0
        self.snapshot=None
        self.points={}
        self.latest={}
        self.baselines={}
        self.good_at={}
        self.started=0
        self.next_batch=0
        self.discarded=0
        self.cache_reset_points=0

    def configure(self,snapshot,now):
        if snapshot is self.snapshot or snapshot==self.snapshot:
            return
        self.cache_reset_points+=len(self.latest)
        self.snapshot=snapshot
        self.points={(d.device_id,p.point_id):p for d in snapshot.devices for p in d.points
                     if snapshot.enabled and d.enabled and p.enabled} if snapshot else {}
        self.latest.clear()
        self.baselines.clear()
        self.good_at.clear()
        self.started=now
        self.next_batch=now+(snapshot.report.batch_interval_ms/1000 if snapshot else 1)

    def accept(self,sample,now):
        key=(sample.device_id,sample.point_id)
        if not self.snapshot or sample.config_version!=self.snapshot.config_version or key not in self.points:
            self.discarded+=1
            return False
        self.latest[key]=sample
        if sample.quality=='GOOD':
            self.good_at[key]=min(now,sample.monotonic_time) if sample.monotonic_time is not None else now
        return True

    def _selected(self,now,wall_ms):
        selected=[]
        for key,p in self.points.items():
            sample=self.latest.get(key) or Sample(self.snapshot.config_version,*key,wall_ms,None,'UNKNOWN')
            if now-self.good_at.get(key,self.started)>=p.stale_after_ms/1000:
                sample=replace(sample,value=None,quality='STALE')
            baseline=self.baselines.get(key)
            if baseline is None:
                selected.append(sample)
                continue
            old,sent_at=baseline
            quality_changed=sample.quality!=old.quality
            changed=False
            if sample.quality=='GOOD':
                if type(sample.value) is bool:
                    changed=sample.value!=old.value
                elif old.value is not None:
                    changed=abs(sample.value-old.value)>p.deadband
            cyclic=p.report_mode in ('cyclic','change_or_cyclic') and now-sent_at>=p.report_interval_ms/1000
            change=p.report_mode in ('change','change_or_cyclic') and changed
            if quality_changed or cyclic or change:
                selected.append(sample)
        return selected

    def prepare(self,now,wall_ms,*,force=False):
        if not self.snapshot or not self.points or (not force and now<self.next_batch):
            return []
        self.next_batch=now+self.snapshot.report.batch_interval_ms/1000
        selected=self._selected(now,wall_ms)
        batches=[]
        limit=min(500,self.snapshot.report.max_batch_points)
        pending=[]
        size=512  # Conservative bounded envelope allowance (IDs <=64).
        def flush():
            if not pending:
                return
            self.sequence+=1
            values=[{'deviceId':s.device_id,'pointId':s.point_id,'timestamp':s.timestamp,
                     'value':s.value if s.quality=='GOOD' else None,'quality':s.quality} for s in pending]
            payload=encode({'schemaVersion':1,'gatewayId':self.gateway,'timestamp':wall_ms,
                'messageId':'batch-'+uuid.uuid4().hex,'bootId':self.boot_id,'sequence':self.sequence,
                'configVersion':self.snapshot.config_version,'values':values})
            if len(payload)>131072:
                raise ValueError('Telemetry batch exceeds byte limit')
            batches.append(ReportBatch(payload,self.snapshot.config_version,tuple(pending)))
        for sample in selected:
            value={'deviceId':sample.device_id,'pointId':sample.point_id,'timestamp':sample.timestamp,
                   'value':sample.value if sample.quality=='GOOD' else None,'quality':sample.quality}
            length=len(encode(value))+1
            if pending and (len(pending)>=limit or size+length>131072):
                flush()
                pending=[]
                size=512
            pending.append(sample)
            size+=length
        flush()
        return batches

    def committed(self,batch,now):
        if self.snapshot is None or batch.version!=self.snapshot.config_version:
            return
        for sample in batch.samples:
            self.baselines[(sample.device_id,sample.point_id)]=(sample,now)
