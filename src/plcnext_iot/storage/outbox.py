"""Immutable v1 payload storage, monotonic retry, bounded same-run correlation."""
import hashlib
import json
from pathlib import Path

from plcnext_iot.contracts import validate_message
from plcnext_iot.config.store import StoreFormatError
from plcnext_iot.storage.database import Database
from plcnext_iot.storage.batch_repository import SqlBatches
from plcnext_iot.storage.outbox_session import OutboxSession
from plcnext_iot.storage.outbox_migration import META_EXTRA, BATCH_COLUMNS
from plcnext_iot.core.platform_time import TimeUnavailable, MAX_SAFE, ID


class OutboxStore(Database):
    def __init__(self,path,gateway,*,max_batches=10000,max_bytes=64*1024*1024,max_dead=128,
                 session=None,repository=None,time_provider=None):
        if any(type(v) is not int or v<1 for v in (max_batches,max_bytes,max_dead)):
            raise ValueError('Outbox limits must be positive integers')
        self.max_batches,self.max_bytes,self.max_dead=max_batches,max_bytes,max_dead
        self.session = session or OutboxSession()
        self.gateway = gateway
        self.time_provider = time_provider
        self._commit_guard = None
        self._injected = repository is not None
        if repository is None:
            SqlBatches.preflight(path)
            if self.session.accounting is not None and Path(path).exists():
                self.session.accounting.recovery()
            super().__init__(path,gateway,0x504F5554,{'batches':'''CREATE TABLE batches(
                message_id TEXT PRIMARY KEY,payload BLOB NOT NULL,hash TEXT NOT NULL,
                created REAL NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_due REAL NOT NULL DEFAULT 0,
                state TEXT NOT NULL CHECK(state IN ('PENDING','DEAD')),reason TEXT,'''+BATCH_COLUMNS+')'},
                format_version=2,meta_extra=META_EXTRA)
        try:
            if repository is None:
                self.c.execute('CREATE UNIQUE INDEX IF NOT EXISTS outbox_local_order ON batches(local_order)')
            self.repo = repository or SqlBatches(self)
            def bounded_rows():
                count=size=0
                for row in self.repo.rows():
                    row=dict(row)
                    row.setdefault('time_trust','LEGACY_UNVERIFIED')
                    if not self._injected and (type(row.get('local_order')) is not int or not 1<=row['local_order']<MAX_SAFE):
                        raise StoreFormatError('Stored local order is corrupt')
                    if row['time_trust'] not in ('LEGACY_UNVERIFIED','SYSTEM_UNVERIFIED','HUB_TRUSTED'):
                        raise StoreFormatError('Unknown batch time trust')
                    raw=self.repo.payload(row['message_id'])
                    if raw is None or hashlib.sha256(raw).hexdigest()!=row['hash'] or validate_message('data',raw,expected_gateway_id=gateway):
                        raise StoreFormatError('Stored telemetry batch is corrupt')
                    decoded=json.loads(raw)
                    if decoded['messageId']!=row['message_id']:
                        raise StoreFormatError('Stored batch identity is corrupt')
                    row['wire_timestamp']=decoded['timestamp']
                    if row['time_trust']=='HUB_TRUSTED' and (type(row.get('enqueued_utc_ms')) is not int
                            or not 0<=row['enqueued_utc_ms']<=MAX_SAFE or type(row.get('uncertainty_ms')) is not int
                            or not 0<=row['uncertainty_ms']<1000
                            or not isinstance(row.get('time_basis_ref'),str) or not ID.fullmatch(row['time_basis_ref'])
                            or not isinstance(row.get('enqueue_boot_id'),str) or not ID.fullmatch(row['enqueue_boot_id'])
                            or type(row.get('clock_revision')) is not int or not 0<=row['clock_revision']<=MAX_SAFE):
                        raise StoreFormatError('Stored trusted time metadata is corrupt')
                    count+=1;size+=row['size']
                    if count>max_batches or size>max_bytes:
                        raise StoreFormatError('Existing Outbox exceeds configured quota')
                    yield row
            self.session.attach(bounded_rows())
            if sum(e.row['state']=='DEAD' for e in self.session.entries.values())>self.max_dead:
                raise StoreFormatError('Existing Outbox exceeds DEAD quota')
        except BaseException:
            self.close()
            raise

    def close(self):
        if not getattr(self,'_injected',False):
            super().close()

    def reset_inflight(self):
        """Compatibility no-op: reconnect never resets monotonic deadlines."""

    def open_gate(self, watermark):
        self.session.open_gate(watermark)

    def _prune(self):
        dead = [(e.row['local_order'],key) for key,e in self.session.entries.items() if e.row['state']=='DEAD']
        for _,key in sorted(dead)[:max(0,len(dead)-self.max_dead)]:
            entry = self.session.entries[key]
            self.repo.prune(key)
            if self.session.accounting is not None:self.session.accounting.removed(entry,pruned=True)
            del self.session.entries[key]
            self.session.emit('pruned',key,entry)
            self.session.detail_truncated = True

    def _expire(self, now=None):
        # Wall-clock arguments are retained for API compatibility, never proof of age.
        if self.time_provider is None:
            return
        current=self.time_provider.snapshot()
        if not current.time_valid:
            return
        mono = self.session.clock()
        for key,entry in list(self.session.entries.items()):
            if key not in self.session.entries:
                continue
            row = entry.row
            comparable=(row.get('time_trust')=='HUB_TRUSTED'
                        and row.get('time_basis_ref')==current.time_basis_ref)
            lower=(max(0,current.utc_ms-current.uncertainty_ms-row['enqueued_utc_ms']-row['uncertainty_ms'])
                   if comparable else 0)
            if row['state']=='PENDING' and comparable and lower>=7*86400*1000 and not (entry.active and mono<entry.due):
                token=self.time_provider.token()
                self._commit_guard=lambda:self.time_provider.require(token,'ENQUEUE')
                try:
                    self._terminal(key,'EXPIRED_DATA','AGE_EXPIRED')
                finally:
                    self._commit_guard=None
        self._prune()

    def _terminal(self,key,reason,source):
        dead=[(e.row['local_order'],k) for k,e in self.session.entries.items() if k==key or e.row['state']=='DEAD']
        pruned=[k for _,k in sorted(dead)[:max(0,len(dead)-self.max_dead)]]
        if hasattr(self.repo,'terminal_with_source'):
            self.repo.terminal_with_source(key,reason,pruned,source)
        else:
            self.repo.terminal(key,reason,pruned)
        self.session.entries[key].row.update(state='DEAD',reason=reason,terminal_source=source)
        self.session.terminal(key,source)
        for old in pruned:
            entry=self.session.entries.pop(old)
            if self.session.accounting is not None:self.session.accounting.removed(entry,pruned=True)
            self.session.emit('pruned',old,entry)
            self.session.detail_truncated=True

    def enqueue(self,payload,now=None,association=None,time_token=None):
        current=None
        if self.time_provider is not None:
            current=self.time_provider.require(time_token,'ENQUEUE')
        if validate_message('data',payload,expected_gateway_id=self.gateway):
            raise ValueError('Invalid telemetry batch')
        wire = json.loads(payload); key=wire['messageId']
        if key in self.session.entries:
            if self.repo.payload(key)!=payload:
                raise ValueError('Batch identity conflict')
            return True
        self._expire(now)
        entries = self.session.entries
        if len(entries)>=self.max_batches or sum(e.row['size'] for e in entries.values())+len(payload)>self.max_bytes:
            self.session.add_fact('enqueue_rejected_attempts')
            if self.session.accounting is not None:self.session.accounting.add('enqueueRejectedAttemptsTotal')
            self.repo.count_attempt('dropped')
            return False
        order=(self.repo.next_order() if hasattr(self.repo,'next_order') else
               max(getattr(self.session,'next_order',1),max((e.row['local_order'] for e in entries.values()),default=0)+1))
        if type(order) is not int or not 1<=order<MAX_SAFE:
            raise StoreFormatError('Outbox local order exhausted')
        trusted=current is not None and current.time_valid
        row = dict(message_id=key,hash=hashlib.sha256(payload).hexdigest(),created=now if now is not None else 0,size=len(payload),
                   attempts=0,next_due=0,state='PENDING',reason=None,
                   local_order=order,enqueued_utc_ms=current.utc_ms if trusted else None,
                   uncertainty_ms=current.uncertainty_ms if trusted else None,
                   time_trust='HUB_TRUSTED' if trusted else 'SYSTEM_UNVERIFIED',
                   time_basis_ref=current.time_basis_ref if trusted else None,
                   enqueue_boot_id=current.boot_id if current is not None else None,
                   clock_revision=current.clock_revision if current is not None else None,terminal_source=None,
                   wire_timestamp=wire['timestamp'])
        association = association or {}
        events = association.get('events',())
        category = 'history' if events and max(events)<=self.session.watermark else 'new'
        entry = self.session.stage(key,row,events,association.get('capture_boot'),category)
        if entry is None:
            if self.session.accounting is not None:self.session.accounting.add('enqueueRejectedAttemptsTotal')
            return False
        try:
            self._commit_guard=(lambda:self.time_provider.require(time_token,'ENQUEUE')) if current is not None else None
            # Injected repositories also receive an explicit commit-decision guard.
            if self._injected and self._commit_guard is not None:
                self._commit_guard()
            self.repo.insert(row,payload)
        except Exception:
            if self.session.accounting is not None:self.session.accounting.add('enqueueRejectedAttemptsTotal')
            raise
        finally:
            self._commit_guard=None
        self.session.next_order=order+1
        entry.row = row
        self.session.staged.pop(key)
        entries[key] = entry
        if self.session.accounting is not None:self.session.accounting.committed(entry)
        self.session.emit('committed',key,entry)
        return True

    def reserve(self, now=None, prefer_new=None,time_token=None):
        if self.time_provider is not None:
            self.time_provider.require(time_token,'SEND')
        session=self.session; mono=session.clock()
        self._expire()
        while session.rate and mono-session.rate[0][0]>=1:
            session.rate.popleft()
        if mono<session.next_send or len(session.rate)>=4:
            return None
        entries=list(session.entries.values())
        for e in entries:
            if e.active and mono>=e.due:
                e.active=False
        active=[e for e in entries if e.active]
        if len(active)>=8:
            return None
        eligible={c:[] for c in ('new','history')}
        for e in entries:
            if e.row['state']=='PENDING' and not e.active and e.due<=mono:
                if self.time_provider is not None and self.time_provider.platform:
                    current=self.time_provider.require(time_token,'SEND')
                    header=e.row['wire_timestamp']
                    # Unknown legacy age is not expiry. A known future/rollback obstacle is maintenance.
                    if (e.row.get('time_trust')=='HUB_TRUSTED' and e.row.get('time_basis_ref')!=current.time_basis_ref
                            or header>current.utc_ms+current.uncertainty_ms+300000):
                        continue
                eligible[e.category].append(e)
        for c in eligible:
            if sum(e.category==c for e in active)>=7:
                eligible[c]=[]
        category='new' if session.prefer_new else 'history'
        if not eligible[category]:
            category='history' if category=='new' else 'new'
        if not eligible[category]:
            return None
        e=min(eligible[category],key=lambda e:e.row['local_order']); key=e.row['message_id']
        payload=self.repo.payload(key)
        if payload is None or hashlib.sha256(payload).hexdigest()!=e.row['hash'] or validate_message('data',payload,expected_gateway_id=self.gateway):
            session.accounting_complete=False
            if session.accounting is not None:session.accounting.incomplete()
            raise StoreFormatError('Stored telemetry batch is corrupt')
        if sum(size for _,size in session.rate)+len(payload)>512*1024:
            return None
        attempts=min(e.row['attempts']+1,9007199254740991)
        self._commit_guard=(lambda:self.time_provider.require(time_token,'SEND')) if self.time_provider is not None else None
        try:
            self.repo.update(key,attempts=attempts,next_due=0)
        finally:
            self._commit_guard=None
        mono=session.clock()
        e.row['attempts']=attempts
        e.previous_sent=e.ever_sent
        e.due=mono+(30 if attempts==1 else 60);e.active=True;e.ever_sent=True
        session.next_send=mono+0.25;session.rate.append((mono,len(payload)))
        session.prefer_new=category!='new'
        session.emit('reserved',key,e,attempt=attempts,category=category)
        return payload

    def submission_failed(self, message_id):
        """SDK definitively rejected this attempt; retain its retry deadline."""
        e=self.session.entries.get(message_id)
        if e is not None and e.row['state']=='PENDING':
            e.ever_sent=e.previous_sent;e.active=False
            self.session.emit('sdk_rejected',message_id,e)

    def acknowledge(self,payload,now=None):
        if validate_message('dataAck',payload,expected_gateway_id=self.gateway):
            return False
        ack=json.loads(payload);key=ack['messageId'];session=self.session
        e=session.entries.get(key)
        if e is None or not e.row['attempts']:
            session.unknown_ack();return False
        raw=self.repo.payload(key)
        if raw is None or hashlib.sha256(raw).hexdigest()!=e.row['hash'] or validate_message('data',raw,expected_gateway_id=self.gateway):
            raise StoreFormatError('Stored ACK association is corrupt')
        if ack['status']=='STORED':
            if e.row['state']=='DEAD' and (e.source!='AGE_EXPIRED' or not e.ever_sent):
                session.add_fact('conflicting_acks')
                session.emit('conflict',key,e)
                return False
            self.repo.delete(key)
            if session.accounting is not None:session.accounting.removed(e,stored=True)
            session.add_fact('stored',len(e.events))
            if e.source=='AGE_EXPIRED':
                session.add_fact('late_stored',len(e.events))
            session.emit('stored',key,e,late=e.source=='AGE_EXPIRED')
            del session.entries[key]
            return True
        if e.row['state']=='DEAD':
            return False
        codes=list(dict.fromkeys(err['code'] for err in ack['errors']))
        if codes and all(c in {'UNKNOWN_CONFIG_VERSION','RESOURCE_LIMIT','STORAGE_ERROR'} for c in codes):
            e.due=session.clock()+60;e.active=False
            session.emit('retryable_reject',key,e)
        else:
            self._terminal(key,','.join(codes),'PERMANENT_REJECT')
        return True

    def stats(self):
        entries=list(self.session.entries.values())
        current=self.time_provider.snapshot() if self.time_provider is not None else None
        return dict(pending=sum(e.row['state']=='PENDING' for e in entries),
                    dead=sum(e.row['state']=='DEAD' for e in entries),
                    payload_bytes=sum(e.row['size'] for e in entries),
                    age_unknown_batches=sum(e.row.get('time_trust')!='HUB_TRUSTED' or current is None or not current.time_valid
                        or e.row.get('time_basis_ref')!=current.time_basis_ref for e in entries),
                    maintenance_batches=sum(e.row['state']=='PENDING' and current is not None and current.time_valid and (
                        e.row.get('time_trust')=='HUB_TRUSTED' and e.row.get('time_basis_ref')!=current.time_basis_ref
                        or e.row['wire_timestamp']>current.utc_ms+current.uncertainty_ms+300000) for e in entries),
                    **self.repo.counters())

    def time_boundary(self):
        if hasattr(self.repo,'time_boundary'):
            basis,floor=self.repo.time_boundary()
            return dict(basis=basis,utc_lower=floor)
        return dict(basis=None,utc_lower=None)
