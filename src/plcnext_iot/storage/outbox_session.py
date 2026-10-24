"""Bounded process-owned scheduling/correlation. Monotonic times never persist."""
from array import array
from collections import deque
from dataclasses import dataclass, field
import time

from plcnext_iot.core.bounds import retained_bytes
from plcnext_iot.core.boot import MAX_SEQUENCE
from plcnext_iot.config.store import StoreError,StoreFormatError


@dataclass(slots=True)
class BatchState:
    row: dict
    category: str = 'history'
    due: float = 0
    active: bool = False
    source: str | None = None
    events: array = field(default_factory=lambda: array('Q'))
    capture_boot: str | None = None
    ever_sent: bool = False
    previous_sent: bool = False


class OutboxSession:
    def __init__(self, *, clock=time.monotonic, max_bytes=4*1024*1024, max_records=10000, on_fact=None,accounting=None):
        self.clock, self.max_bytes, self.max_records = clock, max_bytes, max_records
        self.accounting=accounting
        if accounting is not None:self.max_bytes-=accounting.max_metadata_bytes
        self.entries = {}
        self.staged = {}
        self.watermark = 0
        self.prefer_new = True
        self.next_send = 0
        self.rate = deque(maxlen=4)
        self.sdk_next_send = 0
        self.sdk_rate = deque(maxlen=4)
        self.on_fact = on_fact
        self.detail_truncated = False
        self.accounting_complete = True
        self.counter_overflow = False
        self.facts = dict(stored=0,known_unsent=0,isolated=0,uncertain=0,late_stored=0,
                          unknown_acks=0,conflicting_acks=0,enqueue_rejected_attempts=0)

    def charge(self, entry):
        # Reserve terminal-reason growth and container/array overhead up front.
        row=dict(entry.row,reason=None)
        return retained_bytes(row)+len(entry.events)*8+1536

    def add_fact(self,name,count=1):
        value=self.facts[name]+count
        if value>MAX_SEQUENCE:
            self.counter_overflow=True;self.accounting_complete=False
        self.facts[name]=min(value,MAX_SEQUENCE)

    @property
    def used_bytes(self):
        return sum(self.charge(e) for e in self.entries.values())+sum(self.charge(e) for e in self.staged.values())

    def stage(self, message_id, row, events=(), capture_boot=None, category='new'):
        old = self.entries.get(message_id) or self.staged.get(message_id)
        if old is not None:
            return old
        entry = BatchState(row, category, events=array('Q',events), capture_boot=capture_boot)
        if len(self.entries)+len(self.staged)>=self.max_records or self.used_bytes+self.charge(entry)>self.max_bytes:
            return None
        self.staged[message_id] = entry
        return entry

    def attach(self, rows):
        ids=set()
        for row in rows:
            key = row['message_id']
            ids.add(key)
            if key in self.entries:
                if self.entries[key].row['hash'] != row['hash']:
                    raise StoreFormatError('Outbox process identity conflict')
                self.entries[key].row = row
                continue
            entry = self.stage(key,row,category='history')
            if entry is None:
                raise StoreError('Outbox metadata quota exceeded while opening')
            self.staged.pop(key)
            entry.due = self.clock()+(60 if row['attempts'] else 0)
            entry.ever_sent = row['attempts']>0
            self.entries[key] = entry
            self.accounting_complete = False
            if self.accounting is not None:self.accounting.restored(entry)
        for key in list(self.entries):
            if key not in ids:
                self.accounting_complete=False
                raise StoreFormatError('Outbox records disappeared within the current process')

    def open_gate(self, watermark):
        self.watermark = watermark
        for entry in (*self.entries.values(),*self.staged.values()):
            entry.category = 'history'

    def emit(self, kind, key, entry, **extra):
        if self.accounting is not None and kind in ('reserved','sdk_rejected','retryable_reject','conflict'):
            self.accounting.record_batch(kind,entry,**extra)
        if self.on_fact is not None:
            try:
                result=self.on_fact(dict(kind=kind,message_id=key,hash=entry.row['hash'],
                    capture_boot=entry.capture_boot,sample_seqs=tuple(entry.events),**extra))
                if result is False:
                    self.detail_truncated=True;self.accounting_complete=False
            except Exception:
                self.detail_truncated = True
                self.accounting_complete = False

    def terminal(self, key, source):
        entry = self.entries[key]
        if entry.source is not None:
            return
        entry.source, entry.active = source, False
        fact = 'isolated' if source=='PERMANENT_REJECT' else 'uncertain' if entry.ever_sent else 'known_unsent'
        self.add_fact(fact,len(entry.events))
        if self.accounting is not None:self.accounting.terminal(entry,source)
        self.emit('terminal',key,entry,source=source,outcome=fact)

    def unknown_ack(self):
        self.add_fact('unknown_acks')
