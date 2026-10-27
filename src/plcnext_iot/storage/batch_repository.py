"""Version-2 SQL adapter. Policy accepts a repository for no-database tests."""
import shutil
from pathlib import Path

from plcnext_iot.config.store import StoreError


class SqlBatches:
    @staticmethod
    def preflight(path):
        path=Path(path).resolve()
        parent=path.parent
        while not parent.exists():
            parent=parent.parent
        total=sum(p.stat().st_size for p in (path,path.with_name(path.name+'-wal'),path.with_name(path.name+'-shm')) if p.exists())
        if total+2*1024*1024>256*1024*1024 or shutil.disk_usage(parent).free<66*1024*1024:
            raise StoreError('Telemetry physical storage reserve exhausted before open')

    def __init__(self, database, *, max_physical_bytes=256*1024*1024, min_free_bytes=64*1024*1024):
        self.db = database
        self.max_physical_bytes, self.min_free_bytes = max_physical_bytes, min_free_bytes
        page = self.db.c.execute('PRAGMA page_size').fetchone()[0]
        maximum=max_physical_bytes//2//page
        actual=self.db.c.execute(f'PRAGMA max_page_count={maximum}').fetchone()[0]
        if actual>maximum:
            raise StoreError('Existing telemetry DB exceeds page budget')
        self.db.c.execute('PRAGMA wal_autocheckpoint=64')
        self.db.c.execute('PRAGMA journal_size_limit=0')

    def guard(self):
        path = self.db.path
        total = sum(p.stat().st_size for p in (path, path.with_name(path.name+'-wal'),
                    path.with_name(path.name+'-shm')) if p.exists())
        if total+2*1024*1024 > self.max_physical_bytes or shutil.disk_usage(path.parent).free < self.min_free_bytes+2*1024*1024:
            raise StoreError('Telemetry physical storage reserve exhausted')
        busy, _, _ = self.db.c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
        if busy:
            raise StoreError('Telemetry WAL cannot be bounded while a reader holds it')

    def rows(self):
        for row in self.db.c.execute('''SELECT local_order,message_id,hash,created,attempts,
            next_due,state,reason,length(payload) AS size,enqueued_utc_ms,uncertainty_ms,time_trust,
            time_basis_ref,enqueue_boot_id,clock_revision,terminal_source FROM batches ORDER BY local_order'''):
            yield dict(row)

    def next_order(self):
        return self.db.c.execute('SELECT next_order FROM meta WHERE id=1').fetchone()[0]

    def time_boundary(self):
        return self.db.c.execute('SELECT trusted_basis_ref,trusted_utc_high_water_ms FROM meta WHERE id=1').fetchone()

    def _remember_time(self,row):
        if row.get('time_trust') == 'HUB_TRUSTED':
            old_basis, old_floor = self.time_boundary()
            if old_basis is not None and old_basis != row['time_basis_ref']:
                raise StoreError('Outbox time basis requires explicit maintenance')
            floor = max(0,row['enqueued_utc_ms']-row['uncertainty_ms'])
            self.db.c.execute('UPDATE meta SET trusted_basis_ref=?,trusted_utc_high_water_ms=? WHERE id=1',
                             (row['time_basis_ref'],max(floor,old_floor or 0)))

    def payload(self, message_id):
        row = self.db.c.execute('SELECT payload FROM batches WHERE message_id=?',(message_id,)).fetchone()
        return None if row is None else bytes(row[0])

    def insert(self, row, payload):
        self.guard()
        guard=getattr(self.db,'_commit_guard',None)
        def accepted():
            current=guard() if guard is not None else None
            if current is not None and row.get('time_trust')=='HUB_TRUSTED':
                row.update(enqueued_utc_ms=current.utc_ms,uncertainty_ms=current.uncertainty_ms)
                self.db.c.execute('UPDATE batches SET enqueued_utc_ms=?,uncertainty_ms=? WHERE message_id=?',
                                 (current.utc_ms,current.uncertainty_ms,row['message_id']))
                self._remember_time(row)
        with self.db.transaction(commit_guard=accepted):
            self.db.c.execute("INSERT INTO batches(message_id,payload,hash,created,state,local_order,enqueued_utc_ms,uncertainty_ms,time_trust,time_basis_ref,enqueue_boot_id,clock_revision) VALUES(?,?,?,?,'PENDING',?,?,?,?,?,?,?)",
                (row['message_id'],payload,row['hash'],row['created'],row['local_order'],row.get('enqueued_utc_ms'),
                 row.get('uncertainty_ms'),row.get('time_trust','LEGACY_UNVERIFIED'),row.get('time_basis_ref'),
                 row.get('enqueue_boot_id'),row.get('clock_revision')))
            self.db.c.execute('UPDATE meta SET next_order=? WHERE id=1',(row['local_order']+1,))

    def update(self, message_id, **fields):
        if not set(fields) <= {'attempts','next_due','state','reason'}:
            raise ValueError('Unsupported batch mutation')
        self.guard()
        with self.db.transaction(commit_guard=getattr(self.db,'_commit_guard',None)):
            self.db.c.execute('UPDATE batches SET '+','.join(k+'=?' for k in fields)+' WHERE message_id=?',
                              (*fields.values(),message_id))

    def delete(self, message_id):
        self.guard()
        with self.db.transaction():
            self.db.c.execute('DELETE FROM batches WHERE message_id=?',(message_id,))

    def prune(self, message_id):
        self.guard()
        with self.db.transaction():
            self.db.c.execute('DELETE FROM batches WHERE message_id=?',(message_id,))
            self.db.c.execute('UPDATE meta SET dead_pruned=min(dead_pruned+1,9007199254740991) WHERE id=1')

    def terminal(self, message_id, reason, pruned):
        return self.terminal_with_source(message_id,reason,pruned,None)

    def terminal_with_source(self, message_id, reason, pruned, source):
        self.guard()
        with self.db.transaction(commit_guard=getattr(self.db,'_commit_guard',None)):
            self.db.c.execute("UPDATE batches SET state='DEAD',reason=?,terminal_source=? WHERE message_id=?",(reason,source,message_id))
            for key in pruned:
                self.db.c.execute('DELETE FROM batches WHERE message_id=?',(key,))
            if pruned:
                self.db.c.execute('UPDATE meta SET dead_pruned=min(dead_pruned+?,9007199254740991) WHERE id=1',(len(pruned),))

    def count_attempt(self, name):
        if name not in ('dropped','dead_pruned'):
            raise ValueError('Unsupported legacy counter')
        self.guard()
        with self.db.transaction():
            self.db.c.execute(f'UPDATE meta SET {name}=min({name}+1,9007199254740991) WHERE id=1')

    def counters(self):
        return dict(self.db.c.execute('SELECT dropped,dead_pruned FROM meta WHERE id=1').fetchone())
