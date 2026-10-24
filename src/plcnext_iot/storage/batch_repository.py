"""Version-1 SQL adapter. Policy accepts a repository for no-database tests."""
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
        for row in self.db.c.execute('''SELECT rowid AS local_order,message_id,hash,created,attempts,
            next_due,state,reason,length(payload) AS size FROM batches ORDER BY rowid'''):
            yield dict(row)

    def payload(self, message_id):
        row = self.db.c.execute('SELECT payload FROM batches WHERE message_id=?',(message_id,)).fetchone()
        return None if row is None else bytes(row[0])

    def insert(self, row, payload):
        self.guard()
        with self.db.transaction():
            self.db.c.execute("INSERT INTO batches(message_id,payload,hash,created,state) VALUES(?,?,?,?,'PENDING')",
                (row['message_id'],payload,row['hash'],row['created']))

    def update(self, message_id, **fields):
        if not set(fields) <= {'attempts','next_due','state','reason'}:
            raise ValueError('Unsupported batch mutation')
        self.guard()
        with self.db.transaction():
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
        self.guard()
        with self.db.transaction():
            self.db.c.execute("UPDATE batches SET state='DEAD',reason=? WHERE message_id=?",(reason,message_id))
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
