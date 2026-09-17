"""Single-owner SQLite setup shared by telemetry stores."""
from contextlib import contextmanager
from pathlib import Path
import re
import sqlite3

from plcnext_iot.config.locking import StoreLock
from plcnext_iot.config.store import StoreError,StoreFormatError,StoreIdentityError


class Database:
    def __init__(self,path,gateway,application_id,tables):
        if not isinstance(gateway,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}',gateway):
            raise ValueError('Invalid gateway')
        self.gateway=gateway
        self.path=Path(path).resolve()
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self._lock=StoreLock(self.path.with_suffix(self.path.suffix+'.lock'))
        self.c=None
        try:
            self.c=sqlite3.connect(self.path,isolation_level=None,timeout=5)
            self.c.row_factory=sqlite3.Row
            app=self.c.execute('PRAGMA application_id').fetchone()[0]
            version=self.c.execute('PRAGMA user_version').fetchone()[0]
            names={r[0] for r in self.c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            new=app==0 and version==0 and not names
            if not new and (app!=application_id or version!=1 or names!=set(tables)|{'meta'}):
                raise StoreFormatError('Unknown telemetry database format')
            if not new:
                row=self.c.execute('SELECT gateway FROM meta WHERE id=1').fetchone()
                if row is None or row[0]!=gateway:
                    raise StoreIdentityError('Telemetry database gateway identity is invalid')
            if self.c.execute('PRAGMA quick_check').fetchone()[0]!='ok':
                raise StoreFormatError('Telemetry database integrity check failed')
            self.c.execute('PRAGMA journal_mode=WAL')
            self.c.execute('PRAGMA synchronous=FULL')
            with self.transaction():
                if new:
                    self.c.execute('CREATE TABLE meta(id INTEGER PRIMARY KEY CHECK(id=1),gateway TEXT NOT NULL,dropped INTEGER NOT NULL,dead_pruned INTEGER NOT NULL)')
                    self.c.execute('INSERT INTO meta VALUES(1,?,0,0)',(gateway,))
                    for statement in tables.values():
                        self.c.execute(statement)
                    self.c.execute(f'PRAGMA application_id={application_id}')
                    self.c.execute('PRAGMA user_version=1')
        except BaseException as exc:
            self.close()
            if isinstance(exc,sqlite3.Error):
                raise StoreError('Telemetry database could not be opened') from None
            raise

    @contextmanager
    def transaction(self):
        try:
            self.c.execute('BEGIN IMMEDIATE')
            yield
            self.c.execute('COMMIT')
        except BaseException as exc:
            if self.c.in_transaction:
                self.c.rollback()
            if isinstance(exc,sqlite3.Error):
                raise StoreError('Telemetry transaction failed') from None
            raise

    def close(self):
        try:
            if self.c is not None:
                self.c.close()
                self.c=None
        finally:
            self._lock.close()
