"""Explicit v1 -> v2 migration of an EXISTING Outbox, with a persistent backup."""
import hashlib
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3

from plcnext_iot.config.locking import StoreLock
from plcnext_iot.config.store import StoreFormatError, StoreIdentityError
from plcnext_iot.contracts import validate_message

APP_ID = 0x504F5554
COLUMNS = {
    'local_order': 'INTEGER',
    'enqueued_utc_ms': 'INTEGER',
    'uncertainty_ms': 'INTEGER',
    'time_trust': "TEXT NOT NULL DEFAULT 'LEGACY_UNVERIFIED'",
    'time_basis_ref': 'TEXT',
    'enqueue_boot_id': 'TEXT',
    'clock_revision': 'INTEGER',
    'terminal_source': 'TEXT',
}
META_EXTRA = ',next_order INTEGER NOT NULL DEFAULT 1,trusted_basis_ref TEXT,trusted_utc_high_water_ms INTEGER'
BATCH_COLUMNS = ','.join(name+' '+definition for name,definition in COLUMNS.items())


def migrate_existing(path, gateway, backup, *, before_commit=None):
    """No new test DB, implicit upgrade, downgrade, deletion or fallback initialization."""
    path, backup = Path(path).resolve(), Path(backup).resolve()
    if not path.is_file() or path == backup or backup.exists() or not backup.parent.is_dir():
        raise ValueError('Existing DB and a new persistent backup path are required')
    with closing(StoreLock(path.with_suffix(path.suffix+'.lock'))):
        db = sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,isolation_level=None)
        try:
            if (db.execute('PRAGMA application_id').fetchone()[0] != APP_ID
                    or db.execute('PRAGMA user_version').fetchone()[0] != 1
                    or {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")} != {'meta','batches'}
                    or db.execute('PRAGMA quick_check').fetchone()[0] != 'ok'):
                raise StoreFormatError('Migration requires intact Outbox v1')
            if db.execute('SELECT gateway FROM meta WHERE id=1').fetchone() != (gateway,):
                raise StoreIdentityError('Migration gateway mismatch')
            old_columns = {r[1] for r in db.execute('PRAGMA table_info(batches)')}
            if old_columns != {'message_id','payload','hash','created','attempts','next_due','state','reason'}:
                raise StoreFormatError('Unknown v1 batch layout')
            counters=db.execute('SELECT dropped,dead_pruned FROM meta WHERE id=1').fetchone()
            if any(type(v) is not int or not 0<=v<=9007199254740991 for v in counters):
                raise StoreFormatError('Invalid v1 counters')
            for order,key,raw,digest,attempts,state in db.execute('SELECT rowid,message_id,payload,hash,attempts,state FROM batches'):
                if (not isinstance(raw,bytes) or hashlib.sha256(raw).hexdigest()!=digest
                        or validate_message('data',raw,expected_gateway_id=gateway)
                        or json.loads(raw)['messageId']!=key or not 1<=order<9007199254740991
                        or type(attempts) is not int or not 0<=attempts<=9007199254740991 or state not in ('PENDING','DEAD')):
                    raise StoreFormatError('Invalid v1 batch; retain source for explicit repair')
            # SQLite backup includes committed WAL content; raw file-copy would not.
            fd = os.open(backup, os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o600)
            os.close(fd)
            target = sqlite3.connect(backup)
            try:
                db.backup(target)
                if target.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise StoreFormatError('Backup verification failed')
            finally:
                target.close()
            with backup.open('r+b') as stream:
                os.fsync(stream.fileno())
            manifest = dict(source=str(path), backup=str(backup), gatewayId=gateway, oldVersion=1,
                            backupSha256=hashlib.sha256(backup.read_bytes()).hexdigest(),
                            batches=db.execute('SELECT count(*) FROM batches').fetchone()[0])
            manifest_path = backup.with_name(backup.name+'.json')
            with manifest_path.open('x',encoding='utf-8') as stream:
                json.dump(manifest, stream, indent=2);stream.flush();os.fsync(stream.fileno())
            db.execute('BEGIN IMMEDIATE')
            try:
                for name, definition in COLUMNS.items():
                    db.execute(f'ALTER TABLE batches ADD COLUMN {name} {definition}')
                db.execute('UPDATE batches SET local_order=rowid')
                db.execute('CREATE UNIQUE INDEX outbox_local_order ON batches(local_order)')
                db.execute('ALTER TABLE meta ADD COLUMN next_order INTEGER NOT NULL DEFAULT 1')
                db.execute('ALTER TABLE meta ADD COLUMN trusted_basis_ref TEXT')
                db.execute('ALTER TABLE meta ADD COLUMN trusted_utc_high_water_ms INTEGER')
                db.execute('UPDATE meta SET next_order=(SELECT coalesce(max(local_order),0)+1 FROM batches)')
                db.execute('PRAGMA user_version=2')
                if before_commit is not None:
                    before_commit()
                db.execute('COMMIT')
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
            return dict(manifest, newVersion=2, committed=True)
        finally:
            db.close()
