"""Opt-in migration/restart verification on the existing idle setup-preview DB.

Refuses any non-empty/other database. Restores its original consistent v1 image
in finally. Backup files are retained for recovery; no new test DB is created.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'tests')]
from plcnext_iot.config.locking import StoreLock
from plcnext_iot.core.platform_time import PlatformTime,TimeUnavailable
from plcnext_iot.storage.outbox import OutboxStore
from plcnext_iot.storage.outbox_session import OutboxSession
from plcnext_iot.storage.outbox_migration import migrate_existing
from test_platform_time import Clock,BASE,sync,ready
from test_phase1_outbox import payload,ack

GATEWAY='PLC-UI-DEMO'
def connect(path):return sqlite3.connect(path.resolve().as_uri()+'?mode=rw',uri=True,isolation_level=None)
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def image(db):
    return dict(version=db.execute('PRAGMA user_version').fetchone()[0],
                app=db.execute('PRAGMA application_id').fetchone()[0],
                dump='\n'.join(db.iterdump()),integrity=db.execute('PRAGMA quick_check').fetchone()[0])
def wire(key):
    value=json.loads(payload(key));value.update(gatewayId=GATEWAY,timestamp=BASE)
    return json.dumps(value,separators=(',',':')).encode()
def clock(boot):
    c=Clock();p=PlatformTime(GATEWAY,boot,basis='site-hub-1',drift_ppm=50,drift_evidence='TEST_FIXTURE_ONLY',
        mono=c.mono,system=c.system,jitter=lambda a,b:(a+b)/2)
    p.connect('session-one');sync(p,c);ready(p);return c,p


def run(output):
    path=ROOT/'.local/setup-preview/telemetry.db';agent=path.parent/'agent.db'
    if not path.is_file() or not agent.is_file():raise RuntimeError('Existing preview databases required')
    output.mkdir(parents=True,exist_ok=False)
    original_backup=output/'original-v1.backup';seeded_backup=output/'seeded-v1.backup'
    checks=[];store=None;original=None;report=dict(database=str(path),checks=checks,restored=False)
    agent_hash=digest(agent)
    with closing(StoreLock(path.with_suffix('.db.lock'))), closing(connect(path)) as db:
        original=image(db)
        if (original['version']!=1 or original['app']!=0x504F5554 or original['integrity']!='ok'
                or db.execute('SELECT gateway FROM meta WHERE id=1').fetchone()!=(GATEWAY,)
                or db.execute('SELECT count(*) FROM batches').fetchone()!=(0,)):
            raise RuntimeError('Only the existing empty v1 preview Outbox may be used')
    try:
        def interrupted():raise InterruptedError('Injected before migration COMMIT')
        try:migrate_existing(path,GATEWAY,original_backup,before_commit=interrupted)
        except InterruptedError:pass
        else:raise AssertionError('Migration fault was not raised')
        with closing(connect(path)) as db:
            assert image(db)==original;checks.append('migration_rollback_preserves_v1')
            legacy=wire('legacy-pending');dead=wire('legacy-dead')
            db.execute('BEGIN IMMEDIATE')
            for key,raw,state in [('legacy-pending',legacy,'PENDING'),('legacy-dead',dead,'DEAD')]:
                db.execute('INSERT INTO batches VALUES(?,?,?,?,?,?,?,?)',
                    (key,raw,hashlib.sha256(raw).hexdigest(),-1e12,7,1e15,state,'EXPIRED_DATA' if state=='DEAD' else None))
            db.execute('UPDATE meta SET dropped=5,dead_pruned=2');db.execute('COMMIT')
        result=migrate_existing(path,GATEWAY,seeded_backup);assert result['committed']
        c,p=clock('sql-boot-one');s=OutboxSession(clock=lambda:c.ms/1000)
        store=OutboxStore(path,GATEWAY,time_provider=p,session=s)
        assert store.repo.payload('legacy-pending')==legacy and store.repo.payload('legacy-dead')==dead
        assert store.stats()['dropped']==5 and store.stats()['dead_pruned']==2
        assert store.stats()['age_unknown_batches']==2
        assert s.entries['legacy-pending'].row['attempts']==7
        assert s.entries['legacy-dead'].source is None
        checks.append('legacy_bytes_hash_attempts_counters_dead_and_unknown_age_preserved')
        assert store.reserve(time_token=p.token()) is None
        c.advance(60000);assert store.reserve(time_token=p.token())==legacy
        assert s.entries['legacy-pending'].row['attempts']==8
        assert not store.acknowledge(ack('legacy-dead',gateway=GATEWAY))
        p.require_maintenance();assert store.acknowledge(ack('legacy-pending',gateway=GATEWAY))
        checks.append('new_boot_paced_retry_ignores_old_monotonic_deadline_and_ack_while_invalid')
        store.close();store=None
        c,p=clock('sql-boot-two');store=OutboxStore(path,GATEWAY,time_provider=p,session=OutboxSession(clock=lambda:c.ms/1000))
        raw=wire('trusted-sql');assert store.enqueue(raw,time_token=p.token())
        boundary=store.time_boundary();assert boundary['basis']=='site-hub-1' and boundary['utc_lower']>0
        row=store.session.entries['trusted-sql'].row
        assert row['time_trust']=='HUB_TRUSTED' and row['local_order']==3
        # Actual SQLite INSERT happens, then provider invalidation before COMMIT.
        token=p.token()
        # The trace fires at actual INSERT execution. Production commit_guard,
        # rather than an injected exception, must detect invalidation afterward.
        store.c.set_trace_callback(lambda sql:p.require_maintenance() if sql.startswith('INSERT INTO batches') else None)
        try:
            try:store.enqueue(wire('rolled-back'),time_token=token)
            except TimeUnavailable:pass
            else:raise AssertionError('COMMIT guard did not abort')
        finally:store.c.set_trace_callback(None)
        assert store.repo.payload('rolled-back') is None
        assert store.repo.next_order()==4
        checks.append('actual_sql_transaction_rolls_back_payload_order_and_clock_metadata')
        store.close();store=None
        c,p=clock('sql-boot-three');store=OutboxStore(path,GATEWAY,time_provider=p,session=OutboxSession(clock=lambda:c.ms/1000))
        assert store.repo.payload('trusted-sql')==raw and store.time_boundary()==boundary
        assert store.reserve(time_token=p.token())==raw
        c.advance(7*86400*1000+100);sync(p,c);ready(p);store._expire()
        assert store.session.entries['trusted-sql'].source=='AGE_EXPIRED'
        store.close();store=None
        c,p=clock('sql-boot-four');store=OutboxStore(path,GATEWAY,time_provider=p,session=OutboxSession(clock=lambda:c.ms/1000))
        assert store.session.entries['trusted-sql'].source=='AGE_EXPIRED'
        p.require_maintenance();assert store.acknowledge(ack('trusted-sql',gateway=GATEWAY))
        assert store.repo.payload('trusted-sql') is None
        checks.append('durable_time_boundary_age_terminal_and_late_stored_survive_reopen')
    finally:
        if store is not None:store.close()
        if original_backup.is_file():
            with closing(StoreLock(path.with_suffix('.db.lock'))), closing(connect(path)) as target:
                with closing(sqlite3.connect(original_backup.as_uri()+'?mode=ro',uri=True)) as source:
                    assert image(source)==original
                    source.backup(target)
                assert image(target)==original
                target.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            assert digest(agent)==agent_hash
            report.update(restored=True,agentDbUnchanged=True,originalBackupSha256=digest(original_backup))
        (output/'evidence.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--existing-preview-database',action='store_true',required=True)
    args=parser.parse_args()
    result=run(ROOT/'.local/t03-t04-backup'/uuid.uuid4().hex)
    print(json.dumps(result,ensure_ascii=False,indent=2))
