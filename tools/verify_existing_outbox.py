"""I06: existing EMPTY development SQLite + existing Hub/PostgreSQL, no new DB.

No Agent or driver starts. SQLite URI mode=rw is mandatory, including children.
Only uniquely named test batches/assets are changed, with exact-ID cleanup.
Real STORED comes exclusively from the existing Hub over MQTT.
"""
import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from unittest.mock import patch

from tools import _source_path  # noqa
from plcnext_iot.config.locking import StoreLock
from plcnext_iot.config.store import StoreError
from plcnext_iot.reporting.accounting import Accounting
from plcnext_iot.storage.outbox import OutboxStore
from plcnext_iot.storage.outbox_session import OutboxSession

ROOT=Path(__file__).resolve().parents[1]
HUB=ROOT.parent/'PLCnext-Hub-Backend'


@contextmanager
def existing_only(path):
    approved=path.resolve()
    if not approved.is_file():raise RuntimeError('Existing SQLite file required; creation is forbidden')
    connect=sqlite3.connect
    def guarded(target,*args,**kwargs):
        # Production Database passes a filesystem Path. No :memory:, alternate
        # filename, attach or implicit CREATE path is permitted by this runner.
        if Path(target).resolve()!=approved:raise RuntimeError('Unapproved database path')
        kwargs['uri']=True
        return connect(approved.as_uri()+'?mode=rw',*args,**kwargs)
    with patch.object(sqlite3,'connect',guarded),patch.object(sqlite3.dbapi2,'connect',guarded):
        yield


def fingerprint(path):
    c=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)
    try:
        return dict(applicationId=c.execute('PRAGMA application_id').fetchone()[0],
            userVersion=c.execute('PRAGMA user_version').fetchone()[0],
            schema=c.execute("SELECT type,name,sql FROM sqlite_master ORDER BY type,name").fetchall(),
            meta=c.execute('SELECT gateway,dropped,dead_pruned FROM meta').fetchall(),
            batches=c.execute('SELECT message_id,hash,length(payload),attempts,state,reason FROM batches ORDER BY message_id').fetchall(),
            quickCheck=c.execute('PRAGMA quick_check').fetchone()[0])
    finally:c.close()


def load_fixture(path):return json.loads(path.read_text(encoding='utf-8'))


def child(operation,database,fixture):
    s=load_fixture(fixture);raw=(fixture.parent/'original-wire.json').read_bytes()
    clock=type('Clock',(),{'now':0,'__call__':lambda self:self.now})()
    accounting=Accounting('recovery-'+uuid.uuid4().hex)
    with existing_only(database):
        store=OutboxStore(database,s['gatewayId'],session=OutboxSession(clock=clock,accounting=accounting))
        if operation=='enqueue':assert store.enqueue(raw,time.time())
        elif operation=='reserve':
            clock.now=60;assert store.reserve(None)==raw
        elif operation=='delete':
            ack=(fixture.parent/'real-stored-ack.json').read_bytes()
            assert json.loads(ack)['status']=='STORED'
            assert store.acknowledge(ack)
        elif operation=='publish':
            clock.now=60;assert store.reserve(None)==raw
            hub=RealHub(s)
            received=hub.stored(raw)
            (fixture.parent/'real-stored-ack.json').write_bytes(received)
            (fixture.parent/'first-real-ack-envelope.json').write_text(json.dumps(hub.received,indent=2)+'\n')
        elif operation=='inspect':
            assert store.repo.payload(s['messageId'])==raw
        result=dict(operation=operation,pid=os.getpid(),stats=store.stats(),diagnostics=accounting.snapshot(),
            payloadHash=hashlib.sha256(raw).hexdigest(),database=str(database),newDatabase=False)
        print(json.dumps(result),flush=True)
        if operation in ('enqueue','reserve','publish','delete'):
            os._exit(23) # Commit has returned; deliberately bypass normal SQLite close.
        store.close()


def hub_library():
    sys.path.append(str(HUB/'logs/phase1/.venv/Lib/site-packages'))
    spec=importlib.util.spec_from_file_location('existing_hub_fixture_access',HUB/'tools/phase1/live_hub_check.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def seed(s,state,connection):
    from psycopg.types.json import Jsonb
    base=820000000000000000+uuid.uuid4().int%100000000000000
    s.update(dict(zip(('parent','broker','gateway','release','device','variable'),range(base,base+6))))
    conn=dict(host='127.0.0.1',port=1,unitId=1,connectTimeoutMs=1000,requestTimeoutMs=1000,retryCount=0)
    address=dict(area='holding_register',address=0,byteOrder='ABCDEFGH')
    point=dict(pointId='p0',name='p0',enabled=True,dataType='float64',pollIntervalMs=5000,
        scale=1,offset=0,unit='V',reportMode='cyclic',reportIntervalMs=5000,deadband=0,staleAfterMs=15000,modbus=address)
    config=dict(enabled=False,report=dict(batchIntervalMs=1000,maxBatchPoints=64),devices=[dict(deviceId='i06-meter',
        name='i06-meter',enabled=True,protocol='modbus_tcp',connection=conn,points=[point])])
    cfg=dict(schemaVersion=1,gatewayId=s['gatewayId'],timestamp=time.time_ns()//1000000,messageId=s['prefix']+'-cfg',configVersion=1,config=config)
    raw=json.dumps(cfg,separators=(',',':')).encode()
    with connection() as c:
        assert c.execute('SELECT count(*) FROM biz_iot_gateway WHERE gateway_id=%s',(s['gatewayId'],)).fetchone()[0]==0,'Existing Hub gateway may not be borrowed'
        s['assetsBefore']=c.execute('SELECT count(*) FROM biz_device WHERE deleted=0').fetchone()[0]
        s['tasksBefore']=c.execute('SELECT count(*) FROM biz_collect_task WHERE deleted=0').fetchone()[0]
        c.execute("INSERT INTO mqtt_broker(id,name,host,port,status,deleted) VALUES(%s,%s,'127.0.0.1',1883,0,0)",(s['broker'],s['prefix']))
        c.execute("INSERT INTO biz_device(id,device_name,device_code,type_code,parent_id,ancestors,level,status,collect_enabled) VALUES(%s,%s,%s,'IOT_GATEWAY',0,',0,',1,1,false)",(s['parent'],s['prefix'],s['prefix']))
        c.execute("INSERT INTO biz_iot_gateway(id,parent_device_id,gateway_id,broker_id,binding_state,draft_revision,desired_version,enabled,report) VALUES(%s,%s,%s,%s,'BOUND',1,1,false,%s)",(s['gateway'],s['parent'],s['gatewayId'],s['broker'],Jsonb(config['report'])))
        c.execute("INSERT INTO biz_device(id,device_name,device_code,type_code,parent_id,ancestors,level,status,collect_enabled) VALUES(%s,%s,%s,'METER',%s,%s,2,1,false)",(s['device'],s['prefix']+'-meter',s['prefix']+'-meter',s['parent'],f",0,{s['parent']},"))
        c.execute("INSERT INTO biz_iot_device_config(device_id,gateway_ref,wire_device_id,protocol,connection,revision) VALUES(%s,%s,'i06-meter','modbus_tcp',%s,1)",(s['device'],s['gateway'],Jsonb(conn)))
        c.execute("INSERT INTO biz_device_variable(id,device_id,var_name,var_address,data_type,is_collected,scale_factor,offset_val,unit) VALUES(%s,%s,'p0','fixture','float64',true,1,0,'V')",(s['variable'],s['device']))
        c.execute("INSERT INTO biz_iot_point_config(variable_id,device_id,point_id,wire_data_type,poll_interval_ms,report_interval_ms,stale_after_ms,deadband,address) VALUES(%s,%s,'p0','float64',5000,5000,15000,0,%s)",(s['variable'],s['device'],Jsonb(address)))
        c.execute("INSERT INTO biz_iot_config_release(id,gateway_ref,gateway_id,config_version,message_id,request_id,request_hash,source_revision,origin,payload,payload_hash,status) VALUES(%s,%s,%s,1,%s,%s,%s,1,'USER',%s,%s,'SUPERSEDED')",(s['release'],s['gateway'],s['gatewayId'],cfg['messageId'],s['prefix']+'-req','c'*64,raw,hashlib.sha256(raw).hexdigest()))
        c.execute("INSERT INTO biz_iot_release_point(release_id,wire_device_id,point_id,hub_device_id,variable_id,point_name,wire_data_type,scale,offset_value,enabled) VALUES(%s,'i06-meter','p0',%s,%s,'p0','float64',1,0,true)",(s['release'],s['device'],s['variable']))
        state.write_text(json.dumps(s,indent=2)+'\n',encoding='utf-8') # Before COMMIT; cleanup identities survive interruption.


def cleanup(s,connection):
    with connection() as c:
        assert c.execute('SELECT gateway_id FROM biz_iot_gateway WHERE id=%s AND parent_device_id=%s',(s['gateway'],s['parent'])).fetchone()==(s['gatewayId'],)
        assert c.execute('SELECT device_code FROM biz_device WHERE id=%s',(s['parent'],)).fetchone()==(s['prefix'],)
        c.execute('DELETE FROM biz_device_data WHERE source_gateway_id=%s AND device_id=%s',(s['gatewayId'],s['device']))
        c.execute('DELETE FROM biz_iot_ingest_receipt WHERE gateway_id=%s AND message_id=%s',(s['gatewayId'],s['messageId']))
        c.execute('DELETE FROM biz_iot_release_point WHERE release_id=%s',(s['release'],))
        c.execute('DELETE FROM biz_iot_config_release WHERE id=%s AND gateway_id=%s',(s['release'],s['gatewayId']))
        c.execute('DELETE FROM biz_iot_point_config WHERE variable_id=%s AND device_id=%s',(s['variable'],s['device']))
        c.execute('DELETE FROM biz_iot_device_config WHERE device_id=%s AND gateway_ref=%s',(s['device'],s['gateway']))
        c.execute('DELETE FROM biz_device_variable WHERE id=%s AND device_id=%s',(s['variable'],s['device']))
        c.execute('DELETE FROM biz_iot_session WHERE gateway_ref=%s',(s['gateway'],))
        c.execute('DELETE FROM biz_iot_gateway WHERE id=%s AND gateway_id=%s',(s['gateway'],s['gatewayId']))
        c.execute('DELETE FROM biz_device WHERE id=ANY(%s)',([s['parent'],s['device']],))
        c.execute('DELETE FROM mqtt_broker WHERE id=%s AND name=%s',(s['broker'],s['prefix']))
        assets=c.execute('SELECT count(*) FROM biz_device WHERE deleted=0').fetchone()[0]
        tasks=c.execute('SELECT count(*) FROM biz_collect_task WHERE deleted=0').fetchone()[0]
        assert (assets,tasks)==(s['assetsBefore'],s['tasksBefore'])
        assert c.execute('SELECT count(*) FROM biz_iot_ingest_receipt WHERE gateway_id=%s',(s['gatewayId'],)).fetchone()[0]==0
        return dict(assets=assets,collectTasks=tasks,remainingOwnedRows=0)


class RealHub:
    def __init__(self,s):
        import paho.mqtt.client as mqtt
        self.prefix=f"iot/v1/gateway/{s['gatewayId']}/";self.received=[]
        self.queue=queue.Queue(maxsize=16);self.ready=threading.Event()
        self.client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=s['prefix'])
        self.client.on_connect=lambda c,*_:c.subscribe(self.prefix+'data/ack',qos=1)
        self.client.on_subscribe=lambda *_:self.ready.set()
        self.client.on_message=lambda _c,_u,m:self.queue.put_nowait((bytes(m.payload),m.retain))
        self.client.connect('127.0.0.1',1883);self.client.loop_start()
        assert self.ready.wait(10)

    def stored(self,raw):
        key=json.loads(raw)['messageId'];deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            self.client.publish(self.prefix+'data',raw,qos=1).wait_for_publish(timeout=3)
            try:
                received,retained=self.queue.get(timeout=3)
                value=json.loads(received)
                if value['messageId']==key:
                    assert value['status']=='STORED' and not retained,value
                    self.received.append(dict(raw=received.decode(),retained=retained,receivedMono=time.monotonic()))
                    return received
            except queue.Empty:pass
        raise TimeoutError('Missing real Hub STORED')

    def close(self):self.client.disconnect();self.client.loop_stop()


def sql_faults(database,s,raw):
    results={}
    with existing_only(database):
        store=OutboxStore(database,s['gatewayId'],max_dead=1)
        try:
            # Exact old payload remains; a denied INSERT rolls back an actual
            # production transaction without creating triggers/tables/databases.
            extra=dict(json.loads(raw),messageId=s['prefix']+'-fault')
            store.c.set_authorizer(lambda action,a,b,*_:sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_INSERT and a=='batches' else sqlite3.SQLITE_OK)
            try:
                try:store.enqueue(json.dumps(extra).encode(),time.time())
                except StoreError:results['insertRollback']=True
            finally:store.c.set_authorizer(None)
            assert store.repo.payload(extra['messageId']) is None
            # Corrupt only this connection's uncommitted view, then roll back;
            # never persist damaged bytes to the shared development Outbox.
            entry=store.session.entries[s['messageId']];due=entry.due;entry.due=0
            store.c.execute('BEGIN IMMEDIATE')
            try:
                store.c.execute('UPDATE batches SET payload=? WHERE message_id=?',(raw+b' ',s['messageId']))
                try:store.reserve(None)
                except StoreError:results['corruptPayloadBlocked']=True
            finally:store.c.rollback();entry.due=due
            assert store.repo.payload(s['messageId'])==raw
            # Exercise a real SQLite SQLITE_FULL on the existing file with a
            # temporarily lowered page cap; do not fill the development disk.
            pages=store.c.execute('PRAGMA page_count').fetchone()[0]
            maximum=store.c.execute('PRAGMA max_page_count').fetchone()[0]
            full=dict(json.loads(raw),messageId=s['prefix']+'-full')
            full['values']=[dict(full['values'][0],pointId='f'+str(i)) for i in range(500)]
            store.c.execute(f'PRAGMA max_page_count={pages}')
            try:
                try:store.enqueue(json.dumps(full).encode(),time.time())
                except StoreError:results['sqliteFullRollback']=True
            finally:store.c.execute(f'PRAGMA max_page_count={maximum}')
            assert store.repo.payload(full['messageId']) is None
            assert store.c.execute('PRAGMA page_count').fetchone()[0]==pages
            for suffix in ('dead-a','dead-b'):
                value=dict(json.loads(raw),messageId=s['prefix']+'-'+suffix)
                assert store.enqueue(json.dumps(value).encode(),time.time())
            a=s['prefix']+'-dead-a';b=s['prefix']+'-dead-b'
            store._terminal(a,'EXPIRED_DATA','AGE_EXPIRED')
            store.c.set_authorizer(lambda action,table,*_:sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_DELETE and table=='batches' else sqlite3.SQLITE_OK)
            try:
                try:store._terminal(b,'EXPIRED_DATA','AGE_EXPIRED')
                except StoreError:results['terminalPruneRollback']=True
            finally:store.c.set_authorizer(None)
            assert store.c.execute('SELECT state FROM batches WHERE message_id=?',(b,)).fetchone()[0]=='PENDING'
            assert store.c.execute("SELECT count(*) FROM batches WHERE state='DEAD'").fetchone()[0]==1
            store._terminal(b,'EXPIRED_DATA','AGE_EXPIRED')
            assert store.repo.payload(a) is None
            results['terminalPruneCommit']=True
            # Reader must hold a WAL snapshot containing a live frame.
            reader=sqlite3.connect(database)
            reader.execute('BEGIN');reader.execute('SELECT count(*) FROM batches').fetchone()
            store.c.execute('PRAGMA busy_timeout=20')
            try:
                try:store.repo.guard()
                except StoreError:results['busyWalBlocksWrite']=True
            finally:reader.rollback();reader.close()
            maximum=store.repo.max_physical_bytes;store.repo.max_physical_bytes=1
            try:
                try:store.repo.guard()
                except StoreError:results['physicalPoolRejects']=True
            finally:store.repo.max_physical_bytes=maximum
            with patch('plcnext_iot.storage.batch_repository.shutil.disk_usage',return_value=type('Free',(),{'free':1})()):
                try:store.repo.guard()
                except StoreError:results['freeReserveRejects']=True
            assert store.repo.payload(s['messageId'])==raw
        finally:store.close()
    assert all(results.get(k) for k in ('insertRollback','corruptPayloadBlocked','sqliteFullRollback','terminalPruneRollback','terminalPruneCommit','busyWalBlocksWrite','physicalPoolRejects','freeReserveRejects')),results
    return results


def main(database,output):
    database=database.resolve();output=output.resolve()
    before=fingerprint(database)
    assert before['quickCheck']=='ok' and not before['batches'],'Existing EMPTY development Outbox required'
    assert before['applicationId']==0x504F5554 and before['userVersion']==1
    output.mkdir(parents=True,exist_ok=False)
    state=output/'fixture.json'
    s=dict(gatewayId=before['meta'][0][0],prefix='i06-'+uuid.uuid4().hex,source='SYNTHETIC_INPUT_NO_DEVICE')
    s['messageId']=s['prefix']+'-persistent'
    raw=json.dumps(dict(schemaVersion=1,gatewayId=s['gatewayId'],timestamp=time.time_ns()//1000000,
        messageId=s['messageId'],bootId=s['prefix']+'-old',sequence=1,configVersion=1,
        values=[dict(deviceId='i06-meter',pointId='p0',timestamp=time.time_ns()//1000000,value=42.5,quality='GOOD')]),separators=(',',':')).encode()
    (output/'original-wire.json').write_bytes(raw)
    (output/'before.json').write_text(json.dumps(before,indent=2)+'\n')
    locks=[StoreLock(database.parent/'commissioning.lock'),StoreLock(database.parent/'agent.db.lock')]
    fixture_access=hub_library();hub=None;seeded=False
    evidence=dict(database=str(database),databaseCreated=False,devicesOperated=0,childProcesses=[],timeMode='SYSTEM_RTC_COMPAT_UNVERIFIED')
    evidence['sourceSha256']={str(p.relative_to(ROOT)).replace('\\','/'):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(list((ROOT/'src').rglob('*.py'))+[Path(__file__)])}
    try:
        seed(s,state,fixture_access.connection);seeded=True
        def run_child(operation):
            result=subprocess.run([sys.executable,'-m','tools.verify_existing_outbox','--child',operation,
                '--database',str(database),'--fixture',str(state)],cwd=ROOT,capture_output=True,text=True,timeout=30)
            expected=0 if operation=='inspect' else 23
            if result.returncode!=expected:raise RuntimeError('Recovery child failed: '+operation+' '+result.stderr[-2000:])
            record=json.loads(result.stdout.strip().splitlines()[-1]);evidence['childProcesses'].append(record)
            return record
        run_child('enqueue');run_child('inspect');run_child('reserve');run_child('inspect')
        evidence['sqlFaults']=sql_faults(database,s,raw)
        time.sleep(6) # Existing Hub's ordinary broker scan, no backend restart.
        run_child('publish') # Real ACK received, deliberately exit before delete.
        restored=run_child('inspect')
        assert restored['stats']['pending']>=1 and not restored['diagnostics']['accountingComplete']
        hub=RealHub(s);hub.stored(raw)
        received=load_fixture(output/'first-real-ack-envelope.json')+hub.received
        with fixture_access.connection() as c:
            receipts=c.execute('SELECT payload_hash FROM biz_iot_ingest_receipt WHERE gateway_id=%s AND message_id=%s',(s['gatewayId'],s['messageId'])).fetchall()
            rows=c.execute('SELECT count(*) FROM biz_device_data WHERE source_gateway_id=%s AND source_message_id=%s',(s['gatewayId'],s['messageId'])).fetchone()[0]
        assert receipts==[(hashlib.sha256(raw).hexdigest(),)] and rows==1
        evidence.update(realStoredAcks=len(received),hubReceipts=len(receipts),hubRows=rows,originalPayloadSha256=hashlib.sha256(raw).hexdigest(),deduplication=True)
        (output/'real-acks.json').write_text(json.dumps(received,indent=2)+'\n')
        run_child('delete')
        with existing_only(database):
            store=OutboxStore(database,s['gatewayId'])
            try:assert store.repo.payload(s['messageId']) is None
            finally:store.close()
        evidence['deleteCommitDoesNotResurrect']=True
    finally:
        if hub is not None:hub.close()
        try:
            if seeded:evidence['hubCleanup']=cleanup(s,fixture_access.connection)
            # Only this run's exact identities. Retain evidence; restore original
            # logical data/counters, never copy/recreate/replace the database.
            with existing_only(database):
                c=sqlite3.connect(database)
                try:
                    owned=[s['messageId'],s['prefix']+'-fault',s['prefix']+'-full',s['prefix']+'-dead-a',s['prefix']+'-dead-b']
                    assert all(row[0] in owned for row in c.execute('SELECT message_id FROM batches'))
                    c.executemany('DELETE FROM batches WHERE message_id=?',[(key,) for key in owned])
                    c.execute('UPDATE meta SET dropped=?,dead_pruned=? WHERE gateway=?',(before['meta'][0][1],before['meta'][0][2],s['gatewayId']))
                    c.commit()
                finally:c.close()
            after=fingerprint(database)
            evidence['logicalDatabaseRestored']=after==before
            assert after==before
            (output/'after.json').write_text(json.dumps(after,indent=2)+'\n')
        finally:
            for lock in reversed(locks):lock.close()
            (output/'recovery-evidence.json').write_text(json.dumps(evidence,indent=2)+'\n')
    print(json.dumps(evidence,indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',type=Path,required=True)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--child',choices=('enqueue','reserve','publish','inspect','delete'))
    parser.add_argument('--fixture',type=Path)
    args=parser.parse_args()
    if args.child:child(args.child,args.database.resolve(),args.fixture)
    else:
        if args.output is None:parser.error('--output is required')
        main(args.database,args.output)
