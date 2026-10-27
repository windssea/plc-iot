"""Explicit real-Hub lifecycle test using the existing idle preview databases.

No new database, OS clock write, PLC connection, or synthetic DATA/ACK producer.
Workers run the production Agent/collector/clock/Outbox in separate processes.
"""
import argparse
import asyncio
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'src')]
from plcnext_iot.config.locking import StoreLock
from plcnext_iot.core.boot import ProcessBootContext
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.core.platform_time import PlatformTime
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.storage.outbox_migration import migrate_existing

DATA=ROOT/'.local/setup-preview';GATEWAY='PLC-UI-DEMO'


def db(path,mode='rw'):
    return sqlite3.connect(path.resolve().as_uri()+'?mode='+mode,uri=True,isolation_level=None)


def inspect():
    result={}
    for name in ('agent','telemetry'):
        with closing(db(DATA/(name+'.db'),'ro')) as c:
            result[name]=dict(version=c.execute('PRAGMA user_version').fetchone()[0],integrity=c.execute('PRAGMA quick_check').fetchone()[0])
            if name=='agent':result[name].update(metadata=c.execute('SELECT gateway_id,active_version,previous_version FROM metadata WHERE id=1').fetchone())
            else:
                result[name].update(batches=c.execute('SELECT message_id,hash,attempts,state FROM batches ORDER BY rowid').fetchall())
                if result[name]['version']==2:
                    result[name].update(boundary=c.execute('SELECT trusted_basis_ref,trusted_utc_high_water_ms FROM meta WHERE id=1').fetchone())
    return result


def prepare(output):
    state=inspect()
    assert state['agent']['metadata']==(GATEWAY,None,None) and state['telemetry']['version']==1 and not state['telemetry']['batches']
    output.mkdir(parents=True,exist_ok=False)
    before={}
    for name in ('agent','telemetry'):
        path=DATA/(name+'.db')
        with closing(StoreLock(path.with_suffix('.db.lock'))),closing(db(path,'ro')) as source:
            before[name]='\n'.join(source.iterdump())
            if name=='agent':
                with closing(sqlite3.connect(output/'agent-original.backup')) as target:source.backup(target)
    migration=migrate_existing(DATA/'telemetry.db',GATEWAY,output/'telemetry-original.backup')
    manifest=dict(gatewayId=GATEWAY,directory=str(DATA),migration=migration,originalDumps=before,
        commissioningSha256=hashlib.sha256((DATA/'commissioning.json').read_bytes()).hexdigest(),temporaryDatabaseCreated=False)
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print(json.dumps(dict(prepared=True,manifest=str(output/'manifest.json'),existingDatabasesOnly=True)))


async def worker(args):
    from tools.agent import run
    context=ProcessBootContext()
    # This is an injected diagnostic system date, never an OS clock change.
    evidence='DEV_SAME_HOST_HUB_FUNCTIONAL_TEST_NOT_DRIFT_CERTIFICATION'
    context.time_provider=PlatformTime(GATEWAY,context.boot_id,basis='site-hub-1',drift_ppm=1000,
        drift_evidence=evidence,system=lambda:946684800000)
    settings=AgentSettings(GATEWAY,DATA)
    mqtt=MqttSettings('192.168.0.5',1883,False,time_mode='HUB_MQTT',time_basis_ref='site-hub-1',
        drift_ppm=1000,drift_evidence=evidence)
    holder=[];samples=[]
    def observed(agent):holder.append(agent)
    async def monitor():
        while True:
            row=dict(atMonoMs=context.time_provider.read_mono(),time=context.time_provider.diagnostics())
            if holder and getattr(holder[0],'telemetry',None) is not None:
                row['state']=holder[0].state;row['version']=holder[0].active_config_version
                stats=await holder[0].telemetry.stats()
                row['capture']=dict(captured=stats['capture_events'],eligible=stats['eligible_events'],
                    pending=stats['pending'],dead=stats['dead'],stored=stats['diagnostics'].get('storedConfirmedEventsTotal'))
            samples.append(row)
            print(json.dumps(dict(event='time_observation',bootId=context.boot_id,**row)),flush=True)
            await asyncio.sleep(5)
    task=asyncio.create_task(monitor())
    patcher=None;collector_patchers=[]
    if args.hold_collectors:
        from unittest.mock import patch
        from plcnext_iot.devices.modbus import ModbusRunner
        async def held(_runner):pass
        for method in ('start','stop'):
            item=patch.object(ModbusRunner,method,held);item.start();collector_patchers.append(item)
    if args.crash_after_sdk:
        import os
        from unittest.mock import patch
        from plcnext_iot.messaging.transport import PahoConnection
        original=PahoConnection.submit_publish
        def submitted(connection,topic,payload,**options):
            info=original(connection,topic,payload,**options)
            if topic==f'iot/v1/gateway/{GATEWAY}/data':
                packet=json.loads(payload)
                with closing(db(DATA/'telemetry.db','ro')) as c:
                    row=c.execute('SELECT message_id,hash,attempts,time_trust,time_basis_ref FROM batches WHERE message_id=?',(packet['messageId'],)).fetchone()
                assert row is not None and row[1]==hashlib.sha256(payload).hexdigest() and row[2]>0
                marker=dict(scope='INTENTIONAL_PYTHON_PROCESS_EXIT_AFTER_REAL_SDK_SUBMIT_BEFORE_ACK_APPLY',
                    pid=os.getpid(),bootId=context.boot_id,messageId=packet['messageId'],raw=payload.decode(),sha256=row[1],
                    attempts=row[2],timeTrust=row[3],timeBasisRef=row[4],fakeAckPublished=False,sharedServicesStopped=False)
                with args.crash_marker.open('x',encoding='utf-8') as stream:
                    json.dump(marker,stream,ensure_ascii=False,indent=2);stream.flush();os.fsync(stream.fileno())
                # The real network thread may deliver and receive STORED; the
                # owner event loop is held here, so ACK_APPLY cannot delete it.
                time.sleep(.3)
                os._exit(86)
            return info
        patcher=patch.object(PahoConnection,'submit_publish',submitted);patcher.start()
    code=1
    try:code=await run(settings,[],args.seconds,driver=args.driver,mqtt_settings=mqtt,quiet_samples=True,boot_context=context,runtime_observer=observed)
    finally:
        if patcher is not None:patcher.stop()
        for item in collector_patchers:item.stop()
        task.cancel();await asyncio.gather(task,return_exceptions=True)
        result=dict(pid=__import__('os').getpid(),bootId=context.boot_id,driver=args.driver,seconds=args.seconds,exitCode=code,
            wrongSystemDateInjected=True,operatingSystemClockChanged=False,collectorsHeld=args.hold_collectors,observations=samples,after=inspect())
        args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    return code


def restore(output):
    manifest=json.loads((output/'manifest.json').read_text())
    now=inspect();assert not now['telemetry']['batches'],'Undelivered batches retained: do not restore old backup'
    commissioning=hashlib.sha256((DATA/'commissioning.json').read_bytes()).hexdigest()
    assert commissioning==manifest['commissioningSha256']
    result={}
    for name in ('agent','telemetry'):
        path=DATA/(name+'.db');backup=output/(name+'-original.backup')
        with closing(StoreLock(path.with_suffix('.db.lock'))),closing(db(path)) as target:
            with closing(db(backup,'ro')) as source:
                assert '\n'.join(source.iterdump())==manifest['originalDumps'][name]
                source.backup(target)
            assert '\n'.join(target.iterdump())==manifest['originalDumps'][name]
            target.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            result[name]=dict(restored=True,integrity=target.execute('PRAGMA quick_check').fetchone()[0])
    (output/'restore.json').write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('command',choices=('prepare','worker','inspect','restore'))
    parser.add_argument('--output',type=Path);parser.add_argument('--seconds',type=int,choices=range(1,181),default=75)
    parser.add_argument('--driver',choices=('modbus-tcp','none'),default='modbus-tcp')
    parser.add_argument('--crash-after-sdk',action='store_true');parser.add_argument('--crash-marker',type=Path)
    parser.add_argument('--hold-collectors',action='store_true',help='Install normal runners but hold start/stop in this test process; no new captures')
    args=parser.parse_args()
    if args.crash_after_sdk and args.crash_marker is None:parser.error('--crash-marker required for intentional process exit')
    if args.command=='prepare':prepare(args.output.resolve())
    elif args.command=='worker':raise SystemExit(asyncio.run(worker(args)))
    elif args.command=='restore':restore(args.output.resolve())
    else:print(json.dumps(inspect(),default=str))
