"""Bounded real cold-start/reconnect cohort, using the existing preview DBs.

The only controlled disconnect is this test process's own MQTT client. No shared
Hub/broker/mock/PLC is stopped and no fake DATA or STORED is published.
"""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT),str(ROOT/'src')]
from tools.verify_platform_live import DATA,GATEWAY,inspect
from tools.agent import run
from plcnext_iot.core.boot import ProcessBootContext
from plcnext_iot.core.platform_time import PlatformTime
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.messaging.transport import PahoConnection
from plcnext_iot.reporting.audit import AuditRecorder


async def trial(args):
    if args.output.exists():raise FileExistsError(args.output)
    assert args.ready_file.is_file(),'Start passive observer before this Agent'
    context=ProcessBootContext();evidence='DEV_SAME_HOST_HUB_FUNCTIONAL_TEST_NOT_DRIFT_CERTIFICATION'
    clock=context.time_provider=PlatformTime(GATEWAY,context.boot_id,basis='site-hub-1',drift_ppm=1000,
        drift_evidence=evidence,system=lambda:946684800000)
    class Recorder(AuditRecorder):
        def record(self,event):
            if event.get('kind')=='decision':
                s=clock.snapshot();event=dict(event,decisionMonoMs=s.mono_ms,decisionTimeValid=s.time_valid,decisionClockRevision=s.clock_revision)
            return super().record(event)
    recorder=Recorder(args.output.with_suffix('.audit.jsonl'),max_records=50000,max_bytes=32*1024*1024)
    context.accounting.attach_recorder(recorder)
    stop=asyncio.Event();agents=[];connections=[];stages=[];checks=[]
    original_open=PahoConnection.open;original_submit=PahoConnection.submit_publish
    def fact(kind,**fields):context.accounting.record(kind,observedMonoMs=clock.read_mono(),**fields)
    async def opened(connection):
        await original_open(connection);connections.append(connection)
        fact('connection_open',connectionOrdinal=len(connections),sessionId=clock.session)
    def submitted(connection,topic,payload,**options):
        before=clock.diagnostics();packet=json.loads(payload)
        info=original_submit(connection,topic,payload,**options)
        fact('sdk_submitted',topicSuffix=topic.removeprefix(f'iot/v1/gateway/{GATEWAY}/'),
            messageId=packet.get('messageId',packet.get('requestId','')),payloadHash=hashlib.sha256(payload).hexdigest(),
            sessionId=before.get('session_id',clock.session),connectionRevision=before['connection_revision'],
            clockRevision=before['clock_revision'],timeValid=before['time_valid'],controlReady=before['control_ready'])
        return info
    def observed(agent):
        if agent is not None:agents.append(agent)
    async def wait_for(predicate,seconds,label):
        until=asyncio.get_running_loop().time()+seconds
        while asyncio.get_running_loop().time()<until:
            if predicate():return
            await asyncio.sleep(.1)
        raise RuntimeError('Timed out: '+label)
    async def coordinate():
        await wait_for(lambda:agents and getattr(agents[0],'telemetry',None) is not None and
            agents[0].active_config_version==1 and clock.valid(None,'SEND'),35,'real config1 and current controls')
        agent=agents[0];stages.append(dict(stage='ready',monoMs=clock.read_mono(),version=agent.active_config_version))
        config=asdict(agent.runtime.snapshot)
        args.output.with_suffix('.config.json').write_text(json.dumps(config,ensure_ascii=False,indent=2),encoding='utf-8')
        await asyncio.sleep(12)
        if args.scenario=='reconnect':
            first=len(connections);before=context._sample_sequence
            # An ordinary owned-client close, not a service or broker failure.
            fact('reconnect_requested',capturePrefix=before)
            await connections[-1].close()
            await wait_for(lambda:len(connections)>first and clock.valid(None,'SEND'),35,'new session synced and controls submitted')
            checks.append(dict(reconnected=True,sameBoot=context.boot_id,sequenceBefore=before,sequenceAfter=context._sample_sequence))
            stages.append(dict(stage='reconnected',monoMs=clock.read_mono(),connectionRevision=clock.connection_revision))
        await asyncio.sleep(22)
        # Stop only this test's real collectors; leave normal transport/storage
        # alive for tail proof, without synthetic config, values or ACKs.
        await asyncio.gather(*(r.stop() for r in agent.runtime.runners.values()))
        upper=context._sample_sequence;fact('capture_stopped',capturePrefix=upper)
        stages.append(dict(stage='capture_stopped',monoMs=clock.read_mono(),upper=upper))
        # A real application ACK may be lost. Keep the production 30/60-second
        # retry deadlines and allow their bounded tail, never shorten them.
        until=asyncio.get_running_loop().time()+80
        while asyncio.get_running_loop().time()<until:
            stats=await agent.telemetry.stats();s=context.accounting.snapshot()
            if (s['buffer']['pendingEvents']==0 and s['decisionPending']==0 and
                    not stats['pending'] and not stats['dead'] and not agent.telemetry.engine._owned):
                await asyncio.sleep(.3)
                assert context._sample_sequence==upper,'Capture continued after collector stop'
                checks.append(dict(capturePrefixComplete=upper,tailDrained=True))
                fact('cohort_end',capturePrefix=upper)
                stop.set();return
            await asyncio.sleep(.2)
        raise RuntimeError('Tail not drained; preserve existing DB and evidence')
    coordinator=asyncio.create_task(coordinate());run_task=None;failure=None;code=1
    settings=AgentSettings(GATEWAY,DATA)
    mqtt=MqttSettings('192.168.0.5',1883,False,time_mode='HUB_MQTT',time_basis_ref='site-hub-1',drift_ppm=1000,drift_evidence=evidence)
    try:
        with patch.object(PahoConnection,'open',opened),patch.object(PahoConnection,'submit_publish',submitted):
            run_task=asyncio.create_task(run(settings,[],170,driver='modbus-tcp',mqtt_settings=mqtt,quiet_samples=True,
                stop_event=stop,boot_context=context,runtime_observer=observed))
            finished,_=await asyncio.wait((run_task,coordinator),return_when=asyncio.FIRST_COMPLETED)
            if coordinator in finished:
                try:await coordinator
                except Exception as exc:failure=str(exc);stop.set()
            else:
                if not coordinator.done():coordinator.cancel()
                failure='Agent ended before cohort coordinator'
            code=await run_task
    finally:
        coordinator.cancel();await asyncio.gather(coordinator,return_exceptions=True)
        fact('run_end',capturePrefix=context._sample_sequence)
        audit=recorder.close()
        result=dict(scenario=args.scenario,bootId=context.boot_id,exitCode=code,failure=failure,
            stages=stages,checks=checks,audit=audit,connections=len(connections),
            diagnostics=context.accounting.snapshot(),finalTime=clock.diagnostics(),after=inspect(),
            finalStats=agents[0].telemetry.final_stats if agents else None,
            temporaryDatabaseCreated=False,sharedServicesStopped=False,operatingSystemClockChanged=False)
        args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(dict(output=str(args.output),scenario=args.scenario,exitCode=code,failure=failure,audit=audit,connections=len(connections))),flush=True)
    return 0 if code==0 and failure is None and audit['accountingComplete'] else 1


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--scenario',choices=('cold','reconnect'),required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--ready-file',type=Path,required=True)
    raise SystemExit(asyncio.run(trial(p.parse_args())))
