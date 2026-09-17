"""Modbus -> durable outbox -> real MQTT -> deduplicating SQLite receiver."""
import argparse
import asyncio
import json
from pathlib import Path
import socket
import sys
import tempfile
import time
import uuid

from tools import _source_path  # noqa: F401
from tools.mock_device_smoke import fixtures
from tools.mqtt_smoke import docker
from plcnext_iot.core.lifecycle import AgentLifecycle
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.devices.modbus import ModbusRunner
from plcnext_iot.points.samples import SampleBuffer
from plcnext_iot.reporting.service import TelemetryPipeline
from plcnext_iot.messaging.config_service import MqttConfigService
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.messaging.transport import PahoConnection
from plcnext_iot.storage.receiver import ReceiverStore
from plcnext_iot.contracts import validate_message

ROOT=Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0))
        return sock.getsockname()[1]


async def kill_child(child):
    if child.returncode is None:
        if sys.platform=='win32':
            killer=await asyncio.create_subprocess_exec('taskkill','/PID',str(child.pid),'/T','/F',
                stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
            await killer.wait()
        else:
            child.kill()
        await child.wait()


async def smoke(project,python):
    temporary=tempfile.TemporaryDirectory(prefix='iot-telemetry-')
    root=Path(temporary.name)
    broker_name='plcnext-iot-telemetry-'+uuid.uuid4().hex[:10]
    mock_process=None
    observer=None
    sink=None
    stacks=[]
    children=[]
    errors=(root/'mock.log').open('wb')
    try:
        port,modbus_port=free_port(),free_port()
        await docker('run','-d','--rm','--name',broker_name,'-p',f'127.0.0.1:{port}:1883','--mount',
            f'type=bind,source={ROOT / "deploy/mosquitto-test.conf"},target=/mosquitto/config/mosquitto.conf,readonly',
            'eclipse-mosquitto:2.0.22')
        mock,config,values=fixtures(modbus_port)
        config['config']['report']['batchIntervalMs']=500
        for device in config['config']['devices']:
            for point in device['points']:
                point['reportMode']='change'
        mock_process=await asyncio.create_subprocess_exec(str(python),str(ROOT/'tools/mock_device_server.py'),str(project),
            stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,stderr=errors)
        mock_process.stdin.write((json.dumps({'action':'start','config':mock,'values':values})+'\n').encode())
        await mock_process.stdin.drain()
        ready=await asyncio.wait_for(mock_process.stdout.readline(),15)
        assert ready and json.loads(ready)['ok'], 'Mock failed to start'
        settings=MqttSettings('127.0.0.1',port,tls=False)
        prefix='iot/v1/gateway/PLC-01/'
        observer=PahoConnection(settings,'observer-'+uuid.uuid4().hex,
            [prefix+'data',prefix+'config/ack',prefix+'heartbeat',prefix+'device/status'])
        await observer.open()
        sink=ReceiverStore(root/'receiver.db','PLC-01')
        sink.install_config(json.dumps(config).encode())
        async def start():
            samples=SampleBuffer()
            runtime=DeviceRuntime(lambda device:ModbusRunner(device,runtime,samples))
            agent=AgentLifecycle(AgentSettings('PLC-01',root/'agent'),runtime)
            pipeline=TelemetryPipeline(runtime,samples,root/'agent','PLC-01')
            service=MqttConfigService(agent,settings,telemetry=pipeline)
            stacks.append((agent,pipeline,service))
            await agent.start()
            await pipeline.start()
            await service.start()
            return agent,pipeline,service
        async def stop(stack):
            agent,pipeline,service=stack
            await service.close()
            await agent.close()
            await pipeline.close()
        await observer.publish(prefix+'config/set',json.dumps(config).encode(),retain=True)
        agent,pipeline,service=await start()
        original=None
        heartbeat_seen=False
        online_devices=set()
        async with asyncio.timeout(15):
            while original is None or not heartbeat_seen or len(online_devices)<2:
                message=await observer.receive()
                if message.topic==prefix+'heartbeat':
                    assert not validate_message('heartbeat',message.payload)
                    heartbeat=json.loads(message.payload)
                    assert heartbeat['sessionId']==service.session_id
                    assert heartbeat['bootId']==pipeline.engine.boot_id
                    heartbeat_seen=True
                if message.topic==prefix+'device/status':
                    assert not validate_message('deviceStatus',message.payload)
                    device_status=json.loads(message.payload)['device']
                    if device_status['status']=='ONLINE':
                        online_devices.add(device_status['deviceId'])
                if message.topic!=prefix+'data':
                    continue
                assert not validate_message('data',message.payload)
                ack=sink.receive(message.payload,time.time_ns()//1_000_000)
                assert json.loads(ack)['status']=='STORED'
                data=json.loads(message.payload)
                measured={(v['deviceId'],v['pointId']):v['value'] for v in data['values'] if v['quality']=='GOOD'}
                if measured=={('meter-A','pressure'):12.5,('meter-A','voltage'):230,('meter-B','running'):True,('meter-B','alarm'):False}:
                    original=message.payload
                # Intentionally do not send the application ACK.
        assert (await pipeline.stats())['pending']>=1
        print('Real Modbus data stored by receiver; missing ACK left the outbox intact.',flush=True)
        async def mock_command(command):
            mock_process.stdin.write((json.dumps(command)+'\n').encode())
            await mock_process.stdin.drain()
            reply=await asyncio.wait_for(mock_process.stdout.readline(),10)
            assert reply and json.loads(reply)['ok']
        async def wait_device_state(expected):
            seen=set()
            async with asyncio.timeout(15):
                while len(seen)<2:
                    message=await observer.receive()
                    if message.topic==prefix+'device/status':
                        assert not validate_message('deviceStatus',message.payload)
                        device=json.loads(message.payload)['device']
                        if device['status']==expected:
                            seen.add(device['deviceId'])
        await mock_command({'action':'stop'})
        await wait_device_state('OFFLINE')
        await mock_command({'action':'start','config':mock,'values':values})
        await wait_device_state('ONLINE')
        print('Device status changed OFFLINE on disconnect and ONLINE after recovery.',flush=True)
        original_id=json.loads(original)['messageId']
        await stop(stacks[-1])
        disabled=json.loads(json.dumps(config))
        disabled.update(messageId='disabled-11',configVersion=11)
        disabled['config']['enabled']=False
        sink.install_config(json.dumps(disabled).encode())
        before=sink.stats()['stored']
        sink.close()
        sink=ReceiverStore(root/'receiver.db','PLC-01')
        await observer.publish(prefix+'config/set',json.dumps(disabled).encode(),retain=True)
        agent,pipeline,service=await start()
        replayed=False
        async with asyncio.timeout(15):
            while not replayed or agent.active_config_version!=11 or (await pipeline.stats())['pending']:
                try:
                    message=await asyncio.wait_for(observer.receive(),0.2)
                except TimeoutError:
                    continue
                if message.topic!=prefix+'data':
                    continue
                ack=sink.receive(message.payload,time.time_ns()//1_000_000)
                assert json.loads(ack)['status']=='STORED'
                if json.loads(message.payload)['messageId']==original_id:
                    assert message.payload==original
                    assert sink.stats()['stored']==before
                    replayed=True
                await observer.publish(prefix+'data/ack',ack)
        assert agent.state=='DISABLED'
        print('Restart replay kept original bytes/version; STORED removed the pending batch.',flush=True)
        await stop(stacks[-1])
        sink.close()
        sink=None
        # Exercise actual CLI wiring, using the same persisted Agent and receiver.
        config.update(messageId='enabled-12',configVersion=12)
        await observer.publish(prefix+'config/set',json.dumps(config).encode(),retain=True)
        mqtt_file=root/'mqtt.json'
        mqtt_file.write_text(json.dumps({'host':'127.0.0.1','port':port,'tls':False}))
        bootstrap=root/'bootstrap.json'
        bootstrap.write_text(json.dumps({'gatewayId':'PLC-01','dataDirectory':str(root/'agent'),
            'operationTimeoutSeconds':2,'queueCapacity':4}))
        async def spawn(module,*args):
            child=await asyncio.create_subprocess_exec(sys.executable,'-m',module,*args,cwd=ROOT,
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            children.append(child)
            return child
        receiver=await spawn('tools.telemetry_receive','--mqtt',str(mqtt_file),'--database',str(root/'receiver.db'),
            '--gateway','PLC-01','--run-seconds','4')
        line=await asyncio.wait_for(receiver.stdout.readline(),8)
        assert json.loads(line)['event']=='receiver_ready'
        cli=await spawn('tools.agent','--bootstrap',str(bootstrap),'--mqtt',str(mqtt_file),
            '--driver','modbus-tcp','--quiet-samples','--run-seconds','2')
        out,err=await asyncio.wait_for(cli.communicate(),10)
        assert cli.returncode==0,err.decode(errors='replace')
        events=[json.loads(line) for line in out.decode().splitlines()]
        assert not any(event['event']=='sample' for event in events)
        stats=[event['stats'] for event in events if event['event']=='telemetry_stopped'][0]
        assert stats['pending']==0,stats
        receiver_out,receiver_err=await asyncio.wait_for(receiver.communicate(),8)
        assert receiver.returncode==0,receiver_err.decode(errors='replace')
        events=[json.loads(line) for line in receiver_out.decode().splitlines()]
        assert any(e['event']=='data_ack' and e['ack']['status']=='STORED' for e in events),events
        print(json.dumps({'result':'PASS','checks':['real_modbus_values','durable_before_send','missing_ack_retained',
            'restart_replay_identical','historical_version_replay','receiver_dedup','stored_deletes','disabled_backlog',
            'agent_cli_telemetry','receiver_cli','heartbeat_contract','device_online_status',
            'device_offline_recovery'],'hubConnected':False}))
    finally:
        for child in children:
            await kill_child(child)
        for agent,pipeline,service in reversed(stacks):
            await service.close()
            await agent.close()
            await pipeline.close()
        if observer is not None:
            await observer.close()
        if sink is not None:
            sink.close()
        if mock_process is not None and mock_process.returncode is None:
            try:
                mock_process.stdin.write(b'{"action":"exit"}\n')
                await mock_process.stdin.drain()
                await asyncio.wait_for(mock_process.wait(),5)
            except (TimeoutError,ConnectionError):
                await kill_child(mock_process)
        errors.close()
        try:
            await docker('rm','-f',broker_name)
        finally:
            temporary.cleanup()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mock-project',type=Path,required=True)
    parser.add_argument('--mock-python',type=Path,required=True)
    args=parser.parse_args()
    if not args.mock_python.is_file() or not (args.mock_project/'iot_sim/adapters/modbus.py').is_file():
        parser.error('Mock project or interpreter unavailable')
    asyncio.run(smoke(args.mock_project.resolve(),args.mock_python.resolve()))


if __name__=='__main__':
    main()
