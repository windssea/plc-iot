"""Real Mosquitto acceptance in a temporary, loopback-only Docker container."""
import asyncio
import json
from pathlib import Path
import tempfile
import uuid
import socket
import sys

from tools import _source_path  # noqa: F401
from plcnext_iot.core.lifecycle import AgentLifecycle
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.contracts import validate_message

ROOT = Path(__file__).resolve().parents[1]


async def docker(*args):
    process = await asyncio.create_subprocess_exec('docker', *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await asyncio.wait_for(process.communicate(), 60)
    if process.returncode:
        raise RuntimeError(err.decode(errors='replace')[-2000:])
    return out.decode().strip()


async def smoke():
    assert (ROOT / 'src/plcnext_iot/messaging/transport.py').exists(), 'MQTT transport not implemented'
    from plcnext_iot.messaging.transport import PahoConnection
    from plcnext_iot.messaging.settings import MqttSettings
    from plcnext_iot.messaging.config_service import MqttConfigService
    name = 'plcnext-iot-test-' + uuid.uuid4().hex[:12]
    agents, services, peers = [], [], []
    temporary = tempfile.TemporaryDirectory(prefix='iot-mqtt-')
    prefix = 'iot/v1/gateway/PLC-01/'
    try:
        # Keep the selected port stable across docker restart (an empty HostPort
        # can be reassigned by Docker on restart).
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1',0))
            port = reservation.getsockname()[1]
        await docker('run','-d','--rm','--name',name,'-p',f'127.0.0.1:{port}:1883',
                     '--mount',f'type=bind,source={ROOT / "deploy/mosquitto-test.conf"},target=/mosquitto/config/mosquitto.conf,readonly',
                     'eclipse-mosquitto:2.0.22')
        port = int((await docker('port',name,'1883/tcp')).rsplit(':',1)[1])
        settings = MqttSettings('127.0.0.1',port,tls=False)
        async def peer():
            connection = PahoConnection(settings,'test-' + uuid.uuid4().hex,
                [prefix+'config/ack',prefix+'config/get',prefix+'status'])
            peers.append(connection)
            for attempt in range(20):
                try:
                    await connection.open()
                    return connection
                except (OSError, TimeoutError, ConnectionError):
                    await connection.close()
                    await asyncio.sleep(0.1)
            raise RuntimeError('Test Broker did not become ready')
        observer = await peer()
        async def receive(suffix, predicate=lambda value: True, timeout=15):
            async with asyncio.timeout(timeout):
                while True:
                    message = await observer.receive()
                    if message.topic == prefix + suffix:
                        value = json.loads(message.payload)
                        if predicate(value):
                            kind = {'config/ack':'configAck','config/get':'configGet','status':'status'}[suffix]
                            assert not validate_message(kind,message.payload), value
                            return value
        config = json.loads((ROOT/'contracts/examples/valid/config-empty.json').read_bytes())
        async def send(wire):
            await observer.publish(prefix+'config/set',json.dumps(wire).encode(),retain=True)
        directory = temporary.name
        local = AgentSettings('PLC-01',Path(directory))
        async def start():
            agent = AgentLifecycle(local,DeviceRuntime())
            agents.append(agent)
            await agent.start()
            service = MqttConfigService(agent,settings)
            services.append(service)
            await service.start()
            return agent,service
        print('Broker ready; publishing retained configuration.', flush=True)
        await send(config)  # retained before Agent connects
        agent,service = await start()
        applied = await receive('config/ack')
        assert applied['status']=='APPLIED' and agent.active_config_version==10, applied
        await send(config)
        assert (await receive('config/ack'))['status']=='APPLIED'
        conflict = json.loads(json.dumps(config))
        conflict['messageId']='conflict-10'
        conflict['config']['enabled']=False
        await send(conflict)
        rejected = await receive('config/ack')
        assert rejected['errors'][0]['code']=='VERSION_CONFLICT', rejected
        stale = dict(config,configVersion=9,messageId='stale-9')
        await send(stale)
        assert (await receive('config/ack'))['errors'][0]['code']=='STALE_VERSION'
        invalid = json.loads(json.dumps(config))
        invalid.update(configVersion=11,messageId='bad-11')
        invalid['config']['unknown']='not-allowed'
        await send(invalid)
        assert (await receive('config/ack'))['status']=='REJECTED'
        assert agent.active_config_version==10
        await send(config)
        await receive('config/ack')
        await service.close()
        offline = await receive('status',lambda value: value['reason']=='shutdown')
        assert offline['online'] is False
        await agent.close()
        agent,service = await start()
        restored = await receive('config/ack')
        assert restored['status']=='APPLIED' and restored['activeConfigVersion']==10
        print('Configuration and Agent restart checks passed; restarting Broker.', flush=True)
        old_session = service.session_id
        await observer.close()
        await docker('restart',name)
        observer = await peer()
        get = await receive('config/get',lambda value: value['sessionId']!=old_session)
        assert get['activeConfigVersion']==10
        unsupported = json.loads((ROOT/'contracts/examples/valid/config-set.json').read_bytes())
        unsupported.update(configVersion=11,messageId='unsupported-11')
        await send(unsupported)
        rejected = await receive('config/ack')
        assert rejected['status']=='REJECTED' and rejected['errors'][0]['code']=='APPLY_FAILED'
        assert agent.active_config_version==10
        disabled = json.loads(json.dumps(config))
        disabled.update(configVersion=12,messageId='disabled-12')
        disabled['config']['enabled']=False
        await send(disabled)
        ack = await receive('config/ack')
        assert ack['status']=='APPLIED' and agent.state=='DISABLED'
        # Exercise the actual publisher and Agent CLI, with a temporary settings file.
        mqtt_file = Path(directory)/'mqtt.json'
        mqtt_file.write_text(json.dumps({'host':'127.0.0.1','port':port,'tls':False}))
        config_file = Path(directory)/'config.json'
        disabled.update(configVersion=13,messageId='publisher-13')
        config_file.write_text(json.dumps(disabled))
        async def cli(module,*args):
            child = await asyncio.create_subprocess_exec(sys.executable,'-m',module,*args,cwd=ROOT,
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            try:
                out,err = await asyncio.wait_for(child.communicate(),15)
                assert child.returncode==0, err.decode(errors='replace')
                return [json.loads(line) for line in out.decode().splitlines()]
            finally:
                if child.returncode is None:
                    if sys.platform=='win32':
                        killer = await asyncio.create_subprocess_exec('taskkill','/PID',str(child.pid),'/T','/F',
                            stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
                        await killer.wait()
                    else:
                        child.kill()
                    await child.wait()
        published = await cli('tools.mqtt_config_send','--mqtt',str(mqtt_file),'--config',str(config_file),
                              '--gateway','PLC-01','--timeout','8')
        assert published[0]['status']=='APPLIED' and agent.active_config_version==13
        await service.close()
        await agent.close()
        bootstrap = Path(directory)/'bootstrap.json'
        bootstrap.write_text(json.dumps({'gatewayId':'PLC-01','dataDirectory':directory,
            'operationTimeoutSeconds':1,'queueCapacity':2}))
        local_events = await cli('tools.agent','--bootstrap',str(bootstrap),'--mqtt',str(mqtt_file),'--run-seconds','1')
        assert local_events[-1]['state']=='STOPPED' and local_events[-1]['activeConfigVersion']==13
        # A real abrupt socket loss lets Mosquitto publish the configured LWT.
        from plcnext_iot.messaging.messages import status
        will_connection = PahoConnection(settings,'will-test-'+uuid.uuid4().hex,[],
            will=(prefix+'status',status('PLC-01','will-session',False,'connection_lost')))
        peers.append(will_connection)
        await will_connection.open()
        will_connection._client.socket().shutdown(socket.SHUT_RDWR)
        lwt = await receive('status',lambda value: value['sessionId']=='will-session')
        assert lwt['online'] is False and lwt['timestamp'] is None
        print(json.dumps({'result':'PASS','broker':'eclipse-mosquitto:2.0.22',
            'checks':['retained_config','applied_ack','duplicate','version_conflict','stale_version',
                      'invalid_config','graceful_offline','agent_restart','broker_restart','new_session','disabled_config',
                      'unsupported_driver_rejected','publisher_cli','agent_cli','lwt']}))
    finally:
        for service in services:
            await service.close()
        for agent in agents:
            await agent.close()
        for connection in peers:
            await connection.close()
        try:
            await docker('rm','-f',name)
        except RuntimeError:
            pass
        temporary.cleanup()


if __name__ == '__main__':
    asyncio.run(smoke())
