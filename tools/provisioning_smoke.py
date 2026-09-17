"""HTTP setup -> persisted credentials -> real MQTT -> restart and in-place reload."""
import asyncio
import json
from pathlib import Path
import tempfile
import urllib.request
import uuid

from tools.appliance import serve
from tools.telemetry_smoke import free_port
from tools.mqtt_smoke import docker
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.messaging.transport import PahoConnection

ROOT=Path(__file__).resolve().parents[1]


async def smoke():
    name,other='iot-setup-'+uuid.uuid4().hex[:10],'iot-setup-'+uuid.uuid4().hex[:10]
    task=observer=observer2=None
    with tempfile.TemporaryDirectory(prefix='iot-setup-') as directory:
        try:
            broker_port,second_port,web_port=free_port(),free_port(),free_port()
            for container,host_port in ((name,broker_port),(other,second_port)):
                await docker('run','-d','--rm','--name',container,'-p',f'127.0.0.1:{host_port}:1883',
                    '--mount',f'type=bind,source={ROOT/"deploy/mosquitto-test.conf"},target=/mosquitto/config/mosquitto.conf,readonly',
                    'eclipse-mosquitto:2.0.22')
            topics=['iot/v1/gateway/PLC-SETUP/config/get']
            observer=PahoConnection(MqttSettings('127.0.0.1',broker_port,tls=False),'observer-'+uuid.uuid4().hex,topics)
            observer2=PahoConnection(MqttSettings('127.0.0.1',second_port,tls=False),'observer2-'+uuid.uuid4().hex,topics)
            await observer.open();await observer2.open()
            async def request(path, value=None, token=None):
                def call():
                    req=urllib.request.Request(f'http://127.0.0.1:{web_port}'+path,
                        data=json.dumps(value).encode() if value is not None else None,
                        headers={'Content-Type':'application/json','X-Setup-Token':token or ''})
                    with urllib.request.urlopen(req,timeout=5) as response:
                        return json.load(response)
                return await asyncio.to_thread(call)
            async def start():
                nonlocal task
                task=asyncio.create_task(serve(Path(directory),'127.0.0.1',web_port))
                async with asyncio.timeout(5):
                    while True:
                        try:return await request('/api/status')
                        except OSError:await asyncio.sleep(.05)
            state=await start()
            assert not state['configured']
            value=dict(gatewayId='PLC-SETUP',host='127.0.0.1',port=broker_port,tls=False,username='',password='',caPem='')
            assert (await request('/api/test',value,state['token']))['code']=='CONNECTED'
            assert (await request('/api/setup',value,state['token']))['code']=='SAVED'
            message=await asyncio.wait_for(observer.receive(),10)
            assert json.loads(message.payload)['gatewayId']=='PLC-SETUP'
            assert (await request('/api/config'))['host']=='127.0.0.1'
            task.cancel();await asyncio.gather(task,return_exceptions=True);task=None
            state=await start()
            assert state['configured']
            message=await asyncio.wait_for(observer.receive(),10)
            assert json.loads(message.payload)['gatewayId']=='PLC-SETUP'
            print('PASS: HTTP connection test, save, automatic MQTT start and restart recovery')
            # A saved revision is applied in place: the process and its port stay.
            assert (await request('/api/status'))['changeSeq']==0
            assert (await request('/api/setup',dict(value,port=second_port),state['token']))['code']=='UPDATED'
            message=await asyncio.wait_for(observer2.receive(),15)
            assert json.loads(message.payload)['gatewayId']=='PLC-SETUP'
            async with asyncio.timeout(10):
                while (await request('/api/status'))['appliedSeq']<1:
                    await asyncio.sleep(.1)
            assert task is not None and not task.done(),'the setup process must keep serving'
            assert (await request('/api/status'))['configured']
            print('PASS: broker revision applied in place on the new broker without restarting the process')
        finally:
            if task is not None:
                task.cancel();await asyncio.gather(task,return_exceptions=True)
            if observer is not None:await observer.close()
            if observer2 is not None:await observer2.close()
            await docker('rm','-f',name)
            await docker('rm','-f',other)


if __name__=='__main__':
    asyncio.run(smoke())
