"""Full Agent MQTT coordinator + production Outbox policy, injected I/O only."""
import asyncio
import hashlib
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'tests')]
from test_timed_outbox import DurableRepository,wire
from test_phase1_outbox import ack
from test_platform_time import provider,sync,ready,BASE
from plcnext_iot.core.boot import ProcessBootContext
from plcnext_iot.core.platform_time import PlatformTime,TimeUnavailable
from plcnext_iot.points.samples import SampleBuffer
from plcnext_iot.storage.outbox import OutboxStore
from plcnext_iot.reporting.service import TelemetryPipeline
from plcnext_iot.messaging.config_service import MqttConfigService
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.messaging.transport import Incoming,PublishRejected,PahoConnection
from plcnext_iot.messaging.timed_control import publish_control
from plcnext_iot.core.lifecycle import ApplyResult


class PlatformMqttTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_coordinator_bootstrap_ack_during_unsynced_and_two_control_gate(self):
        context=ProcessBootContext('boot-platform-test')
        clock=PlatformTime('PLC-01',context.boot_id,basis='site-hub-1',drift_ppm=50,
                           drift_evidence='TEST_FIXTURE_ONLY',system=lambda:946684800000)
        context.time_provider=clock
        repo=DurableRepository();raw=wire('historic')
        repo.records['historic']=(dict(message_id='historic',hash=hashlib.sha256(raw).hexdigest(),created=0,
            size=len(raw),attempts=1,next_due=10**15,state='PENDING',reason=None,local_order=1),raw)
        repo.order=2
        runtime=SimpleNamespace(snapshot=None,runners={},boot_context=context,sample_generation=0)
        samples=SampleBuffer(boot_context=context,time_provider=clock,gateway_id='PLC-01')
        pipeline=TelemetryPipeline(runtime,samples,Path('unused-memory-directory'),'PLC-01',boot_context=context,
            store_factory=lambda _path,_gateway:OutboxStore(None,'PLC-01',repository=repo,session=context.outbox_session,time_provider=clock))
        await pipeline.start()
        agent=SimpleNamespace(state='WAITING_CONFIG',runtime=runtime,settings=SimpleNamespace(gateway_id='PLC-01'),
            active_config_version=0,active_message_id=None)
        published=[];acks=[];holder=[]
        original_ack=pipeline.acknowledge
        async def watched_ack(payload):
            acks.append(clock.snapshot().time_valid)
            return await original_ack(payload)
        pipeline.acknowledge=watched_ack
        class Connection:
            def __init__(self,_settings,_id,subscriptions,**options):
                self.subscriptions=subscriptions;self.connected=False;self.dropped=0
                self.incoming=asyncio.Queue();holder.append(self)
            async def open(self):
                self.connected=True
                self.incoming.put_nowait(Incoming('iot/v1/gateway/PLC-01/data/ack',ack('historic'),False,clock.read_mono()))
            def submit_publish(self,topic,payload,retain=False):
                message=json.loads(payload);published.append((topic,message,clock.valid(None,'SEND')))
                if topic.endswith('time/request'):
                    sent=message['clientMonoMs'];now=clock.read_mono()
                    utc=BASE+now
                    response=dict(message,serverReceivedAt=utc,serverSentAt=utc)
                    self.incoming.put_nowait(Incoming('iot/v1/gateway/PLC-01/time/response',json.dumps(response).encode(),False,now))
                return object()
            async def wait_puback(self,_info):await asyncio.sleep(0)
            async def receive(self):return await self.incoming.get()
            async def close(self):self.connected=False
        settings=MqttSettings('unused.invalid',1883,tls=False,time_mode='HUB_MQTT',time_basis_ref='site-hub-1',drift_ppm=50,drift_evidence='TEST_FIXTURE_ONLY')
        service=MqttConfigService(agent,settings,telemetry=pipeline)
        try:
            with patch('plcnext_iot.messaging.config_service.PahoConnection',Connection):
                await service.start()
                async with asyncio.timeout(4):
                    while not clock.valid(None,'SEND'):
                        if service.last_error=='MQTT_SERVICE_FAILED':raise AssertionError(service.last_error)
                        await asyncio.sleep(.02)
                self.assertEqual(acks,[False])
                self.assertNotIn('historic',repo.records)
                topics=[p[0].rsplit('/',1)[-1] for p in published]
                self.assertGreaterEqual(topics.count('request'),3)
                self.assertLess(topics.index('heartbeat'),topics.index('get'))
                for topic,message,_ in published:
                    if not topic.endswith('time/request'):
                        self.assertGreater(message['timestamp'],BASE)
                self.assertIn('iot/v1/gateway/PLC-01/time/response',holder[0].subscriptions)
                self.assertIsNone(service.last_error)
                await service.close()
        finally:
            if service._task is not None and not service._task.done():await service.close()
            await pipeline.close()

    async def test_sdk_rejection_never_marks_get_submitted_or_opens_data(self):
        c,p=provider();sync(p,c)
        class Connection:
            def submit_publish(self,topic,payload,retain=False):
                if topic.endswith('config/get'):raise PublishRejected('Rejected')
                return object()
            async def wait_puback(self,info):pass
        connection=Connection()
        await publish_control(connection,'heartbeat',lambda stamp:b'{}',p,kind='heartbeat')
        with self.assertRaises(PublishRejected):
            await publish_control(connection,'config/get',lambda stamp:b'{}',p,kind='config/get')
        self.assertTrue(p.heartbeat_submitted);self.assertFalse(p.get_submitted)
        self.assertFalse(p.valid(None,'SEND'))

    async def test_deferred_ack_is_bounded_and_rebuilt_after_time_and_session_recovery(self):
        c,p=provider();context=ProcessBootContext('boot-one');context.time_provider=p
        agent=SimpleNamespace(runtime=SimpleNamespace(boot_context=context),settings=SimpleNamespace(gateway_id='PLC-01'),
                              active_config_version=0,active_message_id=None)
        service=MqttConfigService(agent,MqttSettings('unused.invalid'))
        posted=[]
        class Connection:
            def submit_publish(self,topic,payload,retain=False):posted.append(json.loads(payload));return object()
            async def wait_puback(self,info):pass
        connection=Connection()
        command=json.dumps(dict(gatewayId='PLC-01',messageId='config-one',configVersion=1)).encode()
        service._remember_ack(command,ApplyResult('REJECTED',0,'RESOURCE_LIMIT'),False)
        await service._flush_ack(connection);self.assertEqual(posted,[])
        sync(p,c);ready(p);await service._flush_ack(connection)
        self.assertEqual(len(posted),1);self.assertEqual(posted[0]['timestamp'],p.snapshot().utc_ms)
        self.assertEqual(len(service._deferred_acks),0)
        for i in range(100):
            command=json.dumps(dict(gatewayId='PLC-01',messageId='config-'+str(i),configVersion=1)).encode()
            service._remember_ack(command,ApplyResult('REJECTED',0,'RESOURCE_LIMIT'),False)
        self.assertLessEqual(len(service._deferred_acks),32)
        self.assertLessEqual(sum(v[2] for v in service._deferred_acks.values()),65536)
        p.connect('another-session');await service._flush_ack(connection)
        self.assertEqual(len(posted),1)

    async def test_callback_stamps_m4_before_dequeue_and_time_flood_cannot_starve_ack(self):
        import paho.mqtt.client as mqtt
        now=[1000]
        conn=PahoConnection(MqttSettings('unused.invalid'),'test',
            ['x/time/response','x/data/ack'],mono_ms=lambda:now[0])
        conn._client=object();conn._ready.set()
        for topic in ('x/time/response','x/time/response','x/data/ack'):
            msg=mqtt.MQTTMessage();msg.topic=topic.encode();msg.payload=b'{}'
            conn._on_message(None,None,msg)
        now[0]=9000
        first=await conn.receive();second=await conn.receive()
        self.assertEqual(first.received_mono_ms,1000)
        self.assertEqual(second.topic,'x/data/ack')
        conn._client=None


if __name__=='__main__':unittest.main()
