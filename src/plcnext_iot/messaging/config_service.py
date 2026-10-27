"""MQTT configuration ingress owned by the Agent; reconnect never cancels apply."""
import asyncio
import uuid
from collections import OrderedDict
from dataclasses import replace

from plcnext_iot.messaging.messages import config_ack, config_patch_ack, config_get, status, identity
from plcnext_iot.core.lifecycle import ApplyResult
from plcnext_iot.messaging.transport import PahoConnection
from plcnext_iot.messaging.health import HealthPublisher
from plcnext_iot.core.platform_time import TimeUnavailable
from plcnext_iot.messaging.timed_control import publish_control
from plcnext_iot.messaging.messages import encode
from plcnext_iot.core.bounds import retained_bytes


class MqttConfigService:
    def __init__(self, agent, settings, *, telemetry=None):
        self.agent,self.settings = agent,settings
        self.telemetry=telemetry
        context=getattr(telemetry,'context',None) or getattr(agent.runtime,'boot_context',None)
        self.time_provider=getattr(context,'time_provider',None)
        self.health=HealthPublisher(agent, telemetry,boot_context=context)
        self._deferred_acks=OrderedDict()
        self.gateway = agent.settings.gateway_id
        self.prefix = 'iot/v1/gateway/' + self.gateway + '/'
        self.session_id = None
        self.last_error = None
        self.dropped = 0
        self.unacknowledged = 0
        self._connection = None
        self._stop = asyncio.Event()
        self._task = None
        self._failed = asyncio.Event()

    @property
    def connected(self):
        return self._connection is not None and self._connection.connected

    async def start(self):
        if self._task is not None or self._stop.is_set():
            raise RuntimeError('MQTT service starts once')
        if self.agent.state not in ('RUNNING','DISABLED','WAITING_CONFIG'):
            raise RuntimeError('Start local lifecycle before MQTT')
        self._task = asyncio.create_task(self._run(),name='iot-mqtt-config')

    async def wait_failed(self):
        await self._failed.wait()

    async def close(self):
        self._stop.set()
        if self._task is not None:
            await asyncio.shield(self._task)

    async def _run(self):
        delay = 1
        try:
            while not self._stop.is_set() and self.agent.state != 'FAILED':
                self.session_id = 'session-' + uuid.uuid4().hex
                if self.time_provider is not None:
                    self.time_provider.connect(self.session_id)
                topics=[self.prefix+'config/set', self.prefix+'config/patch']
                platform=self.time_provider is not None and self.time_provider.platform
                if platform:
                    topics.append(self.prefix+'time/response')
                if self.telemetry is not None:
                    topics.append(self.prefix+'data/ack')
                options={'mono_ms':self.time_provider.read_mono} if platform else {}
                connection = PahoConnection(self.settings,'iot-' + self.gateway,topics,
                    will=(self.prefix+'status',status(self.gateway,self.session_id,False,'connection_lost')),**options)
                self._connection = connection
                sender=health=None
                sync_publishes=set()
                try:
                    await connection.open()
                    delay = 1
                    self.last_error = None
                    if self.time_provider is None:
                        await connection.publish(self.prefix+'status',status(self.gateway,self.session_id,True,'connected'),retain=True)
                        await self._request_config(connection)
                    health=asyncio.create_task(self.health.run(connection,self.prefix,self.session_id),name='iot-health')
                    if self.telemetry is not None:
                        sender=asyncio.create_task(self.telemetry.send_loop(connection,self.prefix+'data'),name='iot-telemetry-send')
                    next_get = 0 if self.time_provider is not None else asyncio.get_running_loop().time() + 30
                    status_submitted=self.time_provider is None
                    while not self._stop.is_set() and self.agent.state != 'FAILED':
                        for task in list(sync_publishes):
                            if task.done():
                                sync_publishes.remove(task);task.result()
                        if platform and len(sync_publishes)<8:
                            request=self.time_provider.request()
                            if request is not None:
                                info=connection.submit_publish(self.prefix+'time/request',request)
                                sync_publishes.add(asyncio.create_task(connection.wait_puback(info)))
                        if health.done():
                            health.result()
                            raise ConnectionError('Health publisher stopped')
                        if sender is not None and sender.done():
                            sender.result()
                            raise ConnectionError('Telemetry sender stopped')
                        try:
                            incoming = await asyncio.wait_for(connection.receive(),0.2)
                        except TimeoutError:
                            incoming = None
                        if incoming is not None:
                            if platform and incoming.topic==self.prefix+'time/response':
                                if incoming.received_mono_ms is not None:
                                    self.time_provider.response(incoming.payload,received_mono_ms=incoming.received_mono_ms,
                                        session=self.session_id,retained=incoming.retained)
                                continue
                            if incoming.topic==self.prefix+'data/ack':
                                if not incoming.retained:
                                    await self.telemetry.acknowledge(incoming.payload)
                                continue
                            # Parsing and ACK encoding run off-loop. Queueing is bounded;
                            # raw JSON with no trustworthy correlation cannot be answered.
                            request = await asyncio.to_thread(identity,incoming.payload,self.gateway)
                            if request is None:
                                self.unacknowledged += 1
                                continue
                            patch = incoming.topic == self.prefix+'config/patch'
                            if patch and incoming.retained:
                                result = ApplyResult('REJECTED', self.agent.active_config_version, 'RETAINED_PATCH',
                                                     active_message_id=self.agent.active_message_id)
                            else:
                                result = (await self.agent.submit(incoming.payload, kind='configPatch')
                                          if patch else await self.agent.submit(incoming.payload))
                            if self.time_provider is not None:
                                self._remember_ack(incoming.payload,result,patch)
                            else:
                                ack = await asyncio.to_thread(config_patch_ack if patch else config_ack,
                                                              incoming.payload,result,self.gateway)
                                if ack is not None:
                                    await connection.publish(self.prefix+('config/patch/ack' if patch else 'config/ack'),ack)
                                else:
                                    self.unacknowledged += 1
                            if patch:
                                await self._request_config(connection)
                        ready=self.time_provider is None or (self.time_provider.valid(None,'CONTROL_SEND')
                                and self.time_provider.heartbeat_submitted)
                        if ready and not status_submitted:
                            try:
                                await publish_control(connection,self.prefix+'status',
                                    lambda stamp:status(self.gateway,self.session_id,True,'connected',timestamp=stamp),
                                    self.time_provider,retain=True)
                                status_submitted=True
                            except TimeUnavailable:pass
                        if ready:
                            try:await self._flush_ack(connection)
                            except TimeUnavailable:pass
                        if ready and (asyncio.get_running_loop().time() >= next_get
                                or self.time_provider is not None and not self.time_provider.get_submitted):
                            if await self._request_config(connection):
                                next_get = asyncio.get_running_loop().time() + 30
                    if connection.connected and (self.time_provider is None or self.time_provider.valid(None,'CONTROL_SEND')):
                        try:
                            await publish_control(connection,self.prefix+'status',
                                lambda stamp:status(self.gateway,self.session_id,False,'shutdown',timestamp=stamp),
                                self.time_provider,retain=True)
                        except TimeUnavailable:pass
                except (OSError, TimeoutError, ConnectionError):
                    self.last_error = 'MQTT_UNAVAILABLE'
                finally:
                    for task in sync_publishes:task.cancel()
                    await asyncio.gather(*sync_publishes,return_exceptions=True)
                    if health is not None:
                        health.cancel()
                        await asyncio.gather(health,return_exceptions=True)
                    if sender is not None:
                        sender.cancel()
                        await asyncio.gather(sender,return_exceptions=True)
                    self.dropped += connection.dropped
                    await connection.close()
                    if self.time_provider is not None:
                        self.time_provider.disconnect()
                    self._connection = None
                if not self._stop.is_set() and self.agent.state != 'FAILED':
                    try:
                        await asyncio.wait_for(self._stop.wait(),delay)
                    except TimeoutError:
                        pass
                    delay = min(30,delay * 2)
        except Exception:
            self.last_error = 'MQTT_SERVICE_FAILED'
            self._failed.set()

    async def _request_config(self, connection):
        if self.time_provider is not None and (not self.time_provider.valid(None,'CONTROL_SEND')
                or not self.time_provider.heartbeat_submitted):
            return
        version = self.agent.active_config_version
        message_id = self.agent.active_message_id
        if version is not None and (version == 0 or message_id is not None):
            try:
                await publish_control(connection,self.prefix+'config/get',lambda stamp:config_get(
                    self.gateway,version,self.session_id,active_message_id=message_id,capabilities=True,timestamp=stamp),
                    self.time_provider,kind='config/get')
                return True
            except TimeUnavailable:return False

    def _remember_ack(self,payload,result,patch):
        request=identity(payload,self.gateway)
        if request is None:return
        small=encode(dict(gatewayId=self.gateway,messageId=request[0],configVersion=request[1]))
        key=(patch,*request)
        self._deferred_acks[key]=(small,result,retained_bytes(result)+len(small)+512)
        self._deferred_acks.move_to_end(key)
        while len(self._deferred_acks)>32 or sum(r[2] for r in self._deferred_acks.values())>65536:
            self._deferred_acks.popitem(last=False);self.unacknowledged+=1

    async def _flush_ack(self,connection):
        if not self._deferred_acks or not self.time_provider.valid(None,'CONTROL_SEND'):
            return
        key,(payload,result,_)=next(iter(self._deferred_acks.items()))
        version=self.agent.active_config_version
        # Snapshot APPLIED cannot falsely report an older active version after another commit.
        if not key[0] and result.status=='APPLIED' and version!=key[2]:
            self._deferred_acks.pop(key);self.unacknowledged+=1;return
        result=replace(result,active_config_version=version,active_message_id=self.agent.active_message_id)
        encoder=config_patch_ack if key[0] else config_ack
        token=self.time_provider.token()
        stamp=self.time_provider.require(token,'CONTROL_SEND').utc_ms
        if not self.time_provider.platform:stamp=self.time_provider.snapshot().system_utc_ms
        wire=encoder(payload,result,self.gateway,timestamp=stamp)
        if wire is None:
            self._deferred_acks.pop(key);self.unacknowledged+=1;return
        # Re-encode with a new logical timestamp at each actual submission; no old-session wire cache.
        await publish_control(connection,self.prefix+('config/patch/ack' if key[0] else 'config/ack'),
            lambda current:encoder(payload,result,self.gateway,timestamp=current),self.time_provider)
        self._deferred_acks.pop(key,None)
