"""MQTT configuration ingress owned by the Agent; reconnect never cancels apply."""
import asyncio
import uuid

from plcnext_iot.messaging.messages import config_ack, config_get, status, identity
from plcnext_iot.messaging.transport import PahoConnection
from plcnext_iot.messaging.health import HealthPublisher


class MqttConfigService:
    def __init__(self, agent, settings, *, telemetry=None):
        self.agent,self.settings = agent,settings
        self.telemetry=telemetry
        self.health=HealthPublisher(agent, telemetry)
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
                topics=[self.prefix+'config/set']
                if self.telemetry is not None:
                    topics.append(self.prefix+'data/ack')
                connection = PahoConnection(self.settings,'iot-' + self.gateway,topics,
                    will=(self.prefix+'status',status(self.gateway,self.session_id,False,'connection_lost')))
                self._connection = connection
                sender=health=None
                try:
                    await connection.open()
                    delay = 1
                    self.last_error = None
                    await connection.publish(self.prefix+'status',status(self.gateway,self.session_id,True,'connected'),retain=True)
                    await self._request_config(connection)
                    health=asyncio.create_task(self.health.run(connection,self.prefix,self.session_id),name='iot-health')
                    if self.telemetry is not None:
                        sender=asyncio.create_task(self.telemetry.send_loop(connection,self.prefix+'data'),name='iot-telemetry-send')
                    next_get = asyncio.get_running_loop().time() + 30
                    while not self._stop.is_set() and self.agent.state != 'FAILED':
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
                            if incoming.topic==self.prefix+'data/ack':
                                await self.telemetry.acknowledge(incoming.payload)
                                continue
                            # Parsing and ACK encoding run off-loop. Queueing is bounded;
                            # raw JSON with no trustworthy correlation cannot be answered.
                            request = await asyncio.to_thread(identity,incoming.payload,self.gateway)
                            if request is None:
                                self.unacknowledged += 1
                                continue
                            result = await self.agent.submit(incoming.payload)
                            ack = await asyncio.to_thread(config_ack,incoming.payload,result,self.gateway)
                            if ack is not None:
                                await connection.publish(self.prefix+'config/ack',ack)
                            else:
                                self.unacknowledged += 1
                        if asyncio.get_running_loop().time() >= next_get:
                            await self._request_config(connection)
                            next_get = asyncio.get_running_loop().time() + 30
                    if connection.connected:
                        await connection.publish(self.prefix+'status',status(self.gateway,self.session_id,False,'shutdown'),retain=True)
                except (OSError, TimeoutError, ConnectionError):
                    self.last_error = 'MQTT_UNAVAILABLE'
                finally:
                    if health is not None:
                        health.cancel()
                        await asyncio.gather(health,return_exceptions=True)
                    if sender is not None:
                        sender.cancel()
                        await asyncio.gather(sender,return_exceptions=True)
                    self.dropped += connection.dropped
                    await connection.close()
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
        version = self.agent.active_config_version
        if version is not None:
            await connection.publish(self.prefix+'config/get',config_get(self.gateway,version,self.session_id))
