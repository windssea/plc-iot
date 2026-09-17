"""Bounded Paho network-thread bridge. MQTT PUBACK is not application ACK."""
import asyncio
from dataclasses import dataclass
import queue
import ssl
import threading

import paho.mqtt.client as mqtt


@dataclass(frozen=True)
class Incoming:
    topic: str
    payload: bytes
    retained: bool


async def _thread(function, *args):
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


class PahoConnection:
    def __init__(self, settings, client_id, subscriptions, *, will=None):
        self.settings, self.client_id = settings, client_id
        self.subscriptions = tuple(subscriptions)
        self.will = will
        self._client = None
        self._incoming = queue.Queue(maxsize=8)
        self._ready = threading.Event()
        self._lost = threading.Event()
        self._error = None
        self._close_task = None
        self.dropped = 0

    @property
    def connected(self):
        return self._client is not None and self._ready.is_set() and not self._lost.is_set() and self._error is None

    def _on_connect(self, client, _userdata, _flags, reason, _properties):
        if reason.is_failure:
            self._error = 'CONNECT_REJECTED'
            self._ready.set()
        elif self.subscriptions:
            rc, _ = client.subscribe([(topic,1) for topic in self.subscriptions])
            if rc != mqtt.MQTT_ERR_SUCCESS:
                self._error = 'SUBSCRIBE_FAILED'
                self._ready.set()
        else:
            self._ready.set()

    def _on_subscribe(self, _client, _userdata, _mid, reasons, _properties):
        if len(reasons) != len(self.subscriptions) or any(reason.is_failure or reason.value != 1 for reason in reasons):
            self._error = 'SUBSCRIBE_REJECTED'
        self._ready.set()

    def _on_disconnect(self, _client, _userdata, _flags, _reason, _properties):
        self._lost.set()

    def _on_message(self, _client, _userdata, message):
        if message.topic not in self.subscriptions or len(message.payload) > 2 * 1024 * 1024:
            self.dropped += 1
            return
        try:
            self._incoming.put_nowait(Incoming(message.topic,message.payload,bool(message.retain)))
        except queue.Full:
            self.dropped += 1

    async def open(self):
        if self._client is not None:
            raise RuntimeError('Connection already opened')
        self._close_task = None
        self._ready.clear()
        self._lost.clear()
        self._error = None
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=self.client_id,
                             clean_session=True,protocol=mqtt.MQTTv311,reconnect_on_failure=False)
        self._client = client
        client.connect_timeout = 3
        client.max_inflight_messages_set(8)
        client.max_queued_messages_set(32)
        client.on_connect = self._on_connect
        client.on_subscribe = self._on_subscribe
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        try:
            if self.settings.tls:
                client.tls_set_context(ssl.create_default_context(cafile=self.settings.ca_file))
            if self.settings.username is not None:
                client.username_pw_set(self.settings.username,self.settings.password)
            if self.will:
                topic,payload = self.will
                client.will_set(topic,payload,qos=1,retain=True)
            await _thread(client.connect,self.settings.host,self.settings.port,self.settings.keepalive)
            client.loop_start()
            async with asyncio.timeout(5):
                while not self._ready.is_set():
                    if self._lost.is_set():
                        raise ConnectionError('MQTT connection lost before SUBACK')
                    await asyncio.sleep(0.02)
            if not self.connected:
                raise ConnectionError('MQTT connection or subscription rejected')
        except BaseException:
            await self.close()
            raise

    async def receive(self):
        while True:
            if not self.connected:
                raise ConnectionError('MQTT disconnected')
            try:
                return self._incoming.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.02)

    async def publish(self, topic, payload, *, retain=False):
        if not self.connected:
            raise ConnectionError('MQTT disconnected')
        info = self._client.publish(topic,payload,qos=1,retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError('MQTT publish queue unavailable')
        async with asyncio.timeout(5):
            while not info.is_published():
                if not self.connected:
                    raise ConnectionError('MQTT disconnected before PUBACK')
                await asyncio.sleep(0.02)

    async def close(self):
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self):
        client,self._client = self._client,None
        self._lost.set()
        if client is not None:
            client.disconnect()
            await _thread(client.loop_stop)
        while not self._incoming.empty():
            try:
                self._incoming.get_nowait()
            except queue.Empty:
                break
