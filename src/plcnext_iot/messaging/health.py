"""Current device state and non-retained heartbeat, independent of config ingress."""
import asyncio
import time

from plcnext_iot.messaging.messages import encode
from plcnext_iot.core.boot import ProcessBootContext
from plcnext_iot.core.platform_time import TimeUnavailable
from plcnext_iot.messaging.timed_control import publish_control


class HealthPublisher:
    def __init__(self, agent, telemetry=None, *, boot_context=None):
        self.agent, self.telemetry = agent, telemetry
        context = telemetry.engine.boot_context if telemetry else boot_context
        if telemetry is not None and boot_context is not None and context is not boot_context:
            raise ValueError('Health and telemetry must use the same process boot context')
        self.boot_context = context if context is not None else ProcessBootContext()
        self.boot_id = self.boot_context.boot_id
        self.started = self.boot_context.started_at
        self.time_provider=getattr(self.boot_context,'time_provider',None)

    def snapshot(self, session_id, queue, now=None, timestamp=None):
        now = time.monotonic() if now is None else now
        if timestamp is None and self.time_provider is not None:
            reading=self.time_provider.snapshot()
            if not self.time_provider.valid(None,'CONTROL_SEND'):
                return None
            timestamp=reading.utc_ms if reading.time_valid else reading.system_utc_ms
        timestamp = time.time_ns() // 1_000_000 if timestamp is None else timestamp
        agent = self.agent
        config = agent.runtime.snapshot
        version = agent.active_config_version
        if (agent.state not in ('WAITING_CONFIG', 'RUNNING', 'DISABLED') or version is None
                or (config is not None and config.config_version != version)):
            return None
        devices, good, total = [], 0, 0
        for device in config.devices if config else ():
            runner = agent.runtime.runners.get(device.device_id)
            if not config.enabled or not device.enabled or not any(p.enabled for p in device.points):
                state = dict(deviceId=device.device_id, status='DISABLED', lastError=None, lastGoodTimestamp=None)
            elif runner is not None and hasattr(runner, 'diagnostics'):
                state, count, size = runner.diagnostics(device, now)
                good += count
                total += size
            else:
                state = dict(deviceId=device.device_id, status='CONNECTING', lastError=None, lastGoodTimestamp=None)
                total += sum(p.enabled for p in device.points)
            devices.append(state)
        return dict(schemaVersion=1, gatewayId=agent.settings.gateway_id, timestamp=timestamp,
                    sessionId=session_id, bootId=self.boot_id, agentVersion='0.2.0',
                    activeConfigVersion=version, uptimeSeconds=max(0, int(now - self.started)),
                    agentState=agent.state, devices=devices,
                    points=dict(total=total, good=good, bad=total-good), queue=queue)

    async def run(self, connection, prefix, session_id):
        sent, next_heartbeat = {}, 0
        while True:
            stats = await self.telemetry.stats() if self.telemetry else {}
            queue = dict(batches=stats.get('pending', 0) + stats.get('dead', 0),
                         payloadBytes=stats.get('payload_bytes', 0), droppedBatches=stats.get('dropped', 0))
            heartbeat = self.snapshot(session_id, queue)
            now = time.monotonic()
            if heartbeat is not None:
                version = heartbeat['activeConfigVersion']
                if now >= next_heartbeat or (self.time_provider is not None and not self.time_provider.heartbeat_submitted):
                    try:
                        await publish_control(connection,prefix+'heartbeat',
                            lambda stamp:encode(dict(heartbeat,timestamp=heartbeat['timestamp'] if stamp is None else stamp)),
                            self.time_provider,kind='heartbeat')
                        next_heartbeat = now + 30
                    except TimeUnavailable:
                        await asyncio.sleep(.05)
                        continue
                current = set()
                for device in heartbeat['devices']:
                    key = device['deviceId']
                    current.add(key)
                    signature = (version, device['status'], device['lastError'])
                    if version and sent.get(key) != signature:
                        payload = {k: heartbeat[k] for k in ('schemaVersion', 'gatewayId', 'timestamp', 'sessionId')}
                        payload.update(configVersion=version, device=device)
                        try:
                            await publish_control(connection,prefix+'device/status',
                                lambda stamp:encode(dict(payload,timestamp=payload['timestamp'] if stamp is None else stamp)),self.time_provider)
                        except TimeUnavailable:
                            break
                        sent[key] = signature
                sent = {k: v for k, v in sent.items() if k in current}
            await asyncio.sleep(0.5)
