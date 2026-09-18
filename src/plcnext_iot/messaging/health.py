"""Current device state and non-retained heartbeat, independent of config ingress."""
import asyncio
import time
import uuid

from plcnext_iot.messaging.messages import encode


class HealthPublisher:
    def __init__(self, agent, telemetry=None):
        self.agent, self.telemetry = agent, telemetry
        self.boot_id = telemetry.engine.boot_id if telemetry else 'boot-' + uuid.uuid4().hex
        self.started = time.monotonic()

    def snapshot(self, session_id, queue, now=None, timestamp=None):
        now = time.monotonic() if now is None else now
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
                current = set()
                for device in heartbeat['devices']:
                    key = device['deviceId']
                    current.add(key)
                    signature = (version, device['status'], device['lastError'])
                    if version and sent.get(key) != signature:
                        payload = {k: heartbeat[k] for k in ('schemaVersion', 'gatewayId', 'timestamp', 'sessionId')}
                        payload.update(configVersion=version, device=device)
                        await connection.publish(prefix+'device/status', encode(payload))
                        sent[key] = signature
                sent = {k: v for k, v in sent.items() if k in current}
                if now >= next_heartbeat:
                    await connection.publish(prefix+'heartbeat', encode(heartbeat))
                    next_heartbeat = now + 30
            await asyncio.sleep(0.5)
