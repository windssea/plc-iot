"""Bounded real TCP loopback startup trace; no broker, DB or physical device."""
import argparse
import asyncio
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import struct
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.devices.modbus import ModbusRunner
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.drivers.modbus import AsyncModbusTcpClient, ModbusReader
from plcnext_iot.points.samples import SampleBuffer


async def verify(config_path, seconds):
    wire = json.loads(config_path.read_bytes())
    snapshot = ConfigSnapshot.parse(json.dumps(wire).encode(), wire['gatewayId'])
    connections, handlers, accepted, requests, traces = set(), set(), [], [], []

    async def handle(reader, writer):
        handlers.add(asyncio.current_task())
        connections.add(writer)
        accepted.append(asyncio.get_running_loop().time())
        try:
            while True:
                tid, protocol, length, unit = struct.unpack('>HHHB', await reader.readexactly(7))
                fc, address, count = struct.unpack('>BHH', await reader.readexactly(length - 1))
                if protocol or fc not in (3, 4):
                    raise ValueError('This fixture accepts only FC3/4')
                requests.append((unit, fc, address, count))
                # Fixed, unit/address-specific engineering inputs; no remote I/O.
                values = [(unit * 100 + address + i) % 30000 for i in range(count)]
                body = bytes((fc, count * 2)) + struct.pack('>' + 'H' * count, *values)
                writer.write(struct.pack('>HHHB', tid, 0, len(body) + 1, unit) + body)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            connections.discard(writer)
            writer.close()
            await writer.wait_closed()
            handlers.discard(asyncio.current_task())

    server = await asyncio.start_server(handle, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    devices = tuple(replace(d, connection=replace(d.connection, host='127.0.0.1', port=port))
                    for d in snapshot.devices)
    assert len(devices) == 50 and sum(len(d.points) for d in devices) == 250
    assert all(d.protocol == 'modbus_tcp' for d in devices)
    assert all(p.poll_interval_ms == 5000 for d in devices for p in d.points)
    runtime = DeviceRuntime()
    runtime.snapshot = replace(snapshot, devices=devices)
    samples = SampleBuffer(10000, gateway_id=snapshot.gateway_id)
    runtime._runners = {d.device_id: ModbusRunner(d, runtime, samples) for d in devices}
    loop = asyncio.get_running_loop()
    connect = AsyncModbusTcpClient.connect
    read = ModbusReader.read_block
    acquire = runtime.read_budget.acquire

    async def traced_connect(client):
        start = loop.time()
        try:
            return await connect(client)
        finally:
            traces.append(dict(kind='connect', start=start, end=loop.time()))

    async def traced_read(reader, block, *args):
        start = loop.time()
        try:
            return await read(reader, block, *args)
        finally:
            traces.append(dict(kind='block', unit=reader.connection.unit_id, area=block.area,
                               address=block.address, start=start, end=loop.time()))

    @asynccontextmanager
    async def traced_budget(endpoint, deadline):
        start = loop.time()
        async with acquire(endpoint, deadline) as admitted:
            traces.append(dict(kind='admission', start=start, end=loop.time(), admitted=admitted))
            yield admitted

    runtime.read_budget.acquire = traced_budget
    start = loop.time()
    try:
        with patch.object(AsyncModbusTcpClient, 'connect', traced_connect), patch.object(ModbusReader, 'read_block', traced_read):
            for runner in runtime.runners.values():
                await runner.start()
            await asyncio.sleep(seconds)
            for runner in runtime.runners.values():
                await runner.stop()
    finally:
        for runner in runtime.runners.values():
            await runner.stop()
        server.close()
        await server.wait_closed()
        for task in list(handlers):
            task.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)
    events = []
    while not samples._queue.empty():
        events.append(samples.get_nowait())
    origins = {(s.device_id, s.point_id): s.completed_mono_ms for s in events
               if s.kind == 'QUALITY_STATE' and s.quality == 'UNKNOWN'}
    slots, gaps = {}, Counter()
    for s in events:
        if s.kind != 'PHYSICAL_RESULT':
            continue
        key = s.device_id, s.point_id
        slot = round((s.nominal_poll_due_mono_ms - origins[key]) / 5000)
        assert slot not in slots.setdefault(key, set()), 'Duplicate physical slot'
        slots[key].add(slot)
    for key, seen in slots.items():
        for slot in range(max(seen) + 1):
            if slot not in seen:
                gaps[slot] += 1
    connects = [t for t in traces if t['kind'] == 'connect']
    return dict(scope='ISOLATED_REAL_TCP_50_UNIT_STARTUP', durationSeconds=loop.time()-start,
                configSha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
                devices=50, points=250, pollIntervalMs=5000, globalReadCapacity=16,
                endpointReadConcurrency=1, acceptedConnections=len(accepted), connectCalls=len(connects),
                connectDurationSumMs=sum((t['end']-t['start'])*1000 for t in connects),
                missedPollCycles=samples.missed_poll_cycles, missingPhysicalSlots=dict(gaps),
                pointsWithPhysicalResults=len(slots),
                goodLatest=sum(s.quality == 'GOOD' for r in runtime.runners.values() for s in r.latest.values()),
                captured=len(events), physical=sum(s.kind == 'PHYSICAL_RESULT' for s in events),
                activeConnectionsAfterStop=len(connections), sourceDrops=samples.dropped,
                minimumLastSlot=min(map(max, slots.values()), default=-1),
                physicalDevicesAccessed=False, databaseCreated=False, brokerConnected=False,
                traces=traces)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seconds', type=float, default=12)
    args = parser.parse_args()
    if not 11 <= args.seconds <= 30:
        parser.error('Use an 11..30 second bounded window')
    result = asyncio.run(verify(args.config, args.seconds))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: v for k, v in result.items() if k != 'traces'}, ensure_ascii=False))
