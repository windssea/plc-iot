"""Standalone durable MQTT receiver for integration; commit before data/ack=STORED."""
import argparse
import asyncio
import json
import math
from pathlib import Path
import signal
import time
import uuid

from tools import _source_path  # noqa: F401
from plcnext_iot.core.storage_worker import StoreWorker
from plcnext_iot.messaging.transport import PahoConnection
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.storage.receiver import ReceiverStore
from plcnext_iot.config.store import StoreError


async def run(settings,path,gateway,configs,duration):
    worker=StoreWorker(path,gateway,ReceiverStore)
    stop=asyncio.Event()
    loop=asyncio.get_running_loop()
    previous={sig:signal.signal(sig,lambda *_: loop.call_soon_threadsafe(stop.set)) for sig in (signal.SIGINT,signal.SIGTERM)}
    prefix='iot/v1/gateway/'+gateway+'/'
    deadline=loop.time()+duration if duration is not None else float('inf')
    try:
        await worker.open()
        for file in configs:
            with file.open('rb') as stream:
                payload=stream.read(2*1024*1024+1)
            if not await worker.call('install_config',payload):
                raise ValueError('Configuration history capacity reached')
        while not stop.is_set() and loop.time()<deadline:
            connection=PahoConnection(settings,'receiver-'+uuid.uuid4().hex,[prefix+'config/set',prefix+'data'])
            try:
                await connection.open()
                print(json.dumps({'event':'receiver_ready','gatewayId':gateway}),flush=True)
                while not stop.is_set() and loop.time()<deadline:
                    try:
                        incoming=await asyncio.wait_for(connection.receive(),0.2)
                    except TimeoutError:
                        continue
                    if incoming.topic==prefix+'config/set':
                        try:
                            installed=await worker.call('install_config',incoming.payload)
                        except ValueError:
                            installed=False
                        print(json.dumps({'event':'config_history','accepted':installed}),flush=True)
                    else:
                        ack=await worker.call('receive',incoming.payload,time.time_ns()//1_000_000)
                        if ack is not None:
                            await connection.publish(prefix+'data/ack',ack)
                            print(json.dumps({'event':'data_ack','ack':json.loads(ack),'stats':await worker.call('stats')}),flush=True)
            except (OSError,ConnectionError,TimeoutError):
                if not stop.is_set():
                    try:
                        await asyncio.wait_for(stop.wait(),min(1,max(0,deadline-loop.time())))
                    except TimeoutError:
                        pass
            finally:
                await connection.close()
    finally:
        await worker.close()
        for sig,handler in previous.items():
            signal.signal(sig,handler)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mqtt',type=Path,required=True)
    parser.add_argument('--database',type=Path,required=True)
    parser.add_argument('--gateway',required=True)
    parser.add_argument('--config',type=Path,action='append',default=[])
    parser.add_argument('--run-seconds',type=float)
    args=parser.parse_args()
    if args.run_seconds is not None and (not math.isfinite(args.run_seconds) or args.run_seconds<0):
        parser.error('Duration must be finite and nonnegative')
    try:
        settings=MqttSettings.from_file(args.mqtt)
        asyncio.run(run(settings,args.database,args.gateway,args.config,args.run_seconds))
    except (OSError,ValueError,StoreError):
        print(json.dumps({'event':'receiver_failed','code':'RECEIVER_INPUT_OR_STORAGE_ERROR'}))
        return 1
    return 0


if __name__=='__main__':
    raise SystemExit(main())
