"""Publish an explicit full config snapshot and wait for the matching application ACK."""
import argparse
import asyncio
import json
import math
from pathlib import Path
import uuid

from tools import _source_path  # noqa: F401
from plcnext_iot.contracts import validate_message
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.messaging.transport import PahoConnection


async def send(settings, gateway, payload, timeout=30):
    if validate_message('configSet',payload,expected_gateway_id=gateway):
        raise ValueError('Invalid configuration')
    request = json.loads(payload)
    prefix = 'iot/v1/gateway/' + gateway + '/'
    connection = PahoConnection(settings,'config-tool-' + uuid.uuid4().hex,[prefix+'config/ack'])
    try:
        async with asyncio.timeout(timeout):
            await connection.open()
            while True:
                await connection.publish(prefix+'config/set',payload,retain=True)
                try:
                    async with asyncio.timeout(5):
                        while True:
                            message = await connection.receive()
                            if validate_message('configAck',message.payload,expected_gateway_id=gateway):
                                continue
                            ack = json.loads(message.payload)
                            if ack['messageId']==request['messageId'] and ack['configVersion']==request['configVersion']:
                                return ack
                except TimeoutError:
                    continue  # Same serialized payload and identity on every retry.
    finally:
        await connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mqtt',type=Path,required=True)
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--gateway',required=True)
    parser.add_argument('--timeout',type=float,default=30)
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or not 0 < args.timeout <= 300:
        parser.error('Timeout must be in (0,300]')
    try:
        settings = MqttSettings.from_file(args.mqtt)
        with args.config.open('rb') as stream:
            payload = stream.read(2*1024*1024+1)
        result = asyncio.run(send(settings,args.gateway,payload,args.timeout))
    except ValueError:
        print(json.dumps({'result':'INVALID_INPUT'}))
        return 2
    except (OSError,TimeoutError,ConnectionError):
        print(json.dumps({'result':'ACK_NOT_CONFIRMED'}))
        return 1
    print(json.dumps(result))
    return 0 if result['status']=='APPLIED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
