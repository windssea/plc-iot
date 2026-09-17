"""Cross-project TCP acceptance using PLCnext-mock-device in an isolated process."""
import argparse
import asyncio
import json
from pathlib import Path
import socket
import tempfile

from tools import _source_path  # noqa: F401
from plcnext_iot.core.lifecycle import AgentLifecycle
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.devices.modbus import ModbusRunner
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.points.samples import SampleBuffer

ROOT = Path(__file__).resolve().parents[1]


def fixtures(port):
    # Independent expected engineering values, with multiple Unit IDs on one port.
    definitions = [('meter-A', 1, 'pressure', 'input_register', 'float32', 0, 12.5),
                   ('meter-A', 1, 'voltage', 'holding_register', 'uint16', 4, 2300),
                   ('meter-B', 2, 'running', 'coil', 'bool', 17, True),
                   ('meter-B', 2, 'alarm', 'discrete_input', 'bool', 18, False)]
    devices, children, values = {}, {}, []
    for device_id, unit, point_id, area, kind, address, value in definitions:
        devices.setdefault(device_id, {'id':device_id, 'name':device_id, 'protocol':'modbus',
            'protocol_config':{'unit_id':unit}, 'signals':[]})
        devices[device_id]['signals'].append({'id':point_id, 'name':point_id, 'value_type':kind,
            'generator':{'type':'random_range', 'min':0, 'max':1 if kind == 'bool' else 5000},
            'binding':{'type':'modbus', 'table':area, 'address':address, 'wire_type':kind}})
        values.append({'device_id':device_id, 'signal_id':point_id, 'value':value})
        children.setdefault(device_id, {'deviceId':device_id, 'name':device_id, 'enabled':True,
            'protocol':'modbus_tcp', 'connection':{'host':'127.0.0.1','port':port,'unitId':unit,
                'connectTimeoutMs':500,'requestTimeoutMs':200,'retryCount':0}, 'points':[]})
        modbus = {'area':area, 'address':address}
        if kind != 'bool':
            modbus['byteOrder'] = 'ABCD' if kind == 'float32' else 'AB'
        children[device_id]['points'].append({'pointId':point_id,'name':point_id,'enabled':True,
            'dataType':kind,'pollIntervalMs':100,'scale':0.1 if point_id == 'voltage' else 1,
            'offset':0,'reportMode':'cyclic','reportIntervalMs':1000,'deadband':0,
            'staleAfterMs':1000,'modbus':modbus})
    mock = {'version':1,'opcua':{'enabled':False},'mqtt':{'enabled':False},
            'modbus':{'host':'127.0.0.1','port':port},'devices':list(devices.values())}
    config = {'schemaVersion':1,'gatewayId':'PLC-01','messageId':'mock-10','configVersion':10,
              'timestamp':1788825600000,'config':{'enabled':True,
                  'report':{'batchIntervalMs':1000,'maxBatchPoints':500},'devices':list(children.values())}}
    return mock, config, values


async def smoke(project, python):
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    mock, config, values = fixtures(port)
    with tempfile.TemporaryDirectory(prefix='plcnext-iot-smoke-') as directory:
        with (Path(directory) / 'mock-stderr.log').open('wb') as errors:
            process = await asyncio.create_subprocess_exec(str(python), str(ROOT / 'tools/mock_device_server.py'),
                str(project), stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=errors)
            samples = SampleBuffer(64)
            runtime = DeviceRuntime(lambda device: ModbusRunner(device, runtime, samples))
            agent = AgentLifecycle(AgentSettings('PLC-01', Path(directory) / 'agent'), runtime)
            async def command(action, **fields):
                process.stdin.write((json.dumps({'action':action, **fields}) + '\n').encode())
                await process.stdin.drain()
                line = await asyncio.wait_for(process.stdout.readline(), 10)
                if not line or not json.loads(line).get('ok'):
                    raise RuntimeError('Simulator command failed: ' + action)

            async def collect(predicate, count=1):
                found = {}
                async with asyncio.timeout(10):
                    while len(found) < count:
                        sample = await samples.get()
                        if predicate(sample):
                            found[(sample.device_id, sample.point_id)] = sample
                return found

            try:
                await command('start', config=mock, values=values)
                await agent.start()
                result = await agent.submit(json.dumps(config).encode())
                if result.status != 'APPLIED':
                    raise RuntimeError('Configuration rejected: ' + str(result.code))
                good = await collect(lambda s: s.quality == 'GOOD' and s.config_version == 10, 4)
                actual = {key: sample.value for key, sample in good.items()}
                expected = {('meter-A','pressure'):12.5, ('meter-A','voltage'):230,
                            ('meter-B','running'):True, ('meter-B','alarm'):False}
                if actual != expected:
                    raise AssertionError((actual, expected))
                await command('stop')
                await collect(lambda s: s.quality in ('BAD_CONNECTION','BAD_TIMEOUT'))
                values[0]['value'] = 18.75
                await command('start', values=values)
                # start replays initial values; update explicitly publishes the changed signal.
                await command('update', values=values)
                await collect(lambda s: s.point_id == 'pressure' and s.quality == 'GOOD' and s.value == 18.75)
                sibling = runtime.runners['meter-B']
                config['messageId'], config['configVersion'] = 'mock-11', 11
                config['config']['devices'][0]['points'][1]['scale'] = 0.01
                result = await agent.submit(json.dumps(config).encode())
                if result.status != 'APPLIED' or runtime.runners['meter-B'] is not sibling:
                    raise AssertionError('Device switch did not preserve unchanged sibling')
                await collect(lambda s: s.config_version == 11 and s.point_id == 'voltage' and s.value == 23)
                await agent.close()
                if runtime.runners:
                    raise AssertionError('Runners remain after shutdown')
                print(json.dumps({'result':'PASS','mockProject':str(project), 'devices':2,'points':4,
                    'checks':['four_read_tables','unit_ids','decode_and_scale','disconnect_recovery',
                              'config_switch','unchanged_sibling','shutdown'], 'mqttConnected':False}))
            except Exception:
                errors.flush()
                diagnostic = (Path(directory) / 'mock-stderr.log').read_text(errors='replace')[-4000:]
                if diagnostic:
                    print(diagnostic)
                raise
            finally:
                await agent.close()
                if process.returncode is None:
                    try:
                        process.stdin.write(b'{"action":"exit"}\n')
                        await process.stdin.drain()
                        await asyncio.wait_for(process.wait(), 5)
                    except (TimeoutError, BrokenPipeError, ConnectionError):
                        process.kill()
                        await process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mock-project', type=Path, required=True)
    parser.add_argument('--mock-python', type=Path)
    args = parser.parse_args()
    project = args.mock_project.resolve()
    python = args.mock_python or project / '.venv/Scripts/python.exe'
    if not python.is_file():
        python = project / '.venv/bin/python' if args.mock_python is None else python
    if not python.is_file() or not (project / 'iot_sim/adapters/modbus.py').is_file():
        parser.error('Mock project or its Python environment is unavailable')
    asyncio.run(smoke(project, python.resolve()))


if __name__ == '__main__':
    main()
