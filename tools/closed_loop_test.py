"""Closed-loop acceptance: local MQTT 1883, mock 2xModbus+2xOPC UA, config/data/status."""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import tempfile
import time
import urllib.error
import urllib.request
import uuid

from tools import _source_path  # noqa: F401
from tools.agent import run as run_agent
from tools.mqtt_smoke import docker
from plcnext_iot.contracts import validate_message
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.messaging.messages import encode
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.messaging.transport import PahoConnection

ROOT = Path(__file__).resolve().parents[1]
MOCK_BOOTSTRAP = ROOT / 'deploy/closed-loop/mock-bootstrap.yaml'
MOSQUITTO = ROOT / 'deploy/closed-loop/mosquitto.conf'
GATEWAY = 'PLC-01'
PREFIX = f'iot/v1/gateway/{GATEWAY}/'
BROKER_HOST = '127.0.0.1'
BROKER_PORT = 1883
BROKER_NAME = 'plcnext-iot-closed-loop-mqtt'
MOCK_WEB = 18080
MODBUS_PORT = 11502
OPCUA_PORT = 14840
POINTS = [
    ('meter-A', 'voltage'), ('meter-A', 'current'),
    ('meter-B', 'pressure'), ('meter-B', 'running'),
    ('tank-A', 'level'),
    ('tank-B', 'temperature'), ('tank-B', 'alarm'),
]


def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def snapshot(version, devices, enabled=True, message=None):
    return {
        'schemaVersion': 1, 'gatewayId': GATEWAY, 'timestamp': time.time_ns() // 1_000_000,
        'messageId': message or f'cfg-v{version}', 'configVersion': version,
        'config': {'enabled': enabled, 'report': {'batchIntervalMs': 500, 'maxBatchPoints': 500},
                   'devices': devices},
    }


def point(point_id, name, data_type, poll, extra, unit=None, report_mode='cyclic'):
    item = {
        'pointId': point_id, 'name': name, 'enabled': True, 'dataType': data_type,
        'pollIntervalMs': poll, 'scale': extra.pop('scale', 1), 'offset': 0,
        'reportMode': report_mode, 'reportIntervalMs': 1000, 'deadband': 0 if data_type == 'bool' else 0,
        'staleAfterMs': 5000, **extra,
    }
    if unit:
        item['unit'] = unit
    if data_type == 'bool':
        item.update(scale=1, offset=0, deadband=0)
    return item


def modbus_device(device_id, name, unit, points):
    return {
        'deviceId': device_id, 'name': name, 'enabled': True, 'protocol': 'modbus_tcp',
        'connection': {'host': '127.0.0.1', 'port': MODBUS_PORT, 'unitId': unit,
                       'connectTimeoutMs': 3000, 'requestTimeoutMs': 1000, 'retryCount': 1},
        'points': points,
    }


def opcua_device(device_id, name, points):
    return {
        'deviceId': device_id, 'name': name, 'enabled': True, 'protocol': 'opcua',
        'connection': {'host': '127.0.0.1', 'port': OPCUA_PORT, 'path': '/iot-simulator/',
                       'securityPolicy': 'None', 'securityMode': 'None',
                       'connectTimeoutMs': 5000, 'requestTimeoutMs': 2000, 'retryCount': 1},
        'points': points,
    }


def devices_v10(voltage_scale=1, tank_a_poll=1000, tank_b_enabled=True, meter_b_port=MODBUS_PORT,
                ghost_node=False, bad_address=False, tank_user=None, tank_password_env=None,
                report_mode='cyclic', polls=None):
    polls = polls or {}
    def poll_of(key, default):
        return polls.get(key, default)
    meter_a_points = [
        point('voltage', 'Voltage', 'uint16', poll_of('voltage', 1000), {
            'scale': voltage_scale,
            'modbus': {'area': 'holding_register', 'address': 0, 'byteOrder': 'AB'},
        }, 'V', report_mode),
        point('current', 'Current', 'float32', poll_of('current', 1000), {
            'modbus': {'area': 'holding_register', 'address': 2, 'byteOrder': 'ABCD'},
        }, 'A', report_mode),
    ]
    if bad_address:
        meter_a_points.append(point('ghost-reg', 'Ghost Register', 'uint16', 1000, {
            'modbus': {'area': 'holding_register', 'address': 200, 'byteOrder': 'AB'},
        }, report_mode=report_mode))
    meter_a = modbus_device('meter-A', 'Meter A', 1, meter_a_points)
    meter_b = modbus_device('meter-B', 'Meter B', 2, [
        point('pressure', 'Pressure', 'float32', poll_of('pressure', 1000), {
            'modbus': {'area': 'input_register', 'address': 0, 'byteOrder': 'ABCD'},
        }, 'kPa', report_mode),
        point('running', 'Running', 'bool', poll_of('running', 1000), {
            'modbus': {'area': 'coil', 'address': 0},
        }, report_mode=report_mode),
    ])
    meter_b['connection']['port'] = meter_b_port
    tank_a_points = [
        point('level', 'Level', 'float32', poll_of('level', tank_a_poll),
              {'opcua': {'nodeId': 'ns=2;s=tank-A.level'}}, 'm', report_mode),
    ]
    if ghost_node:
        tank_a_points.append(point('ghost', 'Ghost', 'float32', 1000,
                                   {'opcua': {'nodeId': 'ns=2;s=tank-A.missing'}}, report_mode=report_mode))
    tank_a = opcua_device('tank-A', 'Tank A', tank_a_points)
    if tank_user:
        tank_a['connection']['username'] = tank_user
        tank_a['connection']['passwordEnv'] = tank_password_env or 'IOT_OPCUA_PASSWORD_MISSING'
    tank_b = opcua_device('tank-B', 'Tank B', [
        point('temperature', 'Temperature', 'float32', poll_of('temperature', 1000),
              {'opcua': {'nodeId': 'ns=2;s=tank-B.temperature'}}, 'C', report_mode),
        point('alarm', 'Alarm', 'bool', poll_of('alarm', 1000),
              {'opcua': {'nodeId': 'ns=2;s=tank-B.alarm'}}, report_mode=report_mode),
    ])
    tank_b['enabled'] = tank_b_enabled
    return [meter_a, meter_b, tank_a, tank_b]


def mock_devices():
    def signal(sig_id, name, value_type, generator, binding, unit=None):
        item = {'id': sig_id, 'name': name, 'value_type': value_type, 'interval_ms': 1000,
                'generator': generator, 'binding': binding}
        if unit:
            item['unit'] = unit
        return item
    return [
        {'id': 'meter-A', 'name': 'Meter A', 'enabled': True, 'desired_state': 'running',
         'default_interval_ms': 1000, 'protocol': 'modbus', 'protocol_config': {'unit_id': 1},
         'signals': [
             signal('voltage', 'Voltage', 'uint16',
                    {'type': 'increment', 'initial_value': 230, 'value_min': 220, 'value_max': 240,
                     'step_min': 1, 'step_max': 1, 'boundary': 'wrap'},
                    {'type': 'modbus', 'table': 'holding_register', 'address': 0, 'wire_type': 'uint16'},
                    'V'),
             signal('current', 'Current', 'float32',
                    {'type': 'increment', 'initial_value': 5.0, 'value_min': 1.0, 'value_max': 10.0,
                     'step_min': 0.1, 'step_max': 0.1, 'boundary': 'wrap'},
                    {'type': 'modbus', 'table': 'holding_register', 'address': 2, 'wire_type': 'float32'},
                    'A'),
         ]},
        {'id': 'meter-B', 'name': 'Meter B', 'enabled': True, 'desired_state': 'running',
         'default_interval_ms': 1000, 'protocol': 'modbus', 'protocol_config': {'unit_id': 2},
         'signals': [
             signal('pressure', 'Pressure', 'float32',
                    {'type': 'increment', 'initial_value': 12.5, 'value_min': 0, 'value_max': 100,
                     'step_min': 0.5, 'step_max': 0.5, 'boundary': 'wrap'},
                    {'type': 'modbus', 'table': 'input_register', 'address': 0, 'wire_type': 'float32'},
                    'kPa'),
             signal('running', 'Running', 'bool',
                    {'type': 'random_range', 'min': 1, 'max': 1, 'precision': 0},
                    {'type': 'modbus', 'table': 'coil', 'address': 0, 'wire_type': 'bool'}),
         ]},
        {'id': 'tank-A', 'name': 'Tank A', 'enabled': True, 'desired_state': 'running',
         'default_interval_ms': 1000, 'protocol': 'opcua', 'protocol_config': {},
         'signals': [
             signal('level', 'Level', 'float32',
                    {'type': 'increment', 'initial_value': 2.5, 'value_min': 0, 'value_max': 5,
                     'step_min': 0.1, 'step_max': 0.1, 'boundary': 'wrap'},
                    {'type': 'opcua', 'node_id': 'tank-A.level', 'browse_name': 'level',
                     'data_type': 'float32'}, 'm'),
         ]},
        {'id': 'tank-B', 'name': 'Tank B', 'enabled': True, 'desired_state': 'running',
         'default_interval_ms': 1000, 'protocol': 'opcua', 'protocol_config': {},
         'signals': [
             signal('temperature', 'Temperature', 'float32',
                    {'type': 'increment', 'initial_value': 20.0, 'value_min': 18, 'value_max': 35,
                     'step_min': 0.2, 'step_max': 0.2, 'boundary': 'wrap'},
                    {'type': 'opcua', 'node_id': 'tank-B.temperature', 'browse_name': 'temperature',
                     'data_type': 'float32'}, 'C'),
             signal('alarm', 'Alarm', 'bool',
                    {'type': 'random_range', 'min': 0, 'max': 0, 'precision': 0},
                    {'type': 'opcua', 'node_id': 'tank-B.alarm', 'browse_name': 'alarm',
                     'data_type': 'bool'}),
         ]},
    ]


async def wait_tcp(host, port, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return True
        except OSError:
            await asyncio.sleep(0.2)
    return False


def http_json(method, url, body=None, timeout=15):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            parsed = json.loads(raw) if raw else None
        except ValueError:
            parsed = raw.decode('utf-8', 'replace')
        return exc.code, parsed


class Recorder:
    def __init__(self):
        self.cases = []

    def add(self, case_id, name, result, expected, actual, evidence=None, notes=''):
        self.cases.append({
            'id': case_id, 'name': name, 'result': result, 'expected': expected,
            'actual': actual, 'evidence': evidence or {}, 'notes': notes, 'at': utc_now(),
        })
        print(json.dumps({'event': 'case', 'id': case_id, 'result': result, 'name': name}), flush=True)

    def summary(self):
        counts = {'PASS': 0, 'FAIL': 0, 'BLOCKED': 0, 'SKIPPED': 0}
        for case in self.cases:
            counts[case['result']] = counts.get(case['result'], 0) + 1
        return counts


class Bus:
    def __init__(self, settings):
        self.settings = settings
        self.acks, self.data, self.heartbeats, self.device_status, self.status, self.gets = [], [], [], [], [], []
        self.points = {}
        self.devices = {}
        self.stored = 0
        self.ack_enabled = True
        self.held_data = []
        self._task = None
        self._acks_out = []
        self._connection = None
        self._topics = [PREFIX + name for name in
                        ('config/ack', 'config/get', 'status', 'heartbeat', 'device/status', 'data')]

    async def start(self):
        await self._ensure_connection()
        self._task = asyncio.create_task(self._run(), name='closed-loop-bus')

    async def _ensure_connection(self):
        while True:
            if self._connection is not None:
                try:
                    await self._connection.close()
                except Exception:
                    pass
                self._connection = None
            try:
                self._connection = PahoConnection(
                    self.settings, 'closed-loop-' + uuid.uuid4().hex, self._topics)
                await self._connection.open()
                return
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(1)

    async def close(self):
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._connection is not None:
            await self._connection.close()

    async def publish_config(self, payload):
        last = None
        for _ in range(25):
            try:
                await self._connection.publish(PREFIX + 'config/set', payload, retain=True)
                return
            except (ConnectionError, OSError, TimeoutError, AttributeError) as exc:
                last = exc
                await asyncio.sleep(0.4)
        raise ConnectionError(str(last) if last else 'MQTT publish failed')

    async def _run(self):
        while True:
            try:
                incoming = await self._connection.receive()
            except asyncio.CancelledError:
                raise
            except (ConnectionError, OSError, TimeoutError):
                await self._ensure_connection()
                continue
            kind = incoming.topic.rsplit('/', 1)[-1]
            if incoming.topic.endswith('device/status'):
                kind = 'device/status'
            if incoming.topic.endswith('config/ack'):
                kind = 'config/ack'
            if incoming.topic.endswith('config/get'):
                kind = 'config/get'
            try:
                message = json.loads(incoming.payload)
            except ValueError:
                continue
            if kind == 'config/ack':
                self.acks.append(message)
            elif kind == 'config/get':
                self.gets.append(message)
            elif kind == 'status':
                self.status.append(message)
            elif kind == 'heartbeat':
                self.heartbeats.append(message)
            elif kind == 'device/status':
                self.device_status.append(message)
                device = message.get('device') or {}
                if 'deviceId' in device:
                    self.devices[device['deviceId']] = device.get('status')
            elif kind == 'data':
                self.data.append(message)
                for item in message.get('values', []):
                    self.points[(item.get('deviceId'), item.get('pointId'))] = item
                if not validate_message('data', incoming.payload, expected_gateway_id=GATEWAY):
                    if self.ack_enabled:
                        await self._ack_data(message)
                    else:
                        self.held_data.append(message)

    async def _ack_data(self, message):
        ack = encode({
            'schemaVersion': 1, 'gatewayId': GATEWAY,
            'timestamp': time.time_ns() // 1_000_000,
            'messageId': message.get('messageId', 'unknown'),
            'status': 'STORED', 'errors': [],
        })
        if validate_message('dataAck', ack, expected_gateway_id=GATEWAY):
            return
        task = asyncio.create_task(self._connection.publish(PREFIX + 'data/ack', ack))
        self._acks_out.append(task)
        self.stored += 1

    async def flush_held(self):
        held, self.held_data = self.held_data, []
        for message in held:
            await self._ack_data(message)
        return len(held)

    async def wait_ack(self, version, timeout=30, after=0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for ack in reversed(self.acks[after:]):
                if ack.get('configVersion') == version:
                    return ack
            await asyncio.sleep(0.2)
        return None

    async def wait_for(self, predicate, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.2)
        return False


def markdown_report(recorder, extra):
    counts = recorder.summary()
    total = len(recorder.cases)
    lines = [
        '# 闭环联调测试报告',
        '',
        f'日期：{utc_now()}  ',
        '对象：PLCnext-iot Agent 0.2.0 + 本地 MQTT 1883 + PLCnext-mock-device  ',
        '方案：[闭环联调测试方案](闭环联调测试方案.md)',
        '',
        '## 1. 结论',
        '',
        f"共 {total} 条用例，PASS {counts.get('PASS', 0)}，FAIL {counts.get('FAIL', 0)}，"
        f"BLOCKED {counts.get('BLOCKED', 0)}，SKIPPED {counts.get('SKIPPED', 0)}。",
        '',
    ]
    if extra.get('environment'):
        lines += ['## 2. 环境', '', '| 项 | 值 |', '|---|---|']
        for key, value in extra['environment'].items():
            lines.append(f'| {key} | {value} |')
        lines.append('')
    lines += ['## 3. 用例结果', '', '| 编号 | 场景 | 结果 | 期望 | 实际 | 备注 |',
              '|---|---|---|---|---|---|']
    for case in recorder.cases:
        actual = str(case['actual']).replace('|', '\\|')[:120]
        expected = str(case['expected']).replace('|', '\\|')[:80]
        notes = str(case.get('notes') or '').replace('|', '\\|')[:80]
        lines.append(f"| {case['id']} | {case['name']} | **{case['result']}** | {expected} | {actual} | {notes} |")
    lines += ['', '## 4. 问题', '']
    failed = [c for c in recorder.cases if c['result'] != 'PASS']
    if not failed:
        lines.append('本次未发现阻塞问题。')
    else:
        for case in failed:
            lines += [f"### {case['id']} {case['name']}", '',
                      f"- 结果：{case['result']}",
                      f"- 期望：{case['expected']}",
                      f"- 实际：{case['actual']}",
                      f"- 证据：`{json.dumps(case.get('evidence') or {}, ensure_ascii=False)[:500]}`",
                      '']
    if extra.get('samples'):
        lines += ['## 5. 采集样本摘要', '', '| 设备 | 点位 | 质量 | 值 | 配置版本 |', '|---|---|---|---|---|']
        for (device, point_id), item in extra['samples']:
            lines.append(f"| {device} | {point_id} | {item.get('quality')} | {item.get('value')} | {item.get('configVersion', '')} |")
        lines.append('')
    lines += ['## 6. 说明', '',
              '本报告由 `python -m tools.closed_loop_test` 根据实测自动生成。',
              '本轮使用本机 1883 匿名明文 Broker。', '']
    return '\n'.join(lines)


async def mock_put_devices(url, devices, opcua_enabled=True, modbus_enabled=True):
    status, current = await asyncio.to_thread(http_json, 'GET', url + '/api/v1/config')
    if status != 200:
        raise RuntimeError(f'GET mock config failed: {status} {current}')
    body = dict(current)
    body['devices'] = devices
    body['opcua'] = dict(current['opcua'], enabled=opcua_enabled, host='127.0.0.1', port=OPCUA_PORT)
    body['modbus'] = dict(current['modbus'], enabled=modbus_enabled, host='127.0.0.1', port=MODBUS_PORT)
    body['mqtt'] = dict(current['mqtt'], enabled=False)
    body['web'] = dict(current['web'], host='127.0.0.1', port=MOCK_WEB)
    status, result = await asyncio.to_thread(http_json, 'PUT', url + '/api/v1/config', body)
    if status not in (200, 202):
        raise RuntimeError(f'PUT mock config failed: {status} {result}')
    return result


async def run_cases(mock_project, mock_python, report_path, results_path):
    recorder = Recorder()
    extra = {'environment': {
        'mqtt': f'{BROKER_HOST}:{BROKER_PORT} anonymous tls=false',
        'mock': str(mock_project),
        'modbus': f'127.0.0.1:{MODBUS_PORT}',
        'opcua': f'opc.tcp://127.0.0.1:{OPCUA_PORT}/iot-simulator/',
        'gateway': GATEWAY,
    }}
    mock_proc = None
    agent_task = None
    stop = asyncio.Event()
    bus = None
    started_broker = False
    work = tempfile.TemporaryDirectory(prefix='plcnext-iot-closed-loop-', ignore_cleanup_errors=True)
    mock_err = None
    work_path = Path(work.name)
    mock_db = work_path / 'mock.db'
    try:
        mqtt_up = False
        mqtt_note = ''
        probe = PahoConnection(MqttSettings(BROKER_HOST, BROKER_PORT, False),
                               'closed-loop-probe-' + uuid.uuid4().hex, [])
        try:
            await asyncio.wait_for(probe.open(), 5)
            mqtt_up = True
            mqtt_note = f'{BROKER_HOST}:{BROKER_PORT} anonymous CONNECT Success'
        except Exception as exc:
            mqtt_note = f'{BROKER_HOST}:{BROKER_PORT} connect failed: {exc}'
        finally:
            await probe.close()
        if not mqtt_up:
            try:
                await docker('rm', '-f', BROKER_NAME)
            except Exception:
                pass
            await docker(
                'run', '-d', '--name', BROKER_NAME,
                '-p', f'127.0.0.1:{BROKER_PORT}:1883',
                '--mount', f'type=bind,source={MOSQUITTO},target=/mosquitto/config/mosquitto.conf,readonly',
                'eclipse-mosquitto:2.0.22')
            started_broker = True
            mqtt_up = await wait_tcp(BROKER_HOST, BROKER_PORT, 30)
            mqtt_note = ('started anonymous mosquitto on 1883; tcp='
                         + ('open' if mqtt_up else 'closed'))
        extra['environment']['mqtt'] = mqtt_note
        extra['environment']['brokerOwned'] = started_broker
        recorder.add('T01', 'MQTT 就绪', 'PASS' if mqtt_up else 'FAIL',
                     '本机 1883 匿名明文可连接', mqtt_note)
        if not mqtt_up:
            extra['report_notes'] = '1883 Broker 不可用，后续全部 BLOCKED'
            return recorder, extra

        mock_err = (work_path / 'mock-stderr.log').open('wb')
        mock_proc = await asyncio.create_subprocess_exec(
            str(mock_python), '-m', 'iot_sim.main', 'serve',
            '--config', str(MOCK_BOOTSTRAP), '--database', str(mock_db),
            cwd=str(mock_project),
            env={**os.environ, 'PYTHONPATH': str(mock_project),
                 'IOT_SIM_WEB_PORT': str(MOCK_WEB), 'IOT_SIM_WEB_HOST': '127.0.0.1'},
            stdout=asyncio.subprocess.DEVNULL, stderr=mock_err,
        )
        mock_url = f'http://127.0.0.1:{MOCK_WEB}'
        api_up = False
        for _ in range(80):
            try:
                code, _ = await asyncio.to_thread(http_json, 'GET', mock_url + '/api/v1/status')
                if code == 200:
                    api_up = True
                    break
            except Exception:
                pass
            if mock_proc.returncode is not None:
                break
            await asyncio.sleep(0.25)
        if api_up:
            await mock_put_devices(mock_url, mock_devices())
            await asyncio.sleep(1.5)
            modbus_up = await wait_tcp('127.0.0.1', MODBUS_PORT, 15)
            opcua_up = await wait_tcp('127.0.0.1', OPCUA_PORT, 15)
            ok = modbus_up and opcua_up
            recorder.add('T02', '模拟器就绪', 'PASS' if ok else 'FAIL',
                         '四台设备写入且 Modbus/OPC UA 端口监听',
                         f'api={api_up} modbus={modbus_up} opcua={opcua_up}')
        else:
            err = b''
            if mock_proc.stderr:
                err = await asyncio.wait_for(mock_proc.stderr.read(4000), 2)
            recorder.add('T02', '模拟器就绪', 'FAIL', 'Web API 可用',
                         f'proc={mock_proc.returncode} {err[-500:]!r}')
            return recorder, extra

        mqtt_settings = MqttSettings(BROKER_HOST, BROKER_PORT, False)
        (work_path / 'agent').mkdir()
        agent_settings = AgentSettings(GATEWAY, work_path / 'agent', 30, 8)
        bus = Bus(mqtt_settings)
        await bus.start()
        await bus.publish_config(b'')
        await asyncio.sleep(0.4)
        agent_task = asyncio.create_task(
            run_agent(agent_settings, [], None, 'all', mqtt_settings, True, stop, 'boot-closed-loop'),
            name='closed-loop-agent')
        started = await bus.wait_for(lambda: any(s.get('online') is True for s in bus.status)
                                     and bool(bus.gets), 30)
        recorder.add('T03', 'Agent 启动', 'PASS' if started else 'FAIL',
                     'status online 且发出 config/get',
                     f'online={any(s.get("online") is True for s in bus.status)} gets={len(bus.gets)}')
        if not started:
            return recorder, extra

        async def send(version, devices, enabled=True, message=None):
            payload = json.dumps(snapshot(version, devices, enabled, message)).encode()
            issues = validate_message('configSet', payload, expected_gateway_id=GATEWAY)
            if issues:
                return {'status': 'INVALID', 'issues': [issue.code for issue in issues]}
            before = len(bus.acks)
            await bus.publish_config(payload)
            return await bus.wait_ack(version, 30, after=before)

        ack = await send(10, devices_v10())
        applied = ack and ack.get('status') == 'APPLIED' and ack.get('activeConfigVersion') == 10
        recorder.add('T04', '首次下发', 'PASS' if applied else 'FAIL',
                     'APPLIED v10', ack, evidence={'ack': ack})
        if not applied:
            return recorder, extra

        def good_points():
            return {(d, p) for d, p in POINTS
                    if bus.points.get((d, p), {}).get('quality') == 'GOOD'}

        collected = await bus.wait_for(lambda: good_points() == set(POINTS), 40)
        samples = {key: bus.points.get(key) for key in POINTS}
        voltage = samples.get(('meter-A', 'voltage'), {})
        alarm = samples.get(('tank-B', 'alarm'), {})
        values_ok = (isinstance(voltage.get('value'), (int, float)) and 220 <= voltage.get('value') <= 240
                     and alarm.get('value') is False
                     and samples.get(('meter-B', 'running'), {}).get('value') is True
                     and samples.get(('tank-A', 'level'), {}).get('value') is not None
                     and samples.get(('meter-A', 'current'), {}).get('value') != voltage.get('value'))
        recorder.add('T05', '数据采集', 'PASS' if collected and values_ok else 'FAIL',
                     '7 点 GOOD 且量程/取值可区分',
                     {'good': sorted(f'{d}.{p}' for d, p in good_points()),
                      'voltage': voltage.get('value'), 'alarm': alarm.get('value')},
                     evidence={'samples': samples})
        extra['samples'] = list(samples.items())

        status_online = await bus.wait_for(
            lambda: all(bus.devices.get(device) == 'ONLINE'
                        for device in ('meter-A', 'meter-B', 'tank-A', 'tank-B')), 20)
        hb_ok = await bus.wait_for(
            lambda: bus.heartbeats and len(bus.heartbeats[-1].get('devices') or []) == 4
            and (bus.heartbeats[-1].get('points') or {}).get('total', 0) >= 7, 40)
        hb = bus.heartbeats[-1] if bus.heartbeats else {}
        recorder.add('T06', '设备状态', 'PASS' if status_online and hb_ok else 'FAIL',
                     'device/status 四台 ONLINE，heartbeat 含 4 设备',
                     {'live': dict(bus.devices), 'heartbeats': len(bus.heartbeats),
                      'heartbeatDevices': hb.get('devices'), 'heartbeatPoints': hb.get('points')},
                     evidence={'heartbeat': hb})

        stored = await bus.wait_for(lambda: bus.stored >= 1, 20)
        recorder.add('T07', '遥测确认', 'PASS' if stored else 'FAIL',
                     '至少一批 STORED', f'stored={bus.stored} batches={len(bus.data)}')

        ack = await send(11, devices_v10(voltage_scale=0.1, tank_a_poll=2000), message='cfg-v11')
        scaled = await bus.wait_for(lambda: isinstance(bus.points.get(('meter-A', 'voltage'), {}).get('value'), (int, float))
                                    and 22 <= bus.points[('meter-A', 'voltage')]['value'] <= 24
                                    and bus.points[('meter-A', 'voltage')].get('quality') == 'GOOD', 25)
        recorder.add('T08', '修改配置', 'PASS' if ack and ack.get('status') == 'APPLIED' and scaled else 'FAIL',
                     'APPLIED v11 且 voltage 约为 22–24',
                     {'ack': ack, 'voltage': bus.points.get(('meter-A', 'voltage'))})

        mixed = snapshot(11, devices_v10(), message='cfg-v11-conflict')
        mixed['config']['devices'][0]['name'] = 'Changed'
        before = len(bus.acks)
        await bus.publish_config(json.dumps(mixed).encode())
        conflicted = await bus.wait_for(
            lambda: any(item.get('status') == 'REJECTED' and item.get('configVersion') == 11
                        for item in bus.acks[before:]), 15)
        conflict = next((item for item in reversed(bus.acks) if item.get('status') == 'REJECTED'), None)
        still_collecting = await bus.wait_for(
            lambda: bus.points.get(('meter-B', 'pressure'), {}).get('quality') == 'GOOD', 10)
        recorder.add('T09', '非法/冲突配置',
                     'PASS' if conflicted and conflict and still_collecting else 'FAIL',
                     'REJECTED 且采集继续', {'ack': conflict, 'pressure': bus.points.get(('meter-B', 'pressure'))})

        async def toggle_adapter(modbus=True, opcua=True):
            await mock_put_devices(mock_url, mock_devices(), opcua_enabled=opcua, modbus_enabled=modbus)
            await asyncio.sleep(1)

        await toggle_adapter(modbus=False, opcua=True)
        meters_down = await bus.wait_for(
            lambda: bus.devices.get('meter-A') in ('OFFLINE', 'DEGRADED', 'CONNECTING')
            and bus.devices.get('tank-A') == 'ONLINE', 25)
        recorder.add('T10', 'Modbus 掉线', 'PASS' if meters_down else 'FAIL',
                     'meter 离线且 tank 仍 ONLINE', dict(bus.devices))
        await toggle_adapter(True, True)
        meters_up = await bus.wait_for(lambda: bus.devices.get('meter-A') == 'ONLINE'
                                       and bus.points.get(('meter-A', 'voltage'), {}).get('quality') == 'GOOD', 30)
        recorder.add('T11', 'Modbus 恢复', 'PASS' if meters_up else 'FAIL',
                     'meter-A ONLINE 且 GOOD', dict(bus.devices))

        await toggle_adapter(modbus=True, opcua=False)
        tanks_down = await bus.wait_for(
            lambda: bus.devices.get('tank-A') in ('OFFLINE', 'DEGRADED', 'CONNECTING')
            and bus.devices.get('meter-A') == 'ONLINE', 25)
        recorder.add('T12', 'OPC UA 掉线', 'PASS' if tanks_down else 'FAIL',
                     'tank 离线且 meter 仍 ONLINE', dict(bus.devices))
        await toggle_adapter(True, True)
        tanks_up = await bus.wait_for(lambda: bus.devices.get('tank-A') == 'ONLINE'
                                      and bus.points.get(('tank-A', 'level'), {}).get('quality') == 'GOOD', 30)
        recorder.add('T13', 'OPC UA 恢复', 'PASS' if tanks_up else 'FAIL',
                     'tank-A ONLINE 且 GOOD', dict(bus.devices))

        ack = await send(12, devices_v10(voltage_scale=0.1, tank_b_enabled=False), message='cfg-v12')
        disabled = await bus.wait_for(lambda: bus.devices.get('tank-B') == 'DISABLED'
                                      and bus.devices.get('meter-A') == 'ONLINE', 20)
        others = await bus.wait_for(lambda: bus.points.get(('tank-A', 'level'), {}).get('quality') == 'GOOD', 15)
        recorder.add('T14', '停用单台设备',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and disabled and others else 'FAIL',
                     'tank-B DISABLED，其余继续', dict(bus.devices))

        ack = await send(13, devices_v10(), enabled=False, message='cfg-v13')
        stopped = await bus.wait_for(lambda: all(bus.devices.get(d) == 'DISABLED'
                                                 for d in ('meter-A', 'meter-B', 'tank-A', 'tank-B')), 20)
        hb = next((item for item in reversed(bus.heartbeats)
                   if item.get('activeConfigVersion') == 13), {})
        recorder.add('T15', '停止全部采集',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and stopped else 'FAIL',
                     '四台 DISABLED', {'devices': dict(bus.devices), 'heartbeat': hb.get('points')})

        ack = await send(14, devices_v10(), message='cfg-v14')
        resumed = await bus.wait_for(lambda: all(bus.devices.get(d) == 'ONLINE'
                                                 for d in ('meter-A', 'meter-B', 'tank-A', 'tank-B'))
                                     and good_points() == set(POINTS), 40)
        recorder.add('T16', '重新采集', 'PASS' if ack and ack.get('status') == 'APPLIED' and resumed else 'FAIL',
                     '四台 ONLINE 且 7 点 GOOD', {'devices': dict(bus.devices),
                                               'good': sorted(f'{d}.{p}' for d, p in good_points())})

        ack = await send(14, devices_v10(), message='cfg-v14-repeat')
        codes = {e.get('code') for e in (ack or {}).get('errors') or []}
        recorder.add('T18', '同版本重复下发',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and ack.get('activeConfigVersion') == 14 else 'FAIL',
                     '再次 APPLIED v14', ack)

        ack = await send(10, devices_v10(), message='cfg-v10-stale')
        codes = {e.get('code') for e in (ack or {}).get('errors') or []}
        recorder.add('T19', '旧版本下发',
                     'PASS' if ack and ack.get('status') == 'REJECTED' and 'STALE_VERSION' in codes
                     and ack.get('activeConfigVersion') == 14 else 'FAIL',
                     'STALE_VERSION 且活动版本仍为 14', ack)

        await toggle_adapter(modbus=False, opcua=True)
        ack = await send(16, devices_v10(), message='cfg-v16-offline')
        offline_applied = ack and ack.get('status') == 'APPLIED' and ack.get('activeConfigVersion') == 16
        tank_ok = await bus.wait_for(lambda: bus.points.get(('tank-A', 'level'), {}).get('quality') == 'GOOD', 15)
        recorder.add('T20', '设备离线仍 APPLIED',
                     'PASS' if offline_applied and tank_ok else 'FAIL',
                     'APPLIED v16，OPC UA 继续采集',
                     {'ack': ack, 'devices': dict(bus.devices)})
        await toggle_adapter(True, True)
        await bus.wait_for(lambda: bus.devices.get('meter-A') == 'ONLINE', 25)

        ack = await send(17, devices_v10(bad_address=True), message='cfg-v17-bad-addr')
        isolated = await bus.wait_for(
            lambda: bus.points.get(('meter-A', 'voltage'), {}).get('quality') == 'GOOD'
            and bus.points.get(('meter-B', 'pressure'), {}).get('quality') == 'GOOD', 20)
        ghost = bus.points.get(('meter-A', 'ghost-reg'), {})
        recorder.add('T21', '单点非法地址隔离',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and isolated else 'FAIL',
                     '配置 APPLIED，voltage/pressure 仍 GOOD',
                     {'ghost': ghost, 'voltage': bus.points.get(('meter-A', 'voltage'))})

        voltage_ts = (bus.points.get(('meter-A', 'voltage')) or {}).get('timestamp') or 0
        ack = await send(18, devices_v10(report_mode='change'), message='cfg-v18-change')
        changed = await bus.wait_for(
            lambda: (bus.points.get(('meter-A', 'voltage')) or {}).get('quality') == 'GOOD'
            and ((bus.points.get(('meter-A', 'voltage')) or {}).get('timestamp') or 0) > voltage_ts, 25)
        recorder.add('T22', 'change 上报模式',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and changed else 'FAIL',
                     'APPLIED 后电压变化仍上报 GOOD',
                     {'voltage': bus.points.get(('meter-A', 'voltage'))})

        if started_broker:
            voltage_ts = (bus.points.get(('meter-A', 'voltage')) or {}).get('timestamp') or 0
            sessions_before = {s.get('sessionId') for s in bus.status if s.get('sessionId')}
            await docker('stop', BROKER_NAME)
            await asyncio.sleep(4)
            await docker('start', BROKER_NAME)
            recovered = await bus.wait_for(
                lambda: any(item.get('online') is True for item in bus.status[-8:])
                and (bus.points.get(('meter-A', 'voltage')) or {}).get('quality') == 'GOOD'
                and ((bus.points.get(('meter-A', 'voltage')) or {}).get('timestamp') or 0) > voltage_ts, 45)
            sessions_after = {s.get('sessionId') for s in bus.status if s.get('sessionId')}
            recorder.add('T23', 'Broker 短暂断开',
                         'PASS' if recovered else 'FAIL',
                         'Broker 恢复后重新 online 并继续上报',
                         {'recovered': recovered,
                          'newSession': bool(sessions_after - sessions_before),
                          'voltage': bus.points.get(('meter-A', 'voltage'))})
        else:
            recorder.add('T23', 'Broker 短暂断开', 'SKIPPED',
                         '采集继续并在重连后补传', '未执行：1883 由外部进程占用',
                         notes='本轮使用已有 Broker，不停服务')

        await asyncio.sleep(2)
        stored_before, data_before = bus.stored, len(bus.data)
        bus.ack_enabled = False
        await asyncio.sleep(4)
        data_grew = len(bus.data) > data_before
        stored_paused = bus.stored
        bus.ack_enabled = True
        flushed = await bus.flush_held()
        resumed_ack = flushed > 0 or await bus.wait_for(lambda: bus.stored > stored_paused, 20)
        recorder.add('T24', '接收端停 ACK 后补传',
                     'PASS' if data_grew and resumed_ack else 'FAIL',
                     '停 ACK 期间仍收到 data，恢复后补发 STORED',
                     {'storedBefore': stored_before, 'storedPaused': stored_paused,
                      'storedAfter': bus.stored, 'dataGrew': len(bus.data) - data_before,
                      'flushed': flushed})

        stop.set()
        await asyncio.wait_for(asyncio.shield(agent_task), 40)
        stop = asyncio.Event()
        gets_before = len(bus.gets)
        voltage_ts = (bus.points.get(('meter-A', 'voltage')) or {}).get('timestamp') or 0
        agent_task = asyncio.create_task(
            run_agent(agent_settings, [], None, 'all', mqtt_settings, True, stop, 'boot-closed-loop-2'),
            name='closed-loop-agent-2')
        restarted = await bus.wait_for(
            lambda: any(s.get('online') is True for s in bus.status[-8:])
            and (bus.points.get(('meter-A', 'voltage')) or {}).get('quality') == 'GOOD'
            and ((bus.points.get(('meter-A', 'voltage')) or {}).get('timestamp') or 0) > voltage_ts, 40)
        recorder.add('T25', 'Agent 进程重启',
                     'PASS' if restarted else 'FAIL',
                     '重启后恢复采集',
                     {'gets': len(bus.gets) - gets_before, 'voltage': bus.points.get(('meter-A', 'voltage')),
                      'devices': dict(bus.devices)})
        recorder.add('T30', 'retained 启动恢复',
                     'PASS' if restarted else 'FAIL',
                     '无需新下发即可从 retained/本地库恢复采集',
                     {'gets': len(bus.gets) - gets_before})

        ack = await send(19, devices_v10(meter_b_port=1), message='cfg-v19-one-down')
        one_down = await bus.wait_for(
            lambda: bus.devices.get('meter-B') in ('OFFLINE', 'DEGRADED', 'CONNECTING')
            and bus.devices.get('meter-A') == 'ONLINE'
            and bus.points.get(('meter-A', 'voltage'), {}).get('quality') == 'GOOD', 25)
        recorder.add('T26', '只停一台现场设备',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and one_down else 'FAIL',
                     'meter-B 离线，meter-A 继续', dict(bus.devices))
        await send(20, devices_v10(), message='cfg-v20-restore')
        await bus.wait_for(lambda: bus.devices.get('meter-B') == 'ONLINE', 25)

        ack = await send(21, devices_v10(ghost_node=True), message='cfg-v21-ghost')
        ghost_isolated = await bus.wait_for(
            lambda: bus.points.get(('tank-A', 'level'), {}).get('quality') == 'GOOD'
            and bus.points.get(('tank-A', 'ghost'), {}).get('quality') in (
                'BAD_PROTOCOL', 'BAD_DECODE', 'BAD_CONNECTION', 'UNKNOWN'), 20)
        recorder.add('T27', '错误 NodeId 隔离',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and ghost_isolated else 'FAIL',
                     'ghost 坏质量，level 仍 GOOD',
                     {'ghost': bus.points.get(('tank-A', 'ghost')),
                      'level': bus.points.get(('tank-A', 'level')),
                      'status': bus.devices.get('tank-A')})

        ack = await send(22, devices_v10(tank_user='iot', tank_password_env='IOT_OPCUA_PASSWORD_MISSING'),
                         message='cfg-v22-ua-user')
        ua_bad = await bus.wait_for(
            lambda: bus.devices.get('tank-A') in ('OFFLINE', 'DEGRADED', 'CONNECTING')
            and bus.points.get(('meter-A', 'voltage'), {}).get('quality') == 'GOOD', 20)
        recorder.add('T28', 'OPC UA 缺密码环境变量',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and ua_bad else 'FAIL',
                     'tank-A 配置错误，meter 继续',
                     {'ack': ack, 'tank-A': bus.devices.get('tank-A'),
                      'lastPoint': bus.points.get(('tank-A', 'level'))})
        await send(23, devices_v10(), message='cfg-v23-restore')
        await bus.wait_for(lambda: bus.points.get(('tank-A', 'level'), {}).get('quality') == 'GOOD', 25)

        ack = await send(24, devices_v10(polls={'voltage': 500, 'current': 1000, 'pressure': 5000, 'level': 2000}),
                         message='cfg-v24-mixed')
        mixed_core = {('meter-A', 'voltage'), ('meter-A', 'current'), ('meter-B', 'pressure'),
                      ('tank-A', 'level'), ('tank-B', 'temperature')}
        mixed_ok = await bus.wait_for(lambda: mixed_core <= good_points(), 40)
        recorder.add('T29', '混合采集周期',
                     'PASS' if ack and ack.get('status') == 'APPLIED' and mixed_ok else 'FAIL',
                     '不同周期的数值点均为 GOOD',
                     {'good': sorted(f'{d}.{p}' for d, p in good_points())})

        ack = await send(25, [], message='cfg-v25')
        cleared = await bus.wait_for(
            lambda: bool(bus.heartbeats)
            and bus.heartbeats[-1].get('activeConfigVersion') == 25
            and not (bus.heartbeats[-1].get('devices') or []), 45)
        recorder.add('T17', '清空子设备', 'PASS' if ack and ack.get('status') == 'APPLIED' and cleared else 'FAIL',
                     'APPLIED 且心跳无子设备',
                     {'ack': ack, 'heartbeatDevices': (bus.heartbeats[-1].get('devices') if bus.heartbeats else None)})
        extra['samples'] = [(key, bus.points.get(key) or {}) for key in POINTS]
        extra['environment']['storedBatches'] = bus.stored
        extra['environment']['dataBatches'] = len(bus.data)
        return recorder, extra
    finally:
        stop.set()
        if bus is not None:
            await bus.close()
        if agent_task is not None:
            await asyncio.wait_for(asyncio.shield(agent_task), 40)
        if mock_proc is not None and mock_proc.returncode is None:
            mock_proc.terminate()
            try:
                await asyncio.wait_for(mock_proc.wait(), 10)
            except TimeoutError:
                mock_proc.kill()
                await asyncio.gather(mock_proc.wait(), return_exceptions=True)
        if mock_err is not None:
            mock_err.close()
        try:
            log = work_path / 'mock-stderr.log'
            if log.exists():
                dest = ROOT / '.local/closed-loop/mock-stderr.log'
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(log.read_bytes())
        except OSError:
            pass
        if started_broker:
            try:
                await docker('rm', '-f', BROKER_NAME)
            except Exception:
                pass
        work.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mock-project', type=Path, default=Path(r'D:/work/code/PLCnext-mock-device'))
    parser.add_argument('--mock-python', type=Path, default=ROOT / '.local/mock-venv/Scripts/python.exe')
    parser.add_argument('--report', type=Path, default=ROOT / 'doc/闭环联调测试报告.md')
    parser.add_argument('--results', type=Path, default=ROOT / '.local/closed-loop/results.json')
    args = parser.parse_args()
    recorder, extra = asyncio.run(run_cases(args.mock_project.resolve(), args.mock_python.resolve(),
                                            args.report, args.results))
    args.results.parent.mkdir(parents=True, exist_ok=True)
    payload = {'generatedAt': utc_now(), 'summary': recorder.summary(), 'cases': recorder.cases, **extra}

    def jsonable(value):
        if isinstance(value, dict):
            return {str(key) if not isinstance(key, (str, int, float, bool)) and key is not None else key:
                    jsonable(item) for key, item in value.items()}
        if isinstance(value, tuple):
            return [jsonable(item) for item in value]
        if isinstance(value, list):
            return [jsonable(item) for item in value]
        return value

    args.results.write_text(json.dumps(jsonable(payload), ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    args.report.write_text(markdown_report(recorder, extra), encoding='utf-8')
    print(json.dumps({'event': 'finished', 'summary': recorder.summary(),
                      'report': str(args.report), 'results': str(args.results)}), flush=True)
    counts = recorder.summary()
    return 0 if counts.get('FAIL', 0) == 0 and counts.get('BLOCKED', 0) == 0 and counts.get('PASS', 0) else 1


if __name__ == '__main__':
    raise SystemExit(main())
