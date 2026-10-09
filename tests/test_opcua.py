import asyncio
import json
from pathlib import Path
import socket
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.contracts import validate_message
from plcnext_iot.devices.factory import DRIVERS, bind
from plcnext_iot.devices.runtime import DeviceRuntime, UnsupportedDriver
from plcnext_iot.points.samples import SampleBuffer
from plcnext_iot.reporting.engine import ReportEngine


def load_opcua():
    return json.loads((ROOT / 'contracts/examples/valid/config-opcua.json').read_text(encoding='utf-8'))


class OpcUaContractTests(unittest.TestCase):
    def codes(self, message):
        return {issue.code for issue in validate_message('configSet', json.dumps(message).encode())}

    def test_valid_example_and_unknown_fields(self):
        msg = load_opcua()
        self.assertEqual(set(), self.codes(msg))
        snapshot = ConfigSnapshot.parse(json.dumps(msg).encode(), 'PLC-01')
        device = snapshot.devices[0]
        self.assertEqual(device.protocol, 'opcua')
        self.assertIsNone(device.connection.unit_id)
        self.assertEqual(device.points[0].opcua.node_id, 'ns=2;s=tank-A.level')
        self.assertIsNone(device.points[0].modbus)
        msg['config']['devices'][0]['points'][0]['opcua']['password'] = 'secret'
        issues = validate_message('configSet', json.dumps(msg).encode())
        self.assertIn('SCHEMA_INVALID', {i.code for i in issues})
        self.assertNotIn('secret', str(issues))

    def test_security_and_credentials_are_paired(self):
        msg = load_opcua()
        conn = msg['config']['devices'][0]['connection']
        conn['securityMode'] = 'SignAndEncrypt'
        self.assertIn('SCHEMA_INVALID', self.codes(msg))
        conn['securityMode'] = 'None'
        conn['username'] = 'iot_client'
        self.assertIn('SCHEMA_INVALID', self.codes(msg))
        conn['passwordEnv'] = 'IOT_OPCUA_PASSWORD'
        self.assertEqual(set(), self.codes(msg))

    def test_modbus_fields_are_rejected_on_opcua_devices(self):
        msg = load_opcua()
        msg['config']['devices'][0]['connection']['unitId'] = 1
        self.assertIn('SCHEMA_INVALID', self.codes(msg))
        msg = load_opcua()
        msg['config']['devices'][0]['points'][0]['modbus'] = {
            'area': 'holding_register', 'address': 0, 'byteOrder': 'AB'}
        self.assertIn('SCHEMA_INVALID', self.codes(msg))

    def test_mixed_snapshot_keeps_protocol_boundaries(self):
        mixed = json.loads((ROOT / 'contracts/examples/valid/config-set.json').read_text())
        mixed['config']['devices'].append(load_opcua()['config']['devices'][0])
        self.assertEqual(set(), self.codes(mixed))
        snapshot = ConfigSnapshot.parse(json.dumps(mixed).encode(), 'PLC-01')
        protocols = {d.device_id: d.protocol for d in snapshot.devices}
        self.assertEqual(protocols['meter-A'], 'modbus_tcp')
        self.assertEqual(protocols['tank-A'], 'opcua')


class OpcUaRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from asyncua import Server, ua
        self.Server, self.ua = Server, ua
        self.server = Server()
        await self.server.init()
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        self.server.set_endpoint(f'opc.tcp://127.0.0.1:{self.port}/iot/')
        self.idx = await self.server.register_namespace('urn:plcnext-iot:test')
        self.level = await self.server.nodes.objects.add_variable(
            ua.NodeId('tank.level', self.idx), 'Level', 12.5, ua.VariantType.Float)
        self.running = await self.server.nodes.objects.add_variable(
            ua.NodeId('tank.running', self.idx), 'Running', True, ua.VariantType.Boolean)
        await self.level.set_writable()
        await self.running.set_writable()
        await self.server.start()
        self.runtime = None

    async def asyncTearDown(self):
        if self.runtime is not None:
            await self.runtime.shutdown(5)
        try:
            await self.server.stop()
        except Exception:
            pass

    def payload(self, *points):
        msg = load_opcua()
        conn = msg['config']['devices'][0]['connection']
        conn.update(host='127.0.0.1', port=self.port, path='/iot/',
                    connectTimeoutMs=2000, requestTimeoutMs=1000)
        default = {
            'enabled': True, 'pollIntervalMs': 100, 'scale': 1, 'offset': 0,
            'reportMode': 'cyclic', 'reportIntervalMs': 1000, 'deadband': 0, 'staleAfterMs': 1000,
        }
        configured = []
        for point_id, name, data_type, identifier, extra in points:
            item = dict(default, pointId=point_id, name=name, dataType=data_type,
                        opcua={'nodeId': f'ns={self.idx};s={identifier}'})
            item.update(extra)
            configured.append(item)
        msg['config']['devices'][0]['points'] = configured
        return json.dumps(msg).encode()

    async def start_agent(self, payload, driver='opcua'):
        samples = SampleBuffer(64)
        runtime = DeviceRuntime(lambda device: bind(runtime, samples, DRIVERS[driver])(device))
        self.runtime = runtime
        transition = runtime.prepare(ConfigSnapshot.parse(payload, 'PLC-01'))
        await transition.activate()
        transition.publish()
        return runtime, samples

    async def collect(self, samples, predicate, count=1):
        found = {}
        async with asyncio.timeout(8):
            while len(found) < count:
                sample = await samples.get()
                if predicate(sample):
                    found[(sample.device_id, sample.point_id)] = sample
        return found

    async def test_subscription_decodes_scale_and_bool(self):
        payload = self.payload(
            ('level', 'Level', 'float32', 'tank.level', {'scale': 2, 'offset': 1}),
            ('running', 'Running', 'bool', 'tank.running', {}),
        )
        _, samples = await self.start_agent(payload)
        found = await self.collect(samples, lambda s: s.quality == 'GOOD', 2)
        self.assertEqual(found[('tank-A', 'level')].value, 26.0)
        self.assertEqual(found[('tank-A', 'running')].value, True)

    async def test_constant_values_remain_fresh_after_stale_interval(self):
        payload = self.payload(
            ('level', 'Level', 'float32', 'tank.level', {}),
            ('running', 'Running', 'bool', 'tank.running', {}),
        )
        runtime, samples = await self.start_agent(payload)
        await self.collect(samples, lambda s: s.quality == 'GOOD', 2)
        await asyncio.sleep(1.3)
        runner = runtime.runners['tank-A']
        now = asyncio.get_running_loop().time()
        state, good, total = runner.diagnostics(runtime.snapshot.devices[0], now)
        self.assertEqual((state['status'], good, total), ('ONLINE', 2, 2))
        self.assertTrue(all(now - sample.monotonic_time < 0.5
                            for sample in runner.latest.values()))

    async def test_reused_subscription_refreshes_after_config_version_change(self):
        payload = self.payload(('level', 'Level', 'float32', 'tank.level', {}))
        runtime, samples = await self.start_agent(payload)
        old_runner = runtime.runners['tank-A']
        report = ReportEngine('PLC-01', 'boot-test')
        report.configure(runtime.snapshot, asyncio.get_running_loop().time())
        await self.collect(samples, lambda sample: sample.quality == 'GOOD')

        wire = json.loads(payload)
        wire['configVersion'] += 1
        wire['messageId'] = 'cfg-next-version'
        transition = runtime.prepare(ConfigSnapshot.parse(json.dumps(wire).encode(), 'PLC-01'))
        await transition.activate()
        transition.publish()
        self.assertIs(runtime.runners['tank-A'], old_runner)
        report.configure(runtime.snapshot, asyncio.get_running_loop().time())
        async with asyncio.timeout(3):
            while True:
                sample = await samples.get()
                if sample.config_version == wire['configVersion'] and sample.quality == 'GOOD':
                    break
        self.assertTrue(report.accept(sample, asyncio.get_running_loop().time()))

    async def test_unknown_node_is_protocol_error_and_other_points_continue(self):
        payload = self.payload(
            ('level', 'Level', 'float32', 'tank.level', {}),
            ('missing', 'Missing', 'float32', 'tank.missing', {}),
        )
        runtime, samples = await self.start_agent(payload)
        found = await self.collect(samples, lambda s: (
            (s.point_id == 'level' and s.quality == 'GOOD')
            or (s.point_id == 'missing' and s.quality == 'BAD_PROTOCOL')), 2)
        self.assertEqual(found[('tank-A', 'level')].value, 12.5)
        self.assertIsNone(found[('tank-A', 'missing')].value)
        state, good, total = runtime.runners['tank-A'].diagnostics(
            runtime.snapshot.devices[0], asyncio.get_running_loop().time())
        self.assertEqual(state['status'], 'DEGRADED')
        self.assertEqual((good, total), (1, 2))

    async def test_type_mismatch_is_bad_decode(self):
        payload = self.payload(('running', 'Running', 'float32', 'tank.running', {}))
        _, samples = await self.start_agent(payload)
        found = await self.collect(samples, lambda s: s.quality == 'BAD_DECODE')
        self.assertIsNone(found[('tank-A', 'running')].value)

    async def test_modbus_only_driver_rejects_opcua_device(self):
        runtime = DeviceRuntime(lambda device: bind(runtime, SampleBuffer(), DRIVERS['modbus-tcp'])(device))
        with self.assertRaises(UnsupportedDriver):
            runtime.prepare(ConfigSnapshot.parse(self.payload(
                ('level', 'Level', 'float32', 'tank.level', {})), 'PLC-01'))
