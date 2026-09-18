import json
import asyncio
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from plcnext_iot.core.lifecycle import ApplyResult
from plcnext_iot.contracts import Issue, validate_message


class MqttMessagesTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((ROOT / 'src/plcnext_iot/messaging/messages.py').exists(), 'MQTT message mapping not implemented')
        from plcnext_iot.messaging.messages import config_ack, config_get, status
        self.ack, self.get, self.status = config_ack, config_get, status
        self.wire = json.loads((ROOT / 'contracts/examples/valid/config-empty.json').read_bytes())

    def payload(self):
        return json.dumps(self.wire).encode()

    def test_applied_ack_uses_request_identity_and_committed_version(self):
        out = self.ack(self.payload(), ApplyResult('APPLIED', 10), 'PLC-01')
        self.assertEqual(validate_message('configAck', out), [])
        value = json.loads(out)
        self.assertEqual(value['messageId'], self.wire['messageId'])
        self.assertEqual((value['configVersion'],value['activeConfigVersion'],value['status']), (10,10,'APPLIED'))

    def test_local_errors_are_mapped_to_wire_codes(self):
        for local, expected in [('UNSUPPORTED_DRIVER','APPLY_FAILED'),('MESSAGE_ID_CONFLICT','VERSION_CONFLICT'),
                                ('STORAGE_ERROR','STORAGE_ERROR'),('RESOURCE_LIMIT','RESOURCE_LIMIT'),
                                ('DUPLICATE_NODE_ID','DUPLICATE_NODE_ID')]:
            out = self.ack(self.payload(), ApplyResult('REJECTED', 0, local), 'PLC-01')
            self.assertEqual(validate_message('configAck',out), [])
            self.assertEqual(json.loads(out)['errors'][0]['code'], expected)

    def test_invalid_configuration_can_be_rejected_with_valid_envelope(self):
        self.wire['config']['secret'] = 'do-not-echo'
        out = self.ack(self.payload(), ApplyResult('REJECTED',0,issues=(Issue('SCHEMA_INVALID','/config','secret-data'),)), 'PLC-01')
        self.assertEqual(validate_message('configAck',out), [])
        self.assertNotIn(b'secret-data',out)
        self.assertNotIn(b'do-not-echo',out)

    def test_unidentifiable_or_foreign_payload_never_produces_ack(self):
        for payload in (b'{', b'{"messageId":"a","messageId":"b"}', b'x' * (2*1024*1024+1)):
            self.assertIsNone(self.ack(payload, ApplyResult('REJECTED',0,'INVALID_JSON'), 'PLC-01'))
        for field,value in [('gatewayId','OTHER'),('messageId','bad/topic'),('configVersion',True),('configVersion',0)]:
            wire = dict(self.wire, **{field:value})
            self.assertIsNone(self.ack(json.dumps(wire).encode(),ApplyResult('REJECTED',0), 'PLC-01'))

    def test_unknown_commit_outcome_and_inconsistent_success_do_not_fabricate_ack(self):
        self.assertIsNone(self.ack(self.payload(),ApplyResult('FAILED',None,'COMMIT_OUTCOME_UNKNOWN'), 'PLC-01'))
        self.assertIsNone(self.ack(self.payload(),ApplyResult('APPLIED',9), 'PLC-01'))

    def test_status_and_get_conform_to_contract(self):
        self.assertEqual(validate_message('configGet', self.get('PLC-01',10,'session-1')), [])
        for online,reason in [(True,'connected'),(False,'shutdown'),(False,'connection_lost')]:
            out = self.status('PLC-01','session-1', online,reason)
            self.assertEqual(validate_message('status',out), [])
            if reason == 'connection_lost':
                self.assertIsNone(json.loads(out)['timestamp'])


class MqttSettingsTests(unittest.TestCase):
    def test_strict_settings_and_secret_reference(self):
        self.assertTrue((ROOT / 'src/plcnext_iot/messaging/settings.py').exists(), 'MQTT settings not implemented')
        from plcnext_iot.messaging.settings import MqttSettings
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'mqtt.json'
            for invalid in ({'host':'localhost','port':True},{'host':'localhost','tls':'false'},
                            {'host':'localhost','password':'secret'}, {'host':'localhost','username':'u'},
                            {'host':'localhost','port':0}):
                path.write_text(json.dumps(invalid))
                with self.assertRaises(ValueError):
                    MqttSettings.from_file(path)
            path.write_text('{"host":"a","host":"b"}')
            with self.assertRaises(ValueError):
                MqttSettings.from_file(path)
            path.write_text(json.dumps({'host':'localhost','port':1883,'tls':False,
                                       'username':'u','passwordEnv':'IOT_MQTT_TEST_PASSWORD'}))
            with patch.dict(os.environ, {'IOT_MQTT_TEST_PASSWORD':'private-test-value'}):
                settings = MqttSettings.from_file(path)
                self.assertEqual(settings.password, 'private-test-value')
                self.assertNotIn('private-test-value',repr(settings))
            path.write_text('{"host":"localhost"}')
            settings = MqttSettings.from_file(path)
            self.assertTrue(settings.tls)
            self.assertEqual(settings.port,8883)


class MqttTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_ingress_is_bounded_and_rejects_foreign_or_oversize_messages(self):
        import paho.mqtt.client as mqtt
        from plcnext_iot.messaging.transport import PahoConnection
        from plcnext_iot.messaging.settings import MqttSettings
        connection = PahoConnection(MqttSettings('localhost'),'test',['own'])
        for index in range(10):
            message = mqtt.MQTTMessage(index)
            message.topic = b'own'
            message.payload = str(index).encode()
            connection._on_message(None,None,message)
        self.assertEqual(connection._incoming.qsize(),8)
        self.assertEqual(connection.dropped,2)
        message.topic = b'foreign'
        connection._on_message(None,None,message)
        message.topic = b'own'
        message.payload = b'x' * (2*1024*1024+1)
        connection._on_message(None,None,message)
        self.assertEqual(connection.dropped,4)
        self.assertEqual(connection._incoming.get_nowait().payload,b'0')
        await connection.close()
        self.assertTrue(connection._incoming.empty())

    async def test_rejected_or_downgraded_subscription_never_becomes_ready(self):
        from plcnext_iot.messaging.transport import PahoConnection
        from plcnext_iot.messaging.settings import MqttSettings
        async def packet(reader):
            kind = (await reader.readexactly(1))[0]
            length,multiplier = 0,1
            while True:
                digit = (await reader.readexactly(1))[0]
                length += (digit & 127)*multiplier
                if not digit & 128:
                    break
                multiplier *= 128
            return kind, await reader.readexactly(length)
        handlers = set()
        grant = 0x80
        async def broker(reader,writer):
            task = asyncio.current_task()
            handlers.add(task)
            try:
                await packet(reader)
                writer.write(b'\x20\x02\x00\x00')
                await writer.drain()
                _,payload = await packet(reader)
                writer.write(b'\x90\x03' + payload[:2] + bytes([grant]))
                await writer.drain()
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()
                handlers.discard(task)
        server = await asyncio.start_server(broker,'127.0.0.1',0)
        try:
            for grant in (0x80,0):
                c = PahoConnection(MqttSettings('127.0.0.1',server.sockets[0].getsockname()[1],tls=False),'test',['own'])
                try:
                    with self.assertRaises(ConnectionError):
                        await c.open()
                    self.assertFalse(c.connected)
                finally:
                    await c.close()
        finally:
            server.close()
            for task in list(handlers):
                task.cancel()
            await asyncio.gather(*handlers,return_exceptions=True)
            await server.wait_closed()
