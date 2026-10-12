"""Real committed-store reads and Web API boundaries, with no live services."""
import asyncio
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from plcnext_iot.config.store import ConfigStore
from plcnext_iot.provisioning.devices import ConfigUnavailable, detail, read_active
from plcnext_iot.provisioning.server import SetupServer
from plcnext_iot.provisioning.store import SetupStore


def wire(version=10, opcua=False):
    name = 'config-opcua.json' if opcua else 'config-set.json'
    value = json.loads((ROOT / 'contracts/examples/valid' / name).read_text())
    value.update(configVersion=version, messageId=f'config-{version}')
    return value


def commit(store, value):
    candidate = store.prepare(json.dumps(value).encode())
    assert candidate.status == 'PREPARED'
    return store.commit(candidate.token)


class CommittedReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.path = self.directory / 'agent.db'

    def test_missing_database_is_empty_without_creating_directory_or_file(self):
        directory = self.directory / 'not-created'
        self.assertIsNone(read_active(directory, 'PLC-01'))
        self.assertFalse(directory.exists())

    def test_empty_initialized_store_reads_with_owner_lock_held(self):
        with ConfigStore(self.path, 'PLC-01'):
            self.assertIsNone(read_active(self.directory, 'PLC-01'))

    def test_only_active_committed_version_and_no_recovery_of_prepared(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            commit(store, wire())
            pending = store.prepare(json.dumps(wire(11)).encode())
            before = store._conn.execute('SELECT * FROM snapshots ORDER BY version').fetchall()
            hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in self.directory.iterdir() if p.name in ('agent.db', 'agent.db-wal')}
            snapshot = read_active(self.directory, 'PLC-01')
            self.assertEqual((snapshot.config_version, snapshot.message_id), (10, 'config-10'))
            self.assertEqual(store._conn.execute('SELECT * FROM snapshots ORDER BY version').fetchall(), before)
            self.assertEqual(hashes, {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                     for p in self.directory.iterdir() if p.name in hashes})
            self.assertEqual(store._candidate(pending.token)['state'], 'PREPARED')
            store.fail(pending.token)
            self.assertEqual(read_active(self.directory, 'PLC-01').config_version, 10)

    def test_prepared_without_active_is_empty(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            store.prepare(json.dumps(wire()).encode())
            self.assertIsNone(read_active(self.directory, 'PLC-01'))

    def test_metadata_and_payload_use_same_read_transaction_when_commit_interleaves(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            commit(store, wire())
            candidate = store.prepare(json.dumps(wire(11)).encode())
            connect = sqlite3.connect
            interleaved = []

            def reader_connection(*args, **kwargs):
                conn = connect(*args, **kwargs)

                def on_sql(sql):
                    if sql.startswith('SELECT version,payload') and not interleaved:
                        interleaved.append(True)
                        store.commit(candidate.token)
                conn.set_trace_callback(on_sql)
                return conn

            with patch('plcnext_iot.provisioning.devices.sqlite3.connect', reader_connection):
                snapshot = read_active(self.directory, 'PLC-01')
            self.assertTrue(interleaved)
            self.assertEqual((snapshot.config_version, snapshot.message_id), (10, 'config-10'))
            self.assertEqual(read_active(self.directory, 'PLC-01').config_version, 11)

    def test_reader_uses_read_only_uri_and_never_owns_store(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            commit(store, wire())
            connect = sqlite3.connect
            with patch('plcnext_iot.provisioning.devices.sqlite3.connect', wraps=connect) as opened:
                read_active(self.directory, 'PLC-01')
            self.assertTrue(opened.call_args.args[0].endswith('?mode=ro'))
            self.assertTrue(opened.call_args.kwargs['uri'])
            self.assertEqual(opened.call_args.kwargs['isolation_level'], None)

    def test_gateway_mismatch_and_unknown_schema_are_unavailable(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            commit(store, wire())
            with self.assertRaises(ConfigUnavailable):
                read_active(self.directory, 'PLC-02')
        with closing(sqlite3.connect(self.path)) as conn:
            conn.execute('PRAGMA user_version=999')
        with self.assertRaises(ConfigUnavailable):
            read_active(self.directory, 'PLC-01')

    def test_corrupt_file_never_exposes_data_or_reinitializes(self):
        contents = b'BROKEN-DATABASE-secret-and-local-path'
        self.path.write_bytes(contents)
        with self.assertRaises(ConfigUnavailable) as error:
            read_active(self.directory, 'PLC-01')
        self.assertEqual(str(error.exception), 'CONFIG_UNAVAILABLE')
        self.assertEqual(self.path.read_bytes(), contents)

    def test_invalid_active_pointer_state_hash_and_envelope_are_unavailable(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            commit(store, wire())
            row = store._conn.execute('SELECT payload,content_hash FROM snapshots').fetchone()
            mutations = ["UPDATE snapshots SET state='FAILED'",
                         "UPDATE snapshots SET content_hash='bad'",
                         "UPDATE metadata SET active_version=0",
                         "UPDATE snapshots SET payload='{}'"]
            for sql in mutations:
                with self.subTest(sql=sql):
                    store._conn.execute('PRAGMA foreign_keys=OFF')
                    store._conn.execute('BEGIN')
                    store._conn.execute(sql)
                    store._conn.execute('COMMIT')
                    with self.assertRaises(ConfigUnavailable):
                        read_active(self.directory, 'PLC-01')
                    store._conn.execute("UPDATE snapshots SET state='COMMITTED',payload=?,content_hash=?", row)
                    store._conn.execute('UPDATE metadata SET active_version=10')

    def test_locked_store_returns_bounded_unavailable(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            commit(store, wire())
        # Rollback journal EXCLUSIVE blocks even read-only clients; no writer
        # process or network service is involved in this temporary fixture.
        with closing(sqlite3.connect(self.path)) as locker:
            locker.execute('PRAGMA journal_mode=DELETE')
            locker.execute('BEGIN EXCLUSIVE')
            try:
                with self.assertRaises(ConfigUnavailable):
                    read_active(self.directory, 'PLC-01')
            finally:
                locker.rollback()

    def test_opcua_credentials_and_environment_references_are_projected(self):
        value = wire(opcua=True)
        value['config']['devices'][0]['connection'].update(username='private-user', passwordEnv='OPCUA_SECRET')
        with ConfigStore(self.path, 'PLC-01') as store:
            snapshot = commit(store, value)
            public = detail(snapshot.devices[0])
        encoded = json.dumps(public)
        self.assertNotIn('private-user', encoded)
        self.assertNotIn('OPCUA_SECRET', encoded)
        self.assertNotIn('passwordEnv', encoded)
        self.assertEqual((public['connection']['usernameSet'], public['connection']['passwordSet']), (True, True))
        self.assertEqual(public['points'][0]['opcua']['nodeId'], 'ns=2;s=tank-A.level')


class DevicesApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.directory = Path(self.temp.name)
        self.setup = SetupStore(self.directory)
        self.server = SetupServer(self.setup)
        await self.server.start('127.0.0.1', 0)
        self.port = self.server.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        await self.server.close()
        self.temp.cleanup()

    def configured(self):
        self.server.value = dict(gatewayId='PLC-01')

    async def request(self, path, method='GET'):
        reader, writer = await asyncio.open_connection('127.0.0.1', self.port)
        writer.write(f'{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n'.encode())
        await writer.drain()
        result = await reader.read()
        writer.close()
        await writer.wait_closed()
        head, body = result.split(b'\r\n\r\n', 1)
        self.assertIn(b'Cache-Control: no-store', head)
        return int(head.split()[1]), json.loads(body)

    async def test_unconfigured_and_waiting_for_first_config_are_distinct_empty_states(self):
        for configured in (False, True):
            if configured:
                self.configured()
            code, result = await self.request('/api/devices')
            self.assertEqual(code, 200)
            self.assertEqual((result['configured'], result['configVersion'], result['devices']),
                             (configured, 0, []))
            self.assertIsNone(result['enabled'])
            self.assertFalse((self.directory / 'agent.db').exists())

    async def test_list_and_encoded_detail_return_safe_committed_configuration(self):
        self.configured()
        with ConfigStore(self.directory / 'agent.db', 'PLC-01') as store:
            commit(store, wire())
            code, result = await self.request('/api/devices')
            self.assertEqual(code, 200)
            self.assertEqual(result['configVersion'], 10)
            self.assertEqual(result['report'], dict(batchIntervalMs=1000, maxBatchPoints=500))
            self.assertEqual(result['devices'], [dict(deviceId='meter-A', name='Meter A', enabled=True,
                                                      protocol='modbus_tcp', pointCount=1)])
            code, result = await self.request('/api/devices/meter%2DA?configVersion=10')
            self.assertEqual(code, 200)
            self.assertEqual(result['device']['connection']['unitId'], 1)
            self.assertEqual(result['device']['points'][0]['modbus']['address'], 0)
            self.assertEqual(result['device']['points'][0]['scale'], 0.1)

    async def test_configuration_change_prevents_mixing_list_and_detail_versions(self):
        self.configured()
        with ConfigStore(self.directory / 'agent.db', 'PLC-01') as store:
            commit(store, wire())
            commit(store, wire(11))
            code, result = await self.request('/api/devices/meter-A?configVersion=10')
            self.assertEqual((code, result['code'], result['configVersion']), (409, 'CONFIG_CHANGED', 11))
            self.assertNotIn('device', result)
            self.assertEqual((await self.request('/api/devices/meter-A'))[0], 200)

    async def test_missing_device_and_unavailable_store_have_stable_errors(self):
        self.configured()
        code, result = await self.request('/api/devices/not-present')
        self.assertEqual((code, result['code']), (404, 'DEVICE_NOT_FOUND'))
        (self.directory / 'agent.db').write_bytes(b'CORRUPT-PRIVATE')
        code, result = await self.request('/api/devices')
        self.assertEqual((code, result), (503, {'code': 'CONFIG_UNAVAILABLE'}))

    async def test_invalid_paths_methods_and_expected_versions_are_rejected(self):
        for path in ('/api/devices/', '/api/devices/../agent.db', '/api/devices/meter%2FA',
                     '/api/devices/meter%ZZ', '/api/devices/meter%FF', '/api/devices-extra'):
            self.assertEqual((await self.request(path))[0], 404, path)
        for suffix in ('?configVersion=0', '?configVersion=-1', '?configVersion=x',
                       '?configVersion=10&configVersion=10', '?other=10', '?configVersion='):
            self.assertEqual((await self.request('/api/devices/meter-A'+suffix))[0], 400, suffix)
        self.assertEqual((await self.request('/api/devices?configVersion=10'))[0], 400)
        self.assertEqual((await self.request('/api/devices', 'POST'))[0], 404)

    async def test_sqlite_work_runs_off_event_loop(self):
        import threading
        self.configured()
        observed = []
        event_loop_thread = threading.get_ident()

        def reader(_directory, _gateway):
            observed.append(threading.get_ident())
            return None

        with patch('plcnext_iot.provisioning.server.read_active', reader):
            code, _ = await self.request('/api/devices')
        self.assertEqual(code, 200)
        self.assertNotEqual(observed, [event_loop_thread])


if __name__ == '__main__':
    unittest.main()
