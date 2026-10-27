"""Device-command replay, durable bases and real actor ownership, offline."""
import asyncio
import copy
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from plcnext_iot.config.store import ConfigStore, StoreError, StoreFormatError
from plcnext_iot.contracts import validate_message, topic_rules
from plcnext_iot.core.lifecycle import AgentLifecycle, ApplyResult
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.messaging.config_service import MqttConfigService
from plcnext_iot.messaging.messages import config_ack, config_patch_ack, config_get
from plcnext_iot.messaging.transport import Incoming
from plcnext_iot.provisioning.devices import read_active


def full(version=10):
    value = json.loads((ROOT/'contracts/examples/valid/config-set.json').read_text())
    value.update(messageId=f'full-{version}', configVersion=version)
    peer = copy.deepcopy(value['config']['devices'][0])
    peer.update(deviceId='meter-B', name='Meter B')
    peer['connection']['unitId'] = 2
    value['config']['devices'].append(peer)
    return value


def command(version=11, base=10, mid=None, base_mid=None, op='replace', device_id='meter-A'):
    value = dict(schemaVersion=1, gatewayId='PLC-01', messageId=mid or f'patch-{version}',
                 configVersion=version, timestamp=1000, baseConfigVersion=base,
                 baseMessageId=base_mid if base_mid is not None else (f'full-{base}' if base else None),
                 operations=[dict(op=op, deviceId=device_id)])
    if op != 'remove':
        device = full()['config']['devices'][0]
        device.update(deviceId=device_id, name='Modified meter')
        device['connection']['unitId'] = 3
        value['operations'][0]['device'] = device
    if base == 0:
        value['gateway'] = dict(enabled=True, report=dict(batchIntervalMs=1000, maxBatchPoints=500))
    return value


def raw(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()


class PatchContractTests(unittest.TestCase):
    def codes(self, value, kind='configPatch'):
        return {i.code for i in validate_message(kind, raw(value), expected_gateway_id='PLC-01')}

    def test_valid_bootstrap_existing_patch_and_retention_contract(self):
        self.assertFalse(self.codes(command()))
        self.assertFalse(self.codes(command(base=0, op='add')))
        for kind in ('configPatch', 'configPatchAck'):
            self.assertFalse(topic_rules()[kind]['retain'])
            self.assertEqual(topic_rules()[kind]['qos'], 1)

    def test_gateway_only_allowed_but_empty_command_or_empty_gateway_rejected(self):
        value = command(); value['operations'] = []
        self.assertIn('SCHEMA_INVALID', self.codes(value))
        value['gateway'] = {}
        self.assertIn('SCHEMA_INVALID', self.codes(value))
        value['gateway'] = dict(enabled=False)
        self.assertFalse(self.codes(value))
        bootstrap = command(base=0); bootstrap['operations'] = []
        self.assertFalse(self.codes(bootstrap))

    def test_unknown_fields_operations_shape_and_bootstrap_identity_are_rejected(self):
        bad = []
        a = command(); a['secret'] = 'never-echo'; bad.append(a)
        a = command(); a['operations'][0]['extra'] = True; bad.append(a)
        a = command(op='remove'); a['operations'][0]['device'] = {}; bad.append(a)
        a = command(); del a['operations'][0]['device']; bad.append(a)
        a = command(base=0); del a['gateway']; bad.append(a)
        a = command(base=0); a['baseMessageId'] = 'unexpected'; bad.append(a)
        a = command(); a['baseMessageId'] = None; bad.append(a)
        a = command(); a['operations'] = []; bad.append(a)
        a = command(); a['baseConfigVersion'] = True; bad.append(a)
        a = command(); a['operations'] *= 51; bad.append(a)
        for value in bad:
            issues = validate_message('configPatch', raw(value))
            self.assertTrue(issues)
            self.assertNotIn('never-echo', str(issues))

    def test_target_gap_allowed_but_not_old_duplicate_op_or_wrong_device_identity(self):
        self.assertFalse(self.codes(command(version=20)))
        self.assertIn('STALE_VERSION', self.codes(command(version=10)))
        value = command(); value['operations'].append(copy.deepcopy(value['operations'][0]))
        self.assertIn('OPERATION_CONFLICT', self.codes(value))
        value = command(); value['operations'][0]['device']['deviceId'] = 'other'
        self.assertIn('OPERATION_CONFLICT', self.codes(value))

    def test_device_and_points_keep_existing_complete_semantic_validation(self):
        value = command(); point = value['operations'][0]['device']['points'][0]
        point['staleAfterMs'] = 1
        self.assertIn('INVALID_STALE_INTERVAL', self.codes(value))
        point['staleAfterMs'] = 5000
        point['modbus']['address'] = 65535
        point['dataType'] = 'float32'; point['modbus']['byteOrder'] = 'ABCD'
        self.assertIn('INVALID_ADDRESS', self.codes(value))

    def test_patch_ack_historical_applied_is_distinct_from_original_full_ack(self):
        payload = raw(command())
        result = ApplyResult('APPLIED', 12, applied_config_version=11, active_message_id='patch-12')
        ack = config_patch_ack(payload, result, 'PLC-01')
        self.assertFalse(validate_message('configPatchAck', ack))
        self.assertEqual(json.loads(ack)['appliedConfigVersion'], 11)
        self.assertIsNone(config_ack(raw(full(11)), result, 'PLC-01'))
        for bad in (ApplyResult('APPLIED', 10, applied_config_version=11, active_message_id='full-10'),
                    ApplyResult('APPLIED', 12, applied_config_version=12, active_message_id='patch-12'),
                    ApplyResult('FAILED', None, active_message_id=None)):
            self.assertIsNone(config_patch_ack(payload, bad, 'PLC-01'))

    def test_rejected_ack_is_sanitized_and_active_identity_consistent(self):
        ack = config_patch_ack(raw(command()), ApplyResult('REJECTED', 10, 'BASE_CONFIG_MISMATCH',
                                                          active_message_id='full-10'), 'PLC-01')
        self.assertFalse(validate_message('configPatchAck', ack))
        value = json.loads(ack)
        self.assertIsNone(value['appliedConfigVersion'])
        self.assertEqual(value['errors'][0]['code'], 'BASE_CONFIG_MISMATCH')
        self.assertIsNone(config_patch_ack(raw(command()), ApplyResult('REJECTED', 0, 'APPLY_FAILED',
                                                                      active_message_id='other'), 'PLC-01'))
        legacy = config_get('PLC-01', 0, 'session-1')
        extended = config_get('PLC-01', 10, 'session-1', active_message_id='full-10', capabilities=True)
        self.assertFalse(validate_message('configGet', legacy))
        self.assertFalse(validate_message('configGet', extended))
        self.assertEqual(json.loads(extended)['configCapabilities'], ['devicePatchV1'])


class PatchStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.path = self.directory/'agent.db'

    def initialized(self):
        store = ConfigStore(self.path, 'PLC-01')
        self.addCleanup(store.close)
        store.commit(store.prepare(raw(full())).token)
        return store

    def test_add_replace_remove_materialize_full_and_preserve_unselected_devices(self):
        store = self.initialized()
        peer = next(d for d in store.load_active().devices if d.device_id == 'meter-B')
        for version, op, key in ((11, 'add', 'meter-C'), (12, 'replace', 'meter-A'), (13, 'remove', 'meter-C')):
            active = store.load_active()
            prepared = store.prepare_patch(raw(command(version, active.config_version, base_mid=active.message_id,
                                                       op=op, device_id=key)))
            self.assertEqual(prepared.status, 'PREPARED')
            self.assertEqual(store.load_active().config_version, version-1)
            saved = store.commit(prepared.token)
            self.assertEqual(next(d for d in saved.devices if d.device_id == 'meter-B'), peer)
            self.assertEqual(saved.message_id, f'patch-{version}')
            self.assertFalse(validate_message('configSet', saved.wire_json.encode()))
        self.assertEqual({d.device_id for d in store.load_active().devices}, {'meter-A', 'meter-B'})
        self.assertEqual(read_active(self.directory, 'PLC-01').config_version, 13)

    def test_exact_replay_precedes_stale_base_and_survives_restart(self):
        store = self.initialized()
        payload = raw(command())
        store.commit(store.prepare_patch(payload).token)
        newer = raw(command(15, 11, base_mid='patch-11', op='remove', device_id='meter-A'))
        store.commit(store.prepare_patch(newer).token)
        before = store._conn.total_changes
        replay = store.prepare_patch(payload)
        self.assertEqual((replay.status, replay.applied_config_version, replay.active_config_version), ('APPLIED', 11, 15))
        self.assertIsNone(replay.token)
        self.assertEqual(store._conn.total_changes, before)
        store.close()
        with ConfigStore(self.path, 'PLC-01') as reopened:
            self.assertEqual(reopened.prepare_patch(payload).applied_config_version, 11)
            receipt = reopened._conn.execute('SELECT command_digest,command_kind FROM requests WHERE message_id=?',
                                             ('patch-11',)).fetchone()
            self.assertEqual(tuple(receipt), (hashlib.sha256(payload).hexdigest(), 'configPatch'))

    def test_message_id_exact_bytes_kind_version_conflicts_are_not_replayed(self):
        store = self.initialized()
        payload = raw(command())
        store.commit(store.prepare_patch(payload).token)
        self.assertEqual(store.prepare_patch(payload+b' ').code, 'MESSAGE_ID_CONFLICT')
        value = command(); value['timestamp'] += 1
        self.assertEqual(store.prepare_patch(raw(value)).code, 'MESSAGE_ID_CONFLICT')
        self.assertEqual(store.prepare_patch(raw(command(12, mid='patch-11'))).code, 'MESSAGE_ID_CONFLICT')
        materialized = json.loads(store.load_active().wire_json)
        self.assertEqual(store.prepare(raw(materialized)).code, 'MESSAGE_ID_CONFLICT')
        value = command(12, 11, mid='full-10', base_mid='patch-11')
        self.assertEqual(store.prepare_patch(raw(value)).code, 'MESSAGE_ID_CONFLICT')

    def test_exact_base_version_and_original_snapshot_message_required(self):
        store = self.initialized()
        for value in (command(base=9), command(base_mid='wrong-base'), command(base=0, op='add', device_id='meter-C')):
            self.assertEqual(store.prepare_patch(raw(value)).code, 'BASE_CONFIG_MISMATCH')
        self.assertEqual(store.load_active().config_version, 10)

    def test_operation_existence_and_total_merged_limits_are_rejected(self):
        store = self.initialized()
        self.assertEqual(store.prepare_patch(raw(command(op='add'))).code, 'DEVICE_EXISTS')
        for op in ('replace', 'remove'):
            self.assertEqual(store.prepare_patch(raw(command(op=op, device_id='missing'))).code, 'DEVICE_NOT_FOUND')
        value = command()
        point = value['operations'][0]['device']['points'][0]
        value['operations'][0]['device']['points'] = [dict(point, pointId=f'p-{i}') for i in range(2000)]
        self.assertEqual(store.prepare_patch(raw(value)).code, 'POINT_LIMIT_EXCEEDED')

    def test_bootstrap_gateway_options_and_removed_last_device_keep_configured_base(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            prepared = store.prepare_patch(raw(command(5, 0, op='add')))
            self.assertEqual(prepared.status, 'PREPARED')
            store.commit(prepared.token)
            value = command(8, 5, base_mid='patch-5', op='remove')
            value['gateway'] = dict(enabled=False, report=dict(batchIntervalMs=2000, maxBatchPoints=10))
            store.commit(store.prepare_patch(raw(value)).token)
            active = store.load_active()
            self.assertEqual(active.devices, ())
            self.assertFalse(active.enabled)
            self.assertEqual(active.report.batch_interval_ms, 2000)
            self.assertEqual(store.prepare_patch(raw(command(9, 0, op='add'))).code, 'BASE_CONFIG_MISMATCH')

    def test_gateway_only_bootstrap_and_toggle_preserve_complete_devices(self):
        with ConfigStore(self.path, 'PLC-01') as store:
            first = command(1, 0); first['operations'] = []
            store.commit(store.prepare_patch(raw(first)).token)
            self.assertEqual(store.load_active().devices, ())
            add = command(3, 1, base_mid='patch-1', op='add')
            store.commit(store.prepare_patch(raw(add)).token)
            devices = store.load_active().devices
            toggle = command(5, 3, base_mid='patch-3'); toggle['operations'] = []
            toggle['gateway'] = dict(enabled=False)
            store.commit(store.prepare_patch(raw(toggle)).token)
            self.assertFalse(store.load_active().enabled)
            self.assertEqual(store.load_active().devices, devices)

    def test_interrupted_retry_same_wire_reuses_command_identity_but_new_token(self):
        store = self.initialized()
        payload = raw(command())
        first = store.prepare_patch(payload)
        self.assertEqual(store.prepare_patch(payload).token, first.token)
        store.close()
        with ConfigStore(self.path, 'PLC-01') as reopened:
            retry = reopened.prepare_patch(payload)
            self.assertEqual(retry.status, 'PREPARED')
            self.assertNotEqual(first.token, retry.token)
            reopened.commit(retry.token)
            self.assertEqual(reopened.prepare_patch(payload).status, 'APPLIED')

    def test_safe_legacy_nullable_column_upgrade_preserves_payload_and_full_replay(self):
        store = self.initialized()
        before = store.load_active().wire_json
        store.close()
        with closing(sqlite3.connect(self.path)) as conn:
            conn.execute('ALTER TABLE requests DROP COLUMN command_digest')
            conn.execute('ALTER TABLE requests DROP COLUMN command_kind')
            conn.commit()
        with ConfigStore(self.path, 'PLC-01') as reopened:
            self.assertEqual(reopened.load_active().wire_json, before)
            receipt = reopened._conn.execute('SELECT command_digest,command_kind FROM requests').fetchone()
            self.assertEqual(tuple(receipt), (None, None))
            self.assertEqual(reopened.prepare(raw(full())).status, 'APPLIED')
            self.assertEqual(reopened._conn.execute('PRAGMA user_version').fetchone()[0], 1)
            reopened.commit(reopened.prepare_patch(raw(command())).token)

    def test_corrupt_applied_receipt_fails_closed_and_limits_remain_bounded(self):
        store = self.initialized()
        payload = raw(command())
        store.commit(store.prepare_patch(payload).token)
        store._conn.execute("UPDATE snapshots SET state='FAILED' WHERE version=11")
        with self.assertRaises(StoreFormatError):
            store.prepare_patch(payload)

    def test_storage_capacity_rejects_patch_without_creating_partial_receipt(self):
        with ConfigStore(self.path, 'PLC-01', max_requests=1) as store:
            store.commit(store.prepare(raw(full())).token)
            before = store._conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0]
            self.assertEqual(store.prepare_patch(raw(command())).code, 'RESOURCE_LIMIT')
            self.assertEqual(store._conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0], before)
            self.assertEqual(store._conn.execute('SELECT COUNT(*) FROM requests').fetchone()[0], 1)

    def test_late_invalid_batch_operation_leaves_no_partial_receipt_or_snapshot(self):
        store = self.initialized()
        before_active = store.load_active()
        before_requests = [tuple(row) for row in store._conn.execute('SELECT * FROM requests ORDER BY message_id')]
        before_snapshots = [tuple(row) for row in store._conn.execute('SELECT * FROM snapshots ORDER BY version')]
        value = command()
        value['operations'].append(command(op='add', device_id='meter-C')['operations'][0])
        value['operations'].append(command(op='remove', device_id='missing')['operations'][0])
        result = store.prepare_patch(raw(value))
        self.assertEqual((result.status, result.code), ('REJECTED', 'DEVICE_NOT_FOUND'))
        self.assertEqual(store.load_active(), before_active)
        self.assertEqual([tuple(row) for row in store._conn.execute('SELECT * FROM requests ORDER BY message_id')],
                         before_requests)
        self.assertEqual([tuple(row) for row in store._conn.execute('SELECT * FROM snapshots ORDER BY version')],
                         before_snapshots)


class Runner:
    def __init__(self, device):
        self.device, self.starts, self.stops = device, 0, 0
        self.task = None
        self.fail_start = False
    async def start(self):
        self.starts += 1
        self.task = asyncio.create_task(asyncio.Event().wait())
        if self.fail_start:
            raise RuntimeError('driver refused')
    async def stop(self):
        self.stops += 1
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None


class PatchLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.settings = AgentSettings('PLC-01', self.directory)
        self.created = []
        self.fail_new = False
        def factory(device):
            runner = Runner(device); runner.fail_start = self.fail_new
            self.created.append(runner)
            return runner
        self.factory = factory
        self.runtime = DeviceRuntime(factory)
        self.agent = AgentLifecycle(self.settings, self.runtime)
        self.addAsyncCleanup(self.agent.close)
        await self.agent.start()
        await self.agent.submit(raw(full()))

    async def test_only_changed_device_runner_restarts_and_removed_device_stops(self):
        a, b = self.runtime.runners['meter-A'], self.runtime.runners['meter-B']
        result = await self.agent.submit(raw(command()), kind='configPatch')
        self.assertEqual((result.status, result.applied_config_version, result.active_message_id),
                         ('APPLIED', 11, 'patch-11'))
        self.assertIsNot(self.runtime.runners['meter-A'], a)
        self.assertIs(self.runtime.runners['meter-B'], b)
        self.assertEqual((b.starts, b.stops), (1, 0))
        remove = command(12, 11, base_mid='patch-11', op='remove')
        await self.agent.submit(raw(remove), kind='configPatch')
        self.assertNotIn('meter-A', self.runtime.runners)
        self.assertIs(self.runtime.runners['meter-B'], b)
        self.assertEqual((b.starts, b.stops), (1, 0))

    async def test_concurrent_same_base_commands_serialize_and_second_cannot_overwrite(self):
        first, second = await asyncio.gather(
            self.agent.submit(raw(command()), kind='configPatch'),
            self.agent.submit(raw(command(12, device_id='meter-B')), kind='configPatch'))
        self.assertEqual(first.status, 'APPLIED')
        self.assertEqual((second.status, second.code), ('REJECTED', 'BASE_CONFIG_MISMATCH'))
        self.assertEqual(self.agent.active_config_version, 11)

    async def test_partial_driver_failure_rolls_back_and_leaves_peer_runner_untouched(self):
        a, b = self.runtime.runners['meter-A'], self.runtime.runners['meter-B']
        self.fail_new = True
        result = await self.agent.submit(raw(command()), kind='configPatch')
        self.assertEqual(result.status, 'FAILED')
        self.assertIsNone(result.applied_config_version)
        self.assertEqual((result.active_config_version, result.active_message_id), (10, 'full-10'))
        self.assertIs(self.runtime.runners['meter-A'], a)
        self.assertIs(self.runtime.runners['meter-B'], b)
        self.assertEqual((b.starts, b.stops), (1, 0))

    async def test_commit_failure_preserves_base_and_does_not_emit_applied(self):
        with closing(sqlite3.connect(self.directory/'agent.db')) as conn:
            conn.execute("""CREATE TRIGGER refuse_activation BEFORE UPDATE OF active_version ON metadata
                            BEGIN SELECT RAISE(ABORT,'write refused'); END""")
            conn.commit()
        result = await self.agent.submit(raw(command()), kind='configPatch')
        self.assertEqual(result.code, 'STORAGE_ERROR')
        self.assertIsNone(result.applied_config_version)
        self.assertEqual(self.agent.active_message_id, 'full-10')

    async def test_batch_commit_failure_restores_all_changed_runners_and_exact_retry_keeps_peer(self):
        initial = full(11)
        peer_config = copy.deepcopy(initial['config']['devices'][0])
        peer_config.update(deviceId='meter-C', name='Unselected meter C')
        initial['config']['devices'].append(peer_config)
        self.assertEqual((await self.agent.submit(raw(initial))).status, 'APPLIED')
        before = self.runtime.snapshot
        old_a, old_b, peer = (self.runtime.runners[key] for key in ('meter-A', 'meter-B', 'meter-C'))
        value = command(12, 11)
        value['operations'].append(command(12, 11, device_id='meter-B')['operations'][0])
        payload = raw(value)
        with closing(sqlite3.connect(self.directory/'agent.db')) as conn:
            conn.execute("""CREATE TRIGGER refuse_batch_activation BEFORE UPDATE OF active_version ON metadata
                            BEGIN SELECT RAISE(ABORT,'external commit fault'); END""")
            conn.commit()
        try:
            result = await self.agent.submit(payload, kind='configPatch')
            self.assertEqual((result.status, result.code, result.applied_config_version),
                             ('FAILED', 'STORAGE_ERROR', None))
            self.assertEqual((result.active_config_version, result.active_message_id), (11, 'full-11'))
            self.assertEqual(self.agent.state, 'RUNNING')
            self.assertIs(self.runtime.snapshot, before)
            self.assertIs(self.runtime.runners['meter-A'], old_a)
            self.assertIs(self.runtime.runners['meter-B'], old_b)
            self.assertIs(self.runtime.runners['meter-C'], peer)
            self.assertEqual((old_a.starts, old_a.stops, old_b.starts, old_b.stops), (2, 2, 2, 2))
            self.assertEqual((peer.starts, peer.stops), (1, 0))
            self.assertTrue(all(runner.task is None for runner in self.created[-2:]))
            stored = await self.agent._storage.call('load_active')
            self.assertEqual(stored.content_hash, before.content_hash)
            with closing(sqlite3.connect(self.directory/'agent.db')) as conn:
                self.assertEqual(conn.execute('SELECT active_version FROM metadata').fetchone()[0], 11)
                self.assertEqual(conn.execute('SELECT status FROM requests WHERE message_id=?',
                                              (value['messageId'],)).fetchone()[0], 'FAILED')
        finally:
            with closing(sqlite3.connect(self.directory/'agent.db')) as conn:
                conn.execute('DROP TRIGGER refuse_batch_activation')
                conn.commit()
        result = await self.agent.submit(payload, kind='configPatch')
        self.assertEqual((result.status, result.applied_config_version, result.active_message_id),
                         ('APPLIED', 12, 'patch-12'))
        self.assertIsNot(self.runtime.runners['meter-A'], old_a)
        self.assertIsNot(self.runtime.runners['meter-B'], old_b)
        self.assertIs(self.runtime.runners['meter-C'], peer)
        self.assertEqual((peer.starts, peer.stops), (1, 0))
        self.assertEqual(next(d for d in self.runtime.snapshot.devices if d.device_id == 'meter-C'),
                         next(d for d in before.devices if d.device_id == 'meter-C'))
        current = tuple(self.runtime.runners.items())
        self.assertEqual((await self.agent.submit(payload, kind='configPatch')).status, 'APPLIED')
        self.assertEqual(tuple(self.runtime.runners.items()), current)
        with closing(sqlite3.connect(self.directory/'agent.db')) as conn:
            self.assertEqual(conn.execute('SELECT status FROM requests WHERE message_id=?',
                                          (value['messageId'],)).fetchone()[0], 'APPLIED')

    async def test_historical_retry_after_restart_is_applied_with_current_runtime_unmodified(self):
        payload = raw(command())
        await self.agent.submit(payload, kind='configPatch')
        await self.agent.submit(raw(command(15, 11, base_mid='patch-11', op='remove')), kind='configPatch')
        await self.agent.close()
        runtime = DeviceRuntime(self.factory)
        agent = AgentLifecycle(self.settings, runtime)
        self.addAsyncCleanup(agent.close)
        await agent.start()
        before = tuple(runtime.runners.items())
        result = await agent.submit(payload, kind='configPatch')
        self.assertEqual((result.status, result.applied_config_version, result.active_config_version,
                          result.active_message_id), ('APPLIED', 11, 15, 'patch-15'))
        self.assertEqual(tuple(runtime.runners.items()), before)

    async def test_retained_old_full_cannot_roll_back_new_patch(self):
        await self.agent.submit(raw(command()), kind='configPatch')
        result = await self.agent.submit(raw(full()))
        self.assertEqual((result.status, result.code), ('REJECTED', 'STALE_VERSION'))
        self.assertEqual(self.agent.active_message_id, 'patch-11')

    async def test_lost_commit_notification_recovers_exact_identity_and_can_replay(self):
        await self.agent.close()
        class CommitThenRaise(ConfigStore):
            def commit(self, token):
                super().commit(token)
                raise StoreError('notification lost')
        agent = AgentLifecycle(self.settings, DeviceRuntime(self.factory), store_factory=CommitThenRaise)
        self.addAsyncCleanup(agent.close)
        await agent.start()
        payload = raw(command())
        result = await agent.submit(payload, kind='configPatch')
        self.assertEqual((result.status, result.applied_config_version, result.active_message_id),
                         ('APPLIED', 11, 'patch-11'))
        self.assertEqual((await agent.submit(payload, kind='configPatch')).status, 'APPLIED')

    async def test_second_changed_runner_failure_rolls_back_whole_batch_and_keeps_peer(self):
        initial = full(11)
        peer_config = copy.deepcopy(initial['config']['devices'][0])
        peer_config.update(deviceId='meter-C', name='Unselected meter C')
        initial['config']['devices'].append(peer_config)
        self.assertEqual((await self.agent.submit(raw(initial))).status, 'APPLIED')
        before = self.runtime.snapshot
        old_a, old_b, peer = (self.runtime.runners[key] for key in ('meter-A', 'meter-B', 'meter-C'))
        def fail_second(device):
            runner = self.factory(device)
            runner.fail_start = device.device_id == 'meter-B'
            return runner
        self.runtime._factory = fail_second
        value = command(12, 11)
        value['operations'].append(command(12, 11, device_id='meter-B')['operations'][0])
        result = await self.agent.submit(raw(value), kind='configPatch')
        self.assertEqual(result.status, 'FAILED')
        self.assertIsNone(result.applied_config_version)
        self.assertEqual((result.active_config_version, result.active_message_id), (11, 'full-11'))
        self.assertIs(self.runtime.snapshot, before)
        self.assertIs(self.runtime.runners['meter-A'], old_a)
        self.assertIs(self.runtime.runners['meter-B'], old_b)
        self.assertIs(self.runtime.runners['meter-C'], peer)
        self.assertEqual((peer.starts, peer.stops), (1, 0))
        self.assertEqual((old_a.starts, old_b.starts), (2, 2))
        self.assertFalse(old_a.task.done())
        self.assertFalse(old_b.task.done())
        self.assertTrue(all(runner.task is None for runner in self.created[-2:]))


class PatchMqttTests(unittest.IsolatedAsyncioTestCase):
    async def test_patch_uses_existing_ingress_actor_and_separate_ack_then_get(self):
        await self.exercise(False)

    async def test_retained_patch_is_rejected_without_submitting_to_actor(self):
        await self.exercise(True)

    async def exercise(self, retained):
        agent = SimpleNamespace(settings=SimpleNamespace(gateway_id='PLC-01'), state='WAITING_CONFIG',
                                active_config_version=0, active_message_id=None,
                                runtime=SimpleNamespace(boot_context=None))
        calls, published, subscriptions = [], [], []
        async def submit(payload, *, kind='configSet'):
            calls.append((payload, kind))
            agent.active_config_version, agent.active_message_id, agent.state = 11, 'patch-11', 'RUNNING'
            return ApplyResult('APPLIED', 11, applied_config_version=11, active_message_id='patch-11')
        agent.submit = submit
        payload = raw(command(base=0, op='add'))
        service = MqttConfigService(agent, object())
        class Connection:
            connected, dropped = True, 0
            def __init__(self, settings, client_id, topics, **kwargs):
                subscriptions.extend(topics)
                self.sent = False
            async def open(self): pass
            async def close(self): pass
            async def publish(self, topic, data, **kwargs):
                published.append((topic, json.loads(data), kwargs))
                if topic.endswith('config/get') and len([p for p in published if p[0].endswith('config/get')]) == 2:
                    service._stop.set()
            async def receive(self):
                if not self.sent:
                    self.sent = True
                    return Incoming(service.prefix+'config/patch', payload, retained)
                await asyncio.Event().wait()
        async def no_health(*args):
            await asyncio.Event().wait()
        with patch('plcnext_iot.messaging.config_service.PahoConnection', Connection), \
                patch.object(service.health, 'run', no_health):
            await asyncio.wait_for(service._run(), 2)
        self.assertIn(service.prefix+'config/patch', subscriptions)
        self.assertEqual(len(calls), 0 if retained else 1)
        if calls:
            self.assertEqual(calls[0][1], 'configPatch')
        acks = [p for p in published if p[0].endswith('config/patch/ack')]
        self.assertEqual(len(acks), 1)
        self.assertEqual(acks[0][1]['status'], 'REJECTED' if retained else 'APPLIED')
        self.assertFalse(acks[0][2].get('retain', False))
        self.assertFalse(validate_message('configPatchAck', raw(acks[0][1])))
        last_get = [p for p in published if p[0].endswith('config/get')][-1][1]
        self.assertEqual(last_get['configCapabilities'], ['devicePatchV1'])
        self.assertEqual(last_get['activeMessageId'], None if retained else 'patch-11')
        self.assertLess(next(i for i,p in enumerate(published) if p[0].endswith('config/patch/ack')),
                        next(i for i,p in enumerate(published) if p[1] is last_get))
