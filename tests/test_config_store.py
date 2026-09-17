"""Real SQLite transactions and subprocess exits protect last-good configuration."""
import copy
from contextlib import closing
from dataclasses import FrozenInstanceError
import importlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


class ConfigStoreTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((ROOT / "src/plcnext_iot/config/store.py").is_file(),
                        "Configuration store has not been implemented")
        self.models = importlib.import_module("plcnext_iot.config.models")
        self.module = importlib.import_module("plcnext_iot.config.store")
        self.Store = self.module.ConfigStore
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "agent.db"
        self.msg = json.loads((ROOT / "contracts/examples/valid/config-set.json").read_text())

    def payload(self, version=10, message_id=None):
        msg = copy.deepcopy(self.msg)
        msg["configVersion"] = version
        msg["messageId"] = message_id or f"cfg-{version}"
        return json.dumps(msg).encode()

    def test_model_rejects_invalid_content_and_is_deeply_immutable(self):
        snapshot = self.models.ConfigSnapshot.parse(self.payload(), "PLC-01")
        self.assertEqual(snapshot.devices[0].points[0].point_id, "voltage")
        with self.assertRaises(FrozenInstanceError):
            snapshot.devices[0].enabled = False
        with self.assertRaises(FrozenInstanceError):
            snapshot.devices[0].points[0].modbus.address = 8
        self.msg["config"]["devices"][0]["points"][0]["modbus"]["address"] = -1
        with self.assertRaises(self.models.ConfigValidationError):
            self.models.ConfigSnapshot.parse(self.payload(), "PLC-01")

    def test_business_content_ignores_envelope_order_and_numeric_spelling(self):
        a = self.models.ConfigSnapshot.parse(self.payload(), "PLC-01")
        self.msg["timestamp"] += 1000
        self.msg["config"]["devices"][0]["points"][0]["offset"] = 0.0
        b = self.models.ConfigSnapshot.parse(self.payload(11), "PLC-01")
        self.assertEqual(a.content_json, b.content_json)
        d = copy.deepcopy(self.msg["config"]["devices"][0])
        d["deviceId"] = "a-first"
        self.msg["config"]["devices"].append(d)
        a = self.models.ConfigSnapshot.parse(self.payload(), "PLC-01")
        self.msg["config"]["devices"].reverse()
        b = self.models.ConfigSnapshot.parse(self.payload(), "PLC-01")
        self.assertEqual(a.content_json, b.content_json)

    def test_large_finite_numbers_do_not_make_stored_payload_unreadable(self):
        point = copy.deepcopy(self.msg["config"]["devices"][0]["points"][0])
        point.update(scale=1e308, offset=1e308, deadband=1e308)
        self.msg["config"]["devices"][0]["points"] = [dict(point, pointId=f"p-{i}") for i in range(2000)]
        with self.Store(self.path, "PLC-01") as store:
            candidate = store.prepare(self.payload())
            self.assertEqual(candidate.status, "PREPARED")
            store.commit(candidate.token)
        with self.Store(self.path, "PLC-01") as store:
            self.assertEqual(len(store.load_active().devices[0].points), 2000)

    def test_prepare_does_not_activate_and_commit_survives_reopen(self):
        with self.Store(self.path, "PLC-01") as store:
            self.assertIsNone(store.load_active())
            result = store.prepare(self.payload())
            self.assertEqual(result.status, "PREPARED")
            self.assertIsNone(store.load_active())
            store.commit(result.token)
            self.assertEqual(store.load_active().config_version, 10)
        with self.Store(self.path, "PLC-01") as store:
            self.assertEqual(store.load_active().config_version, 10)
            self.assertEqual(store.previous_version, 0)

    def test_committed_duplicate_replies_without_another_candidate(self):
        with self.Store(self.path, "PLC-01") as store:
            store.commit(store.prepare(self.payload()).token)
            for mid in ("cfg-10", "retry-other-id"):
                result = store.prepare(self.payload(message_id=mid))
                self.assertEqual(result.status, "APPLIED")
                self.assertIsNone(result.token)
                self.assertEqual(result.active_config_version, 10)

    def test_invalid_stale_and_conflicting_config_preserve_active(self):
        with self.Store(self.path, "PLC-01") as store:
            store.commit(store.prepare(self.payload()).token)
            self.assertEqual(store.prepare(self.payload(9)).code, "STALE_VERSION")
            self.msg["config"]["enabled"] = False
            self.assertEqual(store.prepare(self.payload(message_id="changed")).code, "VERSION_CONFLICT")
            self.msg["config"]["devices"][0]["points"][0]["pollIntervalMs"] = 0
            self.assertEqual(store.prepare(self.payload(11)).status, "REJECTED")
            self.assertEqual(store.load_active().config_version, 10)

    def test_pending_duplicate_reuses_handle_and_other_candidate_is_busy(self):
        with self.Store(self.path, "PLC-01") as store:
            result = store.prepare(self.payload())
            self.assertEqual(store.prepare(self.payload()).token, result.token)
            self.assertEqual(store.prepare(self.payload(11)).code, "RESOURCE_LIMIT")
            store.fail(result.token)
            retry = store.prepare(self.payload())
            self.assertNotEqual(result.token, retry.token)
            with self.assertRaises(self.module.InvalidCandidate):
                store.commit(result.token)
            store.commit(retry.token)

    def test_failed_version_cannot_be_reused_for_different_content(self):
        with self.Store(self.path, "PLC-01") as store:
            result = store.prepare(self.payload())
            store.fail(result.token)
            self.msg["config"]["enabled"] = False
            self.assertEqual(store.prepare(self.payload()).code, "VERSION_CONFLICT")

    def test_message_id_cannot_be_rebound_to_another_version(self):
        with self.Store(self.path, "PLC-01") as store:
            store.commit(store.prepare(self.payload(message_id="same-id")).token)
            self.assertEqual(store.prepare(self.payload(11, "same-id")).code, "MESSAGE_ID_CONFLICT")

    def test_reopen_interrupts_candidate_and_invalidates_old_token(self):
        with self.Store(self.path, "PLC-01") as store:
            store.commit(store.prepare(self.payload()).token)
            old = store.prepare(self.payload(11))
        with self.Store(self.path, "PLC-01") as store:
            self.assertEqual(store.load_active().config_version, 10)
            with self.assertRaises(self.module.InvalidCandidate):
                store.commit(old.token)
            new = store.prepare(self.payload(11))
            self.assertNotEqual(new.token, old.token)
            store.commit(new.token)
            self.assertEqual(store.previous_version, 10)

    def test_gateway_identity_and_single_owner_are_enforced(self):
        with self.Store(self.path, "PLC-01"):
            with self.assertRaises(self.module.StoreInUse):
                self.Store(self.path, "PLC-01")
        with self.assertRaises(self.module.StoreIdentityError):
            self.Store(self.path, "PLC-02")
        with self.Store(self.path, "PLC-01") as store:
            bad = json.loads(self.payload())
            bad["gatewayId"] = "PLC-02"
            self.assertEqual(store.prepare(json.dumps(bad).encode()).code, "GATEWAY_MISMATCH")

    def test_unknown_database_schema_not_modified(self):
        with closing(sqlite3.connect(self.path)) as conn:
            conn.execute("CREATE TABLE unrelated (value TEXT)")
        with self.assertRaises(self.module.StoreFormatError):
            self.Store(self.path, "PLC-01")
        with closing(sqlite3.connect(self.path)) as conn:
            self.assertEqual(conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),
                             [("unrelated",)])

    def test_commit_write_failure_rolls_back_all_state(self):
        with self.Store(self.path, "PLC-01") as store:
            store.commit(store.prepare(self.payload()).token)
            candidate = store.prepare(self.payload(11))
            with closing(sqlite3.connect(self.path)) as conn:
                conn.execute("""CREATE TRIGGER reject_pointer BEFORE UPDATE OF active_version ON metadata
                              BEGIN SELECT RAISE(ABORT, 'test write failure'); END""")
            with self.assertRaises(self.module.StoreError):
                store.commit(candidate.token)
            self.assertEqual(store.load_active().config_version, 10)
        with self.Store(self.path, "PLC-01") as store:
            self.assertEqual(store.load_active().config_version, 10)

    def test_crash_after_prepare_and_after_commit(self):
        script = """
import os,sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd()/'src'))
from plcnext_iot.config.store import ConfigStore
s=ConfigStore(Path(sys.argv[1]), 'PLC-01')
r=s.prepare(Path(sys.argv[2]).read_bytes())
if sys.argv[3]=='commit': s.commit(r.token)
os._exit(37)
"""
        path = Path(self.temp.name) / "input.json"
        path.write_bytes(self.payload())
        for action, active in [("prepare", None), ("commit", 10)]:
            with self.subTest(action=action):
                result = subprocess.run([sys.executable, "-c", script, str(self.path), str(path), action],
                                        cwd=ROOT, capture_output=True, text=True)
                self.assertEqual(result.returncode, 37, result.stderr)
                with self.Store(self.path, "PLC-01") as store:
                    current = store.load_active()
                    self.assertEqual(current.config_version if current else None, active)

    def test_active_duplicate_can_be_confirmed_while_next_version_is_pending(self):
        with self.Store(self.path, "PLC-01") as store:
            store.commit(store.prepare(self.payload()).token)
            next_version = store.prepare(self.payload(11))
            self.assertEqual(store.prepare(self.payload()).status, "APPLIED")
            self.assertEqual(store.prepare(self.payload(11)).token, next_version.token)

    def test_storage_limits_reject_new_history_but_allow_exact_retransmission(self):
        with self.Store(self.path, "PLC-01", max_snapshots=1, max_requests=1) as store:
            store.commit(store.prepare(self.payload()).token)
            self.assertEqual(store.prepare(self.payload()).status, "APPLIED")
            self.assertEqual(store.prepare(self.payload(11)).code, "RESOURCE_LIMIT")
            self.assertEqual(store.load_active().config_version, 10)

    def test_committed_content_corruption_is_not_silently_recovered(self):
        with self.Store(self.path, "PLC-01") as store:
            store.commit(store.prepare(self.payload()).token)
        with closing(sqlite3.connect(self.path)) as conn:
            with conn:
                conn.execute("UPDATE snapshots SET content_hash='corrupt'")
        with self.assertRaises(self.module.StoreFormatError):
            self.Store(self.path, "PLC-01")

    def test_demo_explains_persistence_only_and_recovers_committed_version(self):
        result = subprocess.run([sys.executable, "-m", "tools.config_demo"], cwd=ROOT,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"recoveredVersion": 10', result.stdout)
        self.assertIn('"deviceActivationPerformed": false', result.stdout)


if __name__ == "__main__":
    unittest.main()
