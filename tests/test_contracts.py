"""Exercise the wire contract, including failures unsafe to accept on a PLC."""
import copy
import importlib
import json
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ContractTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((ROOT / "contracts/v1/protocol.schema.json").is_file(),
                        "The versioned wire schema has not been implemented")
        self.assertTrue((ROOT / "tools/contract_validation.py").is_file(),
                        "The semantic validator has not been implemented")
        self.validate = importlib.import_module("tools.contract_validation").validate_message
        self.config = json.loads((ROOT / "contracts/examples/valid/config-set.json").read_text(encoding="utf-8"))

    def codes(self, kind, message, **kwargs):
        raw = message if isinstance(message, bytes) else json.dumps(message).encode("utf-8")
        return {issue.code for issue in self.validate(kind, raw, **kwargs)}

    def point(self, config=None):
        return (config or self.config)["config"]["devices"][0]["points"][0]

    def test_all_published_examples_match_expected_outcomes(self):
        manifest = json.loads((ROOT / "contracts/examples/manifest.json").read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(manifest), 15)
        for case in manifest:
            with self.subTest(file=case["file"]):
                raw = (ROOT / "contracts/examples" / case["file"]).read_bytes()
                codes = self.codes(case["kind"], raw)
                self.assertEqual(not codes, case["valid"])
                if not case["valid"]:
                    self.assertIn(case["code"], codes)

    def test_config_rejects_unknown_fields_at_every_object_boundary(self):
        for path in [(), ("config",), ("config", "report"),
                     ("config", "devices", 0), ("config", "devices", 0, "connection"),
                     ("config", "devices", 0, "points", 0),
                     ("config", "devices", 0, "points", 0, "modbus")]:
            with self.subTest(path=path):
                msg = copy.deepcopy(self.config)
                node = msg
                for key in path:
                    node = node[key]
                node["password"] = "must-not-be-logged"
                issues = self.validate("configSet", json.dumps(msg).encode())
                self.assertIn("SCHEMA_INVALID", {i.code for i in issues})
                self.assertNotIn("must-not-be-logged", str(issues))

    def test_ids_are_scoped_to_plc_and_child_device(self):
        device = copy.deepcopy(self.config["config"]["devices"][0])
        self.config["config"]["devices"].append(device)
        self.assertIn("DUPLICATE_DEVICE_ID", self.codes("configSet", self.config))
        device["deviceId"] = "meter-second"
        self.assertEqual(set(), self.codes("configSet", self.config))
        device["points"].append(copy.deepcopy(device["points"][0]))
        self.assertIn("DUPLICATE_POINT_ID", self.codes("configSet", self.config))

    def test_full_register_width_must_fit_address_space(self):
        p = self.point()
        p.update(dataType="float32")
        p["modbus"].update(address=65535, byteOrder="ABCD")
        self.assertIn("INVALID_ADDRESS", self.codes("configSet", self.config))
        p["modbus"]["address"] = 65534
        self.assertEqual(set(), self.codes("configSet", self.config))

    def test_stale_interval_cannot_precede_poll(self):
        self.point()["staleAfterMs"] = 999
        self.assertIn("INVALID_STALE_INTERVAL", self.codes("configSet", self.config))
        self.point()["staleAfterMs"] = 1000
        self.assertEqual(set(), self.codes("configSet", self.config))

    def test_disabled_points_are_still_validated(self):
        self.point()["enabled"] = False
        self.point()["pollIntervalMs"] = 0
        self.assertIn("SCHEMA_INVALID", self.codes("configSet", self.config))

    def test_opcua_examples_are_protocol_specific(self):
        opcua = json.loads((ROOT / "contracts/examples/valid/config-opcua.json").read_text(encoding="utf-8"))
        self.assertEqual(set(), self.codes("configSet", opcua))
        opcua["config"]["devices"][0]["protocol"] = "modbus_tcp"
        self.assertIn("SCHEMA_INVALID", self.codes("configSet", opcua))

    def test_types_and_byte_orders_match_modbus_area(self):
        for dtype, area, order, valid in [
            ("uint16", "holding_register", "AB", True),
            ("uint16", "coil", "AB", False),
            ("float32", "input_register", "CDAB", True),
            ("float32", "input_register", "AB", False),
            ("bool", "coil", None, True),
            ("bool", "holding_register", None, False),
            ("bool", "coil", "AB", False),
        ]:
            with self.subTest(dtype=dtype, area=area, order=order):
                msg = copy.deepcopy(self.config)
                p = self.point(msg)
                p.update(dataType=dtype, scale=1, offset=0, deadband=0)
                p["modbus"] = {"area": area, "address": 0}
                if order is not None:
                    p["modbus"]["byteOrder"] = order
                self.assertEqual(not self.codes("configSet", msg), valid)

    def test_total_points_limited_across_devices(self):
        p = copy.deepcopy(self.point())
        devices = []
        for d in range(2):
            device = copy.deepcopy(self.config["config"]["devices"][0])
            device["deviceId"] = f"dev-{d}"
            device["points"] = [dict(p, pointId=f"p-{i}") for i in range(1001)]
            devices.append(device)
        self.config["config"]["devices"] = devices
        self.assertIn("POINT_LIMIT_EXCEEDED", self.codes("configSet", self.config))

    def test_gateway_identity_matches_authenticated_context(self):
        self.assertIn("GATEWAY_MISMATCH", self.codes("configSet", self.config,
                                                     expected_gateway_id="another-plc"))

    def test_rejects_non_json_numbers_and_duplicate_keys(self):
        for raw in [b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}',
                    b'{"gatewayId":"one","gatewayId":"two"}']:
            with self.subTest(raw=raw):
                self.assertIn("INVALID_JSON", self.codes("configSet", raw))

    def test_rejects_invalid_utf8_and_deep_input_without_traceback(self):
        self.assertIn("INVALID_JSON", self.codes("configSet", b'\xff'))
        self.assertIn("INVALID_JSON", self.codes("configSet", b'[' * 2000 + b']' * 2000))

    def test_message_size_checked_before_json_decode(self):
        self.assertIn("PAYLOAD_TOO_LARGE", self.codes("configSet", b' ' * (2 * 1024 * 1024 + 1)))
        self.assertIn("PAYLOAD_TOO_LARGE", self.codes("data", b' ' * (128 * 1024 + 1)))

    def test_topic_injection_and_unsupported_schema_rejected(self):
        self.config["gatewayId"] = "plc/+/other"
        self.assertIn("SCHEMA_INVALID", self.codes("configSet", self.config))
        self.config["gatewayId"] = "PLC-01"
        self.config["schemaVersion"] = 2
        self.assertIn("SCHEMA_INVALID", self.codes("configSet", self.config))

    def test_identity_and_host_cannot_end_with_newline(self):
        for key in ("gatewayId", "messageId"):
            with self.subTest(key=key):
                msg = copy.deepcopy(self.config)
                msg[key] += "\n"
                self.assertIn("SCHEMA_INVALID", self.codes("configSet", msg))
        self.config["config"]["devices"][0]["connection"]["host"] += "\n"
        self.assertIn("SCHEMA_INVALID", self.codes("configSet", self.config))

    def test_telemetry_quality_value_and_point_uniqueness(self):
        msg = json.loads((ROOT / "contracts/examples/valid/data.json").read_text())
        p = msg["values"][0]
        p.update(quality="BAD_TIMEOUT", value=0)
        self.assertIn("SCHEMA_INVALID", self.codes("data", msg))
        p["value"] = None
        self.assertEqual(set(), self.codes("data", msg))
        p["quality"] = "GOOD"
        self.assertIn("SCHEMA_INVALID", self.codes("data", msg))
        p["value"] = 220.6
        msg["values"].append(copy.deepcopy(p))
        self.assertIn("DUPLICATE_VALUE", self.codes("data", msg))

    def test_applied_ack_must_report_requested_version(self):
        msg = json.loads((ROOT / "contracts/examples/valid/config-ack.json").read_text())
        msg["activeConfigVersion"] = msg["configVersion"] - 1
        self.assertIn("ACK_VERSION_MISMATCH", self.codes("configAck", msg))

    def test_lwt_has_unknown_occurrence_time(self):
        msg = json.loads((ROOT / "contracts/examples/valid/status-lwt.json").read_text())
        self.assertEqual(set(), self.codes("status", msg))
        msg["timestamp"] = 1788748123123
        self.assertIn("SCHEMA_INVALID", self.codes("status", msg))

    def test_rejection_ack_requires_error_and_success_forbids_it(self):
        msg = json.loads((ROOT / "contracts/examples/valid/data-ack.json").read_text())
        msg["status"] = "REJECTED"
        self.assertIn("SCHEMA_INVALID", self.codes("dataAck", msg))
        msg["errors"] = [{"code": "UNKNOWN_POINT", "path": "/values/0/pointId",
                          "message": "Point is absent from published snapshot."}]
        self.assertEqual(set(), self.codes("dataAck", msg))
        msg["status"] = "STORED"
        self.assertIn("SCHEMA_INVALID", self.codes("dataAck", msg))

    def test_heartbeat_counts_and_waiting_state_are_consistent(self):
        msg = json.loads((ROOT / "contracts/examples/valid/heartbeat.json").read_text())
        msg["points"]["bad"] = 1
        self.assertIn("INCONSISTENT_COUNTS", self.codes("heartbeat", msg))
        msg["points"]["bad"] = 0
        msg["agentState"] = "WAITING_CONFIG"
        self.assertIn("INCONSISTENT_STATE", self.codes("heartbeat", msg))

    def test_schema_is_valid_and_topics_cover_all_message_examples(self):
        from jsonschema import Draft202012Validator
        schema = json.loads((ROOT / "contracts/v1/protocol.schema.json").read_text())
        Draft202012Validator.check_schema(schema)
        manifest = json.loads((ROOT / "contracts/examples/manifest.json").read_text())
        topics = json.loads((ROOT / "contracts/v1/topics.json").read_text())["topics"]
        self.assertEqual({t["kind"] for t in topics}, {c["kind"] for c in manifest if c["valid"]})
        for t in topics:
            self.assertIn(t["kind"], schema["$defs"])

    def test_unknown_message_kind_has_explicit_error(self):
        self.assertIn("UNSUPPORTED_MESSAGE", self.codes("commandSet", b'{}'))

    def test_cli_exit_codes_and_does_not_echo_bad_payload(self):
        for args, code in [([], 0), (["--kind", "configSet", "--file",
            "contracts/examples/invalid/unknown-field.json"], 1),
            (["--kind", "configSet", "--file", "does-not-exist.json"], 2)]:
            with self.subTest(args=args):
                result = subprocess.run([sys.executable, "-m", "tools.validate_contracts", *args],
                                        cwd=ROOT, capture_output=True, text=True)
                self.assertEqual(result.returncode, code, result.stdout + result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                self.assertNotIn("must-not-be-logged", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
