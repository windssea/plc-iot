"""Offline reference validator; no MQTT, configuration state or Hub dependency.

The versioned JSON Schema owns structural rules. This module adds wire decoding
and cross-field invariants not expressible in portable JSON Schema.
"""
from dataclasses import dataclass
from functools import lru_cache
from itertools import islice
import json
import math
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry

CONTRACT_DIR = Path(__file__).resolve().parents[2] / "contracts"
MAX_ISSUES = 32


@dataclass(frozen=True)
class Issue:
    code: str
    path: str
    message: str


@lru_cache(maxsize=1)
def topic_rules() -> dict:
    data = json.loads((CONTRACT_DIR / "v1/topics.json").read_text(encoding="utf-8"))
    return {item["kind"]: item for item in data["topics"]}


@lru_cache(maxsize=9)
def _validator(kind: str) -> Draft202012Validator:
    schema = json.loads((CONTRACT_DIR / "v1/protocol.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    # References remain local. Registry's default retrieval refuses network I/O.
    entry = {"$schema": schema["$schema"], "$defs": schema["$defs"],
             "$ref": "#/$defs/" + kind}
    return Draft202012Validator(entry, registry=Registry())


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def _float(text):
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("Non-finite JSON number")
    return value


def _constant(_text):
    raise ValueError("Non-JSON constant")


def _check_depth(message):
    pending = [(message, 0)]
    while pending:
        item, depth = pending.pop()
        if depth > 64:
            raise ValueError("JSON nesting exceeds the contract limit")
        if isinstance(item, dict):
            pending.extend((value, depth + 1) for value in item.values())
        elif isinstance(item, list):
            pending.extend((value, depth + 1) for value in item)


def _pointer(parts):
    return "".join("/" + str(part).replace("~", "~0").replace("/", "~1") for part in parts)


def validate_message(kind: str, payload: bytes, *,
                     expected_gateway_id: str | None = None) -> list[Issue]:
    """Return bounded, sanitized issues. This does not apply/persist any message."""
    if kind not in topic_rules():
        return [Issue("UNSUPPORTED_MESSAGE", "", "Message kind is not supported.")]
    if len(payload) > topic_rules()[kind]["maxPayloadBytes"]:
        return [Issue("PAYLOAD_TOO_LARGE", "", "Payload exceeds the byte limit.")]
    try:
        message = json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs,
                             parse_float=_float, parse_constant=_constant)
        _check_depth(message)
    except (UnicodeError, ValueError, RecursionError):
        return [Issue("INVALID_JSON", "", "Expected finite, unique-key UTF-8 JSON.")]

    # Never expose jsonschema's raw error.message: it may contain input secrets.
    issues = [Issue("SCHEMA_INVALID", _pointer(error.absolute_path),
                    f"Schema constraint failed: {error.validator}.")
              for error in islice(_validator(kind).iter_errors(message), MAX_ISSUES)]
    if issues:
        return issues
    if expected_gateway_id is not None and message["gatewayId"] != expected_gateway_id:
        return [Issue("GATEWAY_MISMATCH", "/gatewayId", "Gateway differs from the trusted context.")]
    return _semantic_issues(kind, message)[:MAX_ISSUES]


def _semantic_issues(kind: str, message: dict) -> list[Issue]:
    if kind == "configSet":
        return _config_issues(message["config"])
    if kind == "configAck" and message["status"] == "APPLIED":
        if message["configVersion"] != message["activeConfigVersion"]:
            return [Issue("ACK_VERSION_MISMATCH", "/activeConfigVersion",
                          "APPLIED must report the requested version.")]
    if kind == "data":
        seen = set()
        for index, value in enumerate(message["values"]):
            key = (value["deviceId"], value["pointId"])
            if key in seen:
                return [Issue("DUPLICATE_VALUE", f"/values/{index}",
                              "A batch contains a repeated device/point identity.")]
            seen.add(key)
    if kind == "heartbeat":
        return _heartbeat_issues(message)
    return []


def _config_issues(config: dict) -> list[Issue]:
    issues = []
    devices = set()
    total = 0
    for di, device in enumerate(config["devices"]):
        dp = f"/config/devices/{di}"
        if device["deviceId"] in devices:
            issues.append(Issue("DUPLICATE_DEVICE_ID", dp + "/deviceId",
                                "Child device identity must be unique within the PLC."))
        devices.add(device["deviceId"])
        points = set()
        node_ids = set()
        total += len(device["points"])
        for pi, point in enumerate(device["points"]):
            pp = f"{dp}/points/{pi}"
            if point["pointId"] in points:
                issues.append(Issue("DUPLICATE_POINT_ID", pp + "/pointId",
                                    "Point identity must be unique within its child device."))
            points.add(point["pointId"])
            if point["staleAfterMs"] < point["pollIntervalMs"]:
                issues.append(Issue("INVALID_STALE_INTERVAL", pp + "/staleAfterMs",
                                    "Stale interval cannot be shorter than polling interval."))
            if device["protocol"] == "modbus_tcp":
                width = 2 if point["dataType"] in ("int32", "uint32", "float32") else 1
                if point["modbus"]["address"] + width - 1 > 65535:
                    issues.append(Issue("INVALID_ADDRESS", pp + "/modbus/address",
                                        "Complete value width exceeds the address range."))
            elif device["protocol"] == "opcua":
                node_id = point["opcua"]["nodeId"]
                if node_id in node_ids:
                    issues.append(Issue("DUPLICATE_NODE_ID", pp + "/opcua/nodeId",
                                        "OPC UA node identity must be unique within its child device."))
                node_ids.add(node_id)
    if total > 2000:
        issues.append(Issue("POINT_LIMIT_EXCEEDED", "/config/devices",
                            "Total configured points exceed the PLC limit."))
    return issues


def _heartbeat_issues(message: dict) -> list[Issue]:
    issues = []
    counts = message["points"]
    if counts["good"] + counts["bad"] != counts["total"]:
        issues.append(Issue("INCONSISTENT_COUNTS", "/points", "Good plus bad must equal total."))
    waiting = message["agentState"] == "WAITING_CONFIG"
    if waiting != (message["activeConfigVersion"] == 0):
        issues.append(Issue("INCONSISTENT_STATE", "/agentState",
                            "Only an unconfigured agent can be WAITING_CONFIG."))
    if waiting and (message["devices"] or counts["total"]):
        issues.append(Issue("INCONSISTENT_STATE", "/devices",
                            "An unconfigured agent cannot report configured devices or points."))
    ids = [d["deviceId"] for d in message["devices"]]
    if len(set(ids)) != len(ids):
        issues.append(Issue("DUPLICATE_DEVICE_ID", "/devices", "Repeated device status identity."))
    return issues
