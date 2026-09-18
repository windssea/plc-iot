"""Validated immutable models; no device connections or mutable shared mappings."""
from dataclasses import dataclass
import hashlib
import json

from plcnext_iot.contracts import Issue, validate_message


class ConfigValidationError(ValueError):
    def __init__(self, issues: list[Issue]):
        self.issues = tuple(issues)
        super().__init__("Configuration failed contract validation.")


@dataclass(frozen=True)
class ModbusAddress:
    area: str
    address: int
    byte_order: str | None


@dataclass(frozen=True)
class OpcUaAddress:
    node_id: str


@dataclass(frozen=True)
class PointConfig:
    point_id: str
    name: str
    enabled: bool
    data_type: str
    poll_interval_ms: int
    scale: int | float
    offset: int | float
    unit: str | None
    report_mode: str
    report_interval_ms: int
    deadband: int | float
    stale_after_ms: int
    modbus: ModbusAddress | None
    opcua: OpcUaAddress | None = None


@dataclass(frozen=True)
class ConnectionConfig:
    host: str
    port: int
    unit_id: int | None
    connect_timeout_ms: int
    request_timeout_ms: int
    retry_count: int
    path: str | None = None
    security_policy: str | None = None
    security_mode: str | None = None
    username: str | None = None
    password_env: str | None = None


@dataclass(frozen=True)
class DeviceConfig:
    device_id: str
    name: str
    enabled: bool
    protocol: str
    connection: ConnectionConfig
    points: tuple[PointConfig, ...]


@dataclass(frozen=True)
class ReportConfig:
    batch_interval_ms: int
    max_batch_points: int


def _normalize(value):
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _point(p, protocol):
    common = (p["pointId"], p["name"], p["enabled"], p["dataType"], p["pollIntervalMs"],
              p["scale"], p["offset"], p.get("unit"), p["reportMode"], p["reportIntervalMs"],
              p["deadband"], p["staleAfterMs"])
    if protocol == "opcua":
        return PointConfig(*common, None, OpcUaAddress(p["opcua"]["nodeId"]))
    return PointConfig(*common, ModbusAddress(
        p["modbus"]["area"], p["modbus"]["address"], p["modbus"].get("byteOrder")))


def _connection(c, protocol):
    timeouts = (c["connectTimeoutMs"], c["requestTimeoutMs"], c["retryCount"])
    if protocol == "opcua":
        return ConnectionConfig(c["host"], c["port"], None, *timeouts, c["path"],
                                c["securityPolicy"], c["securityMode"],
                                c.get("username"), c.get("passwordEnv"))
    return ConnectionConfig(c["host"], c["port"], c["unitId"], *timeouts)


def _device(d):
    return DeviceConfig(d["deviceId"], d["name"], d["enabled"], d["protocol"],
                        _connection(d["connection"], d["protocol"]),
                        tuple(_point(p, d["protocol"]) for p in d["points"]))


@dataclass(frozen=True)
class ConfigSnapshot:
    gateway_id: str
    message_id: str
    config_version: int
    timestamp: int
    enabled: bool
    report: ReportConfig
    devices: tuple[DeviceConfig, ...]
    content_json: str
    wire_json: str

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.content_json.encode("utf-8")).hexdigest()

    @classmethod
    def parse(cls, payload: bytes, gateway_id: str) -> "ConfigSnapshot":
        issues = validate_message("configSet", payload, expected_gateway_id=gateway_id)
        if issues:
            raise ConfigValidationError(issues)
        wire = _normalize(json.loads(payload.decode("utf-8")))
        config = wire["config"]
        config["devices"].sort(key=lambda d: d["deviceId"])
        for device in config["devices"]:
            device["points"].sort(key=lambda p: p["pointId"])
        return cls(wire["gatewayId"], wire["messageId"], wire["configVersion"],
                   wire["timestamp"], config["enabled"],
                   ReportConfig(config["report"]["batchIntervalMs"], config["report"]["maxBatchPoints"]),
                   tuple(_device(d) for d in config["devices"]), _json(config), payload.decode("utf-8"))
