"""Minimal local bootstrap for the core; northbound credentials are not added yet."""
from dataclasses import dataclass
import json
import math
from pathlib import Path
import re


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate bootstrap field.")
        value[key] = item
    return value


@dataclass(frozen=True)
class AgentSettings:
    gateway_id: str
    data_directory: Path
    operation_timeout_seconds: float = 5.0
    queue_capacity: int = 8

    def __post_init__(self):
        if not isinstance(self.gateway_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", self.gateway_id):
            raise ValueError("Invalid gateway identity.")
        if (type(self.operation_timeout_seconds) not in (int, float)
                or not math.isfinite(self.operation_timeout_seconds)
                or not 0 < self.operation_timeout_seconds <= 300):
            raise ValueError("Operation timeout must be finite and within (0,300] seconds.")
        if type(self.queue_capacity) is not int or not 1 <= self.queue_capacity <= 16:
            raise ValueError("Queue capacity must be an integer within [1,16].")
        object.__setattr__(self, "data_directory", Path(self.data_directory).resolve())

    @property
    def database_path(self):
        return self.data_directory / "agent.db"

    @classmethod
    def from_file(cls, path: Path):
        path = Path(path).resolve()
        try:
            with path.open("rb") as stream:
                payload = stream.read(16385)
            if len(payload) > 16384:
                raise ValueError("Bootstrap is too large.")
            value = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique)
            fields = {"gatewayId", "dataDirectory", "operationTimeoutSeconds", "queueCapacity"}
            if not isinstance(value, dict) or set(value) != fields:
                raise ValueError("Bootstrap must contain exactly the four documented fields.")
            if not isinstance(value["dataDirectory"], str) or not value["dataDirectory"].strip():
                raise ValueError("Data directory is required.")
            return cls(value["gatewayId"], path.parent / value["dataDirectory"],
                       value["operationTimeoutSeconds"], value["queueCapacity"])
        except (OSError, UnicodeError, ValueError, TypeError, RecursionError):
            raise ValueError("Bootstrap could not be read or failed validation.") from None
