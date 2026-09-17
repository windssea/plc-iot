"""Demonstrate SQLite config persistence only; never activates field devices."""
import json
from pathlib import Path
import tempfile

from tools import _source_path  # noqa: F401
from plcnext_iot.config.store import ConfigStore


def main():
    root = Path(__file__).resolve().parents[1]
    message = json.loads((root / "contracts/examples/valid/config-set.json").read_text(encoding="utf-8"))
    with tempfile.TemporaryDirectory(prefix="plcnext-config-demo-") as directory:
        path = Path(directory) / "agent.db"
        with ConfigStore(path, "PLC-01") as store:
            candidate = store.prepare(json.dumps(message).encode("utf-8"))
            prepared_active = store.load_active()
            store.commit(candidate.token)  # Storage exercise, not a real runtime APPLIED.
            message.update(configVersion=11, messageId="demo-v11")
            store.prepare(json.dumps(message).encode("utf-8"))
        with ConfigStore(path, "PLC-01") as store:
            recovered = store.load_active()
            print(json.dumps({"mode": "persistence-demo",
                              "deviceActivationPerformed": False,
                              "activeAfterPrepare": prepared_active,
                              "committedVersion": 10,
                              "interruptedCandidateVersion": 11,
                              "recoveredVersion": recovered.config_version}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
