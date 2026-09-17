"""Local preflight: no broker connection, database migration or service mutation."""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import ssl
import sys
import tempfile

from tools import _source_path  # noqa: F401


def check(bootstrap, mqtt=None, *, require_systemd=False, release_root=None):
    results = []
    def record(name, ok):
        results.append(dict(check=name, ok=bool(ok)))
    record('python_3_12', sys.version_info[:2] == (3, 12))
    record('sqlite', sqlite3.sqlite_version_info >= (3, 24, 0))
    if require_systemd:
        record('systemd', Path('/run/systemd/system').is_dir() and shutil.which('systemctl'))
    for name, version in (('jsonschema','4.25.1'), ('pymodbus','3.13.1'), ('paho-mqtt','2.1.0')):
        try:
            ok = importlib.metadata.version(name) == version
        except importlib.metadata.PackageNotFoundError:
            ok = False
        record('dependency_'+name, ok)
    try:
        from plcnext_iot.core.settings import AgentSettings
        settings = AgentSettings.from_file(bootstrap)
        record('bootstrap', settings.gateway_id != 'REPLACE-PLC-ID')
        directory = settings.data_directory
        record('data_directory_exists', directory.is_dir())
        if directory.is_dir():
            # Probe a unique temporary file only; never open live SQLite files.
            try:
                with tempfile.TemporaryFile(dir=directory) as stream:
                    stream.write(b'probe')
                    stream.flush()
                    os.fsync(stream.fileno())
                writable = True
            except OSError:
                writable = False
            record('data_directory_writable', writable)
            record('free_space_256_mib', shutil.disk_usage(directory).free >= 256*1024*1024)
    except (ValueError, ImportError, OSError):
        record('bootstrap', False)
    if mqtt is not None:
        try:
            from plcnext_iot.messaging.settings import MqttSettings
            north = MqttSettings.from_file(mqtt)
            if north.tls:
                ssl.create_default_context(cafile=north.ca_file)
            record('mqtt_and_ca', True)
        except (ValueError, ImportError, OSError):
            record('mqtt_and_ca', False)
    if release_root is not None:
        root = Path(release_root).resolve()
        try:
            manifest = json.loads((root/'manifest.json').read_text())
            ok = bool(manifest['files'])
            for name, digest in manifest['files'].items():
                path = (root/name).resolve()
                if not path.is_relative_to(root) or path == root:
                    ok = False
                    break
                if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                    ok = False
                    break
            record('release_integrity', ok)
        except (OSError, ValueError, KeyError, TypeError):
            record('release_integrity', False)
    return dict(ok=all(item['ok'] for item in results), system=platform.system(),
                architecture=platform.machine(), checks=results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap', type=Path, required=True)
    parser.add_argument('--mqtt', type=Path)
    parser.add_argument('--require-systemd', action='store_true')
    parser.add_argument('--release-root', type=Path)
    args = parser.parse_args()
    result = check(args.bootstrap, args.mqtt, require_systemd=args.require_systemd, release_root=args.release_root)
    print(json.dumps(result))
    return 0 if result['ok'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
