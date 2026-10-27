"""Run startup/clock/Outbox regressions with all SQLite connections forbidden.

Network fixtures are owned loopback sockets or injected transports. No real Hub,
broker, mock or PLC access, no new databases and no background service launch.
"""
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'tests')]
MODULES = ('test_modbus_pool', 'test_phase1_collectors', 'test_runtime_health',
           'test_platform_time', 'test_timed_outbox', 'test_platform_mqtt')


def main():
    with patch.object(sqlite3, 'connect', side_effect=AssertionError('SQLite is forbidden in this regression entry')):
        suite = unittest.defaultTestLoader.loadTestsFromNames(MODULES)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
