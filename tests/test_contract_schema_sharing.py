"""Concurrent cold validators share one loaded/checked protocol, offline."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from plcnext_iot import contracts


class SharedSchemaTests(unittest.TestCase):
    def test_concurrent_distinct_kinds_load_check_once_and_keep_independent_results(self):
        cases = [('configSet', 'config-set.json'), ('configPatch', 'config-patch.json'),
                 ('configPatchAck', 'config-patch-ack.json'), ('configGet', 'config-get.json')]
        payloads = [(kind, (ROOT/'contracts/examples/valid'/name).read_bytes()) for kind,name in cases]
        with contracts._SCHEMA_LOCK:
            contracts._validator.cache_clear()
            contracts._schema.cache_clear()
        self.addCleanup(contracts._validator.cache_clear)
        self.addCleanup(contracts._schema.cache_clear)
        barrier = threading.Barrier(len(cases))
        reads = []
        original_read = Path.read_text
        original_check = contracts.Draft202012Validator.check_schema
        def read(path, *args, **kwargs):
            if path.name == 'protocol.schema.json':
                reads.append(path)
            return original_read(path, *args, **kwargs)
        def validate(case):
            kind, payload = case
            barrier.wait(timeout=3)
            issues = contracts.validate_message(kind, payload, expected_gateway_id='PLC-01')
            return issues, contracts._validator(kind).schema['$defs']
        with patch.object(Path, 'read_text', read), \
                patch.object(contracts.Draft202012Validator, 'check_schema', wraps=original_check) as checked, \
                ThreadPoolExecutor(max_workers=len(cases)) as workers:
            results = list(workers.map(validate, payloads))
            self.assertEqual(checked.call_count, 1)
            self.assertEqual(len(reads), 1)
        for issues, definitions in results:
            self.assertEqual(issues, [])
            self.assertIs(definitions, results[0][1])
        bad = json.loads(payloads[1][1]); bad['operations'][0]['device']['connection']['port'] = 0
        self.assertTrue(contracts.validate_message('configPatch', json.dumps(bad).encode()))
        self.assertEqual(contracts.validate_message('configSet', payloads[0][1]), [])
