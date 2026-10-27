"""Shared timeSyncV1 wire fixtures; no MQTT, SQLite or device connection."""
import json
from pathlib import Path
import unittest

from tools.contract_validation import validate_message


class TimeSyncContractTests(unittest.TestCase):
    def test_shared_golden_cases(self):
        path = Path(__file__).resolve().parents[1] / 'contracts/time-sync-cases.json'
        cases = json.loads(path.read_text(encoding='utf-8'))['cases']
        self.assertEqual(len(cases), 10)
        for case in cases:
            with self.subTest(case=case['name']):
                issues = validate_message(case['kind'], json.dumps(case['payload']).encode(),
                                          expected_gateway_id='PLC-01')
                self.assertEqual(not issues, case['valid'])

    def test_wire_decoding_is_strict_and_bounded(self):
        for raw in (b'{"x":1,"x":1}', b'\xef\xbb\xbf{}', b'\xff', b' ' * 2049):
            self.assertTrue(validate_message('timeRequest', raw))


if __name__ == '__main__':
    unittest.main()
