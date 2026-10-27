"""Pure commissioning parsing: broker edits cannot silently change time authority."""
import json
import unittest
from pathlib import Path
from plcnext_iot.messaging.settings import MqttSettings
from plcnext_iot.provisioning.store import parse,parse_update,SetupStore,SettingsError


class TimeSettingsTests(unittest.TestCase):
    def setUp(self):
        self.base=dict(gatewayId='PLC-01',host='localhost',port=1883,tls=False,username='',password='',caPem='')
        self.timing=dict(mode='HUB_MQTT',timeBasisRef='site-hub-1',maxDriftPpm=50,driftEvidence='TEST_FIXTURE_ONLY')

    def test_explicit_time_policy_survives_broker_edit_and_runtime_conversion(self):
        value=parse(json.dumps(dict(self.base,time=self.timing)).encode())
        updated=parse_update(json.dumps(dict(self.base,host='new-broker')).encode(),value)
        self.assertEqual(updated['time'],self.timing)
        # settings() is pure; avoid creating a directory or any database.
        store=SetupStore.__new__(SetupStore);store.directory=Path('.');store.ca_path=Path('unused.pem')
        settings=store.settings(updated)[1]
        self.assertEqual((settings.time_mode,settings.time_basis_ref,settings.drift_ppm),('HUB_MQTT','site-hub-1',50))

    def test_legacy_configuration_remains_explicitly_unverified(self):
        self.assertNotIn('time',parse(json.dumps(self.base).encode()))
        self.assertEqual(MqttSettings('localhost',1883,False).time_mode,'SYSTEM_RTC')

    def test_no_default_drift_proof_or_unknown_time_keys(self):
        for timing in [dict(mode='HUB_MQTT'),dict(self.timing,maxDriftPpm=True),dict(self.timing,extra=True),dict(self.timing,driftEvidence='')]:
            with self.assertRaises(SettingsError):parse(json.dumps(dict(self.base,time=timing)).encode())

    def test_web_broker_update_cannot_replace_clock_policy(self):
        old=dict(self.base,time=self.timing)
        with self.assertRaises(SettingsError):parse_update(json.dumps(old).encode(),old)


if __name__=='__main__':unittest.main()
