import json
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.build_plcnext_apps import TARGETS, metadata, quadlet, validate_version
from jsonschema import Draft202012Validator


class PlcnextAppTests(unittest.TestCase):
    def test_metadata_matches_official_schema_for_both_targets(self):
        schema=json.loads((ROOT/'deploy/plcnext-app/app-info-schema.json').read_text(encoding='utf-8'))
        for profile in TARGETS.values():
            value=metadata(profile,profile['developmentAppId'],'0.2.0','a'*64,'plcnext-iot:0.2.0',8080,'2026.0.3')
            Draft202012Validator(schema).validate(value)
            self.assertTrue(value['updateconfigs']['keep_persistentdata'])
            self.assertFalse(value['updateconfigs']['autoupdate_enabled'])
            self.assertEqual(value['ocicontainer']['images'][0]['path'],'/images/iot.tar.gz')
            self.assertEqual(value['plcnextapp']['additionalInfo'][0]['type'],'PortLink')

    def test_architecture_and_app_identity_separated(self):
        self.assertEqual(TARGETS['axcf2152']['platform'],'linux/arm/v7')
        self.assertEqual(TARGETS['axcf2152']['target'],'AXC F 2152')
        self.assertEqual(TARGETS['vplc']['platform'],'linux/amd64')
        self.assertEqual(TARGETS['vplc']['target'],
                         'VPLCNEXT CONTROL 500 (x86),VPLCNEXT CONTROL 1000 (x86),VPLCNEXT CONTROL 2000 (x86),VPLCNEXT CONTROL 3000 (x86)')
        self.assertNotEqual(TARGETS['axcf2152']['developmentAppId'],TARGETS['vplc']['developmentAppId'])

    def test_only_app_storage_and_rootless_user_mapping(self):
        definition=quadlet('a'*64)
        self.assertIn('Volume=${APP_PERSISTENT_DIR}/iot:/var/lib/plcnext-iot',definition)
        self.assertIn('UserNS=keep-id:uid=10001,gid=10001',definition)
        self.assertIn('ContainerName=plcnext-iot_${APP_ID}',definition)
        self.assertNotIn('docker.sock',definition)
        self.assertNotIn('Privileged=',definition)

    def test_invalid_packaging_values_rejected(self):
        for version in ('../escape','1.2.999','dev','1.2.3.4'):
            with self.assertRaises(ValueError):validate_version(version)
        for identifier,port,minimum in [('bad',8080,'25.6.0'),('1'*14,80,'25.6.0'),('1'*14,8080,'24.0.0')]:
            with self.assertRaises(ValueError):
                metadata(TARGETS['axcf2152'],identifier,'0.2.0','a'*64,'image',port,minimum)
        value=metadata(TARGETS['axcf2152'],TARGETS['axcf2152']['developmentAppId'],
                       '0.2.0','a'*64,'plcnext-iot:0.2.0',8080,'25.6.0')
        self.assertEqual(value['plcnextapp']['minfirmware_version'],'25.6.0')
        value=metadata(TARGETS['axcf2152'],TARGETS['axcf2152']['developmentAppId'],
                       '0.2.0','a'*64,'plcnext-iot:0.2.0',8080,'2025.6.0')
        self.assertEqual(value['plcnextapp']['minfirmware_version'],'25.6.0')
        with self.assertRaises(ValueError):
            metadata(TARGETS['axcf2152'],TARGETS['axcf2152']['developmentAppId'],
                     '0.2.0','a'*64,'image',8080,'2024.0.0')
        with self.assertRaises(ValueError):quadlet('bad\nExec=command')
