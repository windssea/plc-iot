import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.build_release import build
from tools.preflight import check


class DeploymentTests(unittest.TestCase):
    def test_archive_allowlist_and_integrity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root/'release.tar.gz'
            manifest = build('test-1', archive)
            self.assertNotIn('tests/test_deployment.py', manifest['files'])
            self.assertNotIn('deploy/mqtt.example.json', manifest['files'])
            self.assertTrue(all('.local' not in name and '__pycache__' not in name for name in manifest['files']))
            with tarfile.open(archive) as stream:
                stream.extractall(root, filter='data')
            release = root/'plcnext-iot-test-1'
            bootstrap = root/'bootstrap.json'
            bootstrap.write_text(json.dumps(dict(gatewayId='PLC-test',dataDirectory=str(root),
                operationTimeoutSeconds=5,queueCapacity=8)))
            result = check(bootstrap, release_root=release)
            self.assertTrue(result['ok'], result)
            (release/'tools/agent.py').write_text('tampered')
            self.assertFalse(check(bootstrap, release_root=release)['ok'])
            with self.assertRaises(FileExistsError):
                build('test-1', archive)

    def test_invalid_version_rejected(self):
        for version in ('../escape', '', '/tmp/release', 'x'*65, 'version with spaces'):
            with self.assertRaises(ValueError):
                build(version, Path('unused.tar.gz'))

    def test_preflight_does_not_create_data_or_reveal_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bootstrap = root/'bootstrap.json'
            bootstrap.write_text(json.dumps(dict(gatewayId='PLC-test',dataDirectory=str(root/'missing'),
                operationTimeoutSeconds=5,queueCapacity=8)))
            mqtt = root/'mqtt.json'
            mqtt.write_text('{"host":"secret-value","password":"sensitive-value"}')
            result = check(bootstrap, mqtt)
            self.assertFalse(result['ok'])
            self.assertFalse((root/'missing').exists())
            self.assertNotIn('secret-value', json.dumps(result))
            self.assertNotIn('sensitive-value', json.dumps(result))
