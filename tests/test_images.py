from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.build_images import commands, PLATFORMS
from deploy.container.dependencies import wheel_platform


class ImageTests(unittest.TestCase):
    def test_target_wheels(self):
        self.assertEqual(wheel_platform('amd64',''), 'manylinux_2_17_x86_64')
        self.assertEqual(wheel_platform('arm64',''), 'manylinux_2_17_aarch64')
        self.assertEqual(wheel_platform('arm','v7'), 'manylinux_2_17_armv7l')
        for arch, variant in [('arm','v6'), ('arm',''), ('riscv64','')]:
            with self.assertRaises(ValueError):
                wheel_platform(arch, variant)

    def test_offline_archives_never_push(self):
        with tempfile.TemporaryDirectory() as directory:
            builds = commands('plcnext-iot:1.0', PLATFORMS, directory, 'archive')
            self.assertEqual(len(builds), 3)
            for build, suffix in zip(builds, ('amd64','arm64','armv7')):
                self.assertNotIn('--push', build)
                self.assertIn('plcnext-iot:1.0-'+suffix, build)
                self.assertIn('type=docker,dest='+str(Path(directory)/('plcnext-iot-'+suffix+'.tar')), build)
            (Path(directory)/'plcnext-iot-amd64.tar').touch()
            with self.assertRaises(ValueError):
                commands('plcnext-iot:1.0', PLATFORMS, directory, 'archive')

    def test_push_uses_one_platform_index(self):
        build, = commands('registry.example/iot:1.0', PLATFORMS, None, 'push')
        self.assertIn(','.join(PLATFORMS), build)
        self.assertIn('--push', build)
        self.assertNotIn('--load', build)

    def test_platform_typo_fails_before_build(self):
        with self.assertRaises(ValueError):
            commands('iot:1', ['linux/arm'], '.', 'load')
