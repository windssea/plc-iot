"""Install target wheels using the build host; never execute target binaries."""
from pathlib import Path
import subprocess
import sys


def wheel_platform(architecture, variant):
    targets = {('amd64', ''): 'x86_64', ('arm64', ''): 'aarch64',
               ('arm64', 'v8'): 'aarch64', ('arm', 'v7'): 'armv7l'}
    try:
        return 'manylinux_2_17_' + targets[(architecture, variant)]
    except KeyError:
        raise ValueError('Supported targets: linux/amd64, linux/arm64, linux/arm/v7') from None


def main():
    platform = wheel_platform(*sys.argv[1:])
    subprocess.run([sys.executable, '-m', 'pip', 'install', '--no-cache-dir',
        '--only-binary=:all:', '--platform', platform, '--implementation', 'cp',
        '--python-version', '3.12', '--abi', 'cp312', '--no-compile',
        '--target', '/dependencies', '-r', 'requirements-runtime.lock'], check=True)
    Path('/data').mkdir(mode=0o700)


if __name__ == '__main__':
    main()
