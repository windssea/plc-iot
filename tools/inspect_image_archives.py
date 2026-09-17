"""Verify image metadata and native ELF architecture without running target code."""
import argparse
import json
from pathlib import Path
import struct
import tarfile


def inspect(path, architecture, machine):
    with tarfile.open(path) as archive:
        manifest, = json.load(archive.extractfile('manifest.json'))
        config = json.load(archive.extractfile(manifest['Config']))
        assert config['architecture'] == architecture, config['architecture']
        assert config['os'] == 'linux'
        assert config['config']['User'] == '10001:10001'
        found = []
        for layer in manifest['Layers']:
            with tarfile.open(fileobj=archive.extractfile(layer), mode='r|*') as contents:
                for member in contents:
                    if member.isfile() and member.name.startswith('opt/dependencies/') and member.name.endswith('.so'):
                        header = contents.extractfile(member).read(20)
                        assert header[:4] == b'\x7fELF'
                        endian = '<' if header[5] == 1 else '>'
                        assert struct.unpack(endian+'H', header[18:20])[0] == machine, member.name
                        found.append(member.name)
        assert found, 'Native extension missing'
    return dict(archive=str(path), architecture=architecture, nativeLibraries=found, result='PASS')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    args=parser.parse_args()
    for suffix, architecture, machine in [('amd64','amd64',62),('arm64','arm64',183),('armv7','arm',40)]:
        print(json.dumps(inspect(args.directory/('plcnext-iot-'+suffix+'.tar'),architecture,machine)))


if __name__=='__main__':
    main()
