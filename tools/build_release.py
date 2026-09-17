"""Build a source-only deployment archive from an explicit allowlist."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import tarfile

ROOT = Path(__file__).resolve().parents[1]


def build(version, output, root=ROOT):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', version):
        raise ValueError('Invalid release identifier')
    paths = [p for folder in ('src', 'contracts/v1', 'deploy/linux')
             for p in (root/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts]
    paths += [root/p for p in ('tools/_source_path.py', 'tools/agent.py', 'tools/appliance.py',
        'tools/preflight.py', 'requirements-runtime.txt', 'requirements-contracts.txt', 'requirements-runtime.lock')]
    files = {}
    for path in sorted(paths):
        if path.is_symlink():
            raise ValueError('Release input must not be a symlink')
        name = path.relative_to(root).as_posix()
        content = path.read_bytes()
        if path.suffix in ('.sh', '.service', '.json', '.py', '.txt'):
            content = content.decode('utf-8-sig').replace('\r\n', '\n').encode('utf-8')
        files[name] = content
    manifest = dict(version=version, files={name: hashlib.sha256(data).hexdigest() for name,data in files.items()})
    files['manifest.json'] = json.dumps(manifest, sort_keys=True, indent=2).encode()
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('xb') as stream:
        with tarfile.open(fileobj=stream, mode='w:gz') as archive:
            for name, data in files.items():
                info = tarfile.TarInfo('plcnext-iot-'+version+'/'+name)
                info.size = len(data)
                info.mode = 0o755 if name.endswith('.sh') else 0o644
                archive.addfile(info, io.BytesIO(data))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = build(args.version, args.output)
    print(json.dumps(dict(version=manifest['version'], files=len(manifest['files']),
                          archive=str(args.output.resolve()))))


if __name__ == '__main__':
    main()
