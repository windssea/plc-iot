"""Build multi-architecture images; default output is offline per-platform archives."""
import argparse
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PLATFORMS = ('linux/amd64', 'linux/arm64', 'linux/arm/v7')


def commands(image, platforms, output, mode, builder=None):
    if not platforms or len(set(platforms)) != len(platforms) or any(p not in PLATFORMS for p in platforms):
        raise ValueError('Unsupported or duplicate platform')
    base = ['docker', 'buildx', 'build', '--file', str(ROOT/'deploy/container/Dockerfile')]
    base += ['--build-arg', 'VERSION='+image.rsplit(':', 1)[-1]]
    if builder:
        base += ['--builder', builder]
    if mode == 'push':
        return [base + ['--platform', ','.join(platforms), '--tag', image, '--push', str(ROOT)]]
    result = []
    for platform in platforms:
        suffix = platform.removeprefix('linux/').replace('/', '')
        command = base + ['--platform', platform, '--tag', image+'-'+suffix]
        if mode == 'load':
            command += ['--load']
        elif mode == 'archive':
            destination = Path(output).resolve()/('plcnext-iot-'+suffix+'.tar')
            if destination.exists():
                raise ValueError('Archive already exists: '+str(destination))
            command += ['--output', 'type=docker,dest='+str(destination)]
        else:
            raise ValueError('Unsupported output mode')
        result.append(command+[str(ROOT)])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True, help='Image name including an explicit tag')
    parser.add_argument('--platforms', default=','.join(PLATFORMS))
    parser.add_argument('--mode', choices=('archive','load','push'), default='archive')
    parser.add_argument('--output', type=Path, default=ROOT/'.local/images')
    parser.add_argument('--builder')
    args = parser.parse_args()
    if ':' not in args.image.rsplit('/', 1)[-1] or '@' in args.image:
        parser.error('--image must contain a tag, not a digest')
    try:
        builds = commands(args.image, args.platforms.split(','), args.output, args.mode, args.builder)
    except ValueError as exc:
        parser.error(str(exc))
    if args.mode == 'archive':
        args.output.mkdir(parents=True, exist_ok=True)
    for command in builds:
        subprocess.run(command, check=True)


if __name__ == '__main__':
    main()
