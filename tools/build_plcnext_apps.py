"""Build separate SquashFS PLCnext Function Extension packages from offline images."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tarfile

from tools import _source_path  # noqa: F401
from jsonschema import Draft202012Validator
from tools.inspect_image_archives import inspect

ROOT = Path(__file__).resolve().parents[1]
PACKAGER = 'plcnext-iot-app-packager:local'
TARGETS = json.loads((ROOT/'deploy/plcnext-app/targets.json').read_text(encoding='utf-8'))


def validate_version(version):
    if not re.fullmatch(r'\d{1,3}\.\d{1,3}\.\d{1,3}', version) or any(int(n)>255 for n in version.split('.')):
        raise ValueError('Version must be major.minor.patch with components 0..255')


def firmware_tuple(value):
    if not re.fullmatch(r'\d+\.\d+\.\d+', value):
        raise ValueError('Firmware must be major.minor.patch')
    major, minor, patch = (int(part) for part in value.split('.'))
    # WBM shows "2025.6.0 (25.6.0.41)"; AppManager compares the short 25.x form.
    if major >= 2000:
        major -= 2000
    return major, minor, patch


def metadata(profile, identifier, version, image_id, image_name, port, minimum):
    validate_version(version)
    if not re.fullmatch(r'\d{14}', identifier):
        raise ValueError('App identifier must contain exactly 14 digits')
    parsed = firmware_tuple(minimum)
    if parsed < (25, 0, 0):
        raise ValueError('OCI App-part requires firmware 25.0.0 / 2025.0.0 or newer')
    minimum = f'{parsed[0]}.{parsed[1]}.{parsed[2]}'
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError('Rootless web port must be 1024..65535')
    return dict(plcnextapp=dict(name='PLCnext IoT', identifier=identifier, version=version,
        target=profile['target'], minfirmware_version=minimum, licensetype='Free',
        additionalInfo=[dict(key='Initialization',value='Open local setup',type='PortLink',url=str(port))]),
        datastorage=dict(persistentdata=True,temporarydata=False,
            directoriesToCreate=dict(persistent=[dict(path='iot')])),
        ocicontainer=dict(quadletFiles=[dict(type='Main',path='/iot.container')],
            environmentVariables=[dict(name='IOT_WEB_PORT',value=str(port))],
            images=[dict(name=image_name,id=image_id,path='/images/iot.tar.gz')]),
        updateconfigs=dict(autoupdate_enabled=False,keep_persistentdata=True,keep_temporarydata=False))


def quadlet(image_id):
    if not re.fullmatch(r'[a-f0-9]{64}', image_id):
        raise ValueError('Invalid image ID')
    return f'''[Unit]
Description=PLCnext IoT child-device collector

[Container]
ContainerName=plcnext-iot_${{APP_ID}}
Image={image_id}
UserNS=keep-id:uid=10001,gid=10001
User=10001
Group=10001
PublishPort=${{IOT_WEB_PORT}}:8080
Volume=${{APP_PERSISTENT_DIR}}/iot:/var/lib/plcnext-iot
ReadOnly=true
NoNewPrivileges=true
DropCapability=all
Tmpfs=/tmp:rw,size=16m,mode=1777
StopTimeout=1800

[Service]
Restart=on-failure
RestartSec=10
TimeoutStopSec=1830

[Install]
WantedBy=default.target
'''


def stage(profile, image, destination, identifier, version, port=8080, minimum='25.6.0'):
    inspect(image,profile['architecture'],profile['elfMachine'])
    with tarfile.open(image) as archive:
        manifest, = json.load(archive.extractfile('manifest.json'))
        config_bytes = archive.extractfile(manifest['Config']).read()
        image_id = hashlib.sha256(config_bytes).hexdigest()
        config = json.loads(config_bytes)
        if config['config'].get('Entrypoint') != ['python','-m','tools.appliance']:
            raise ValueError('Expected appliance initialization entrypoint')
        image_name, = manifest['RepoTags']
    data = metadata(profile,identifier,version,image_id,image_name,port,minimum)
    schema = json.loads((ROOT/'deploy/plcnext-app/app-info-schema.json').read_text(encoding='utf-8'))
    Draft202012Validator(schema).validate(data)
    destination.mkdir(parents=True,exist_ok=False)
    (destination/'images').mkdir()
    with image.open('rb') as source, (destination/'images/iot.tar.gz').open('xb') as output:
        with gzip.GzipFile(fileobj=output,mode='wb',filename='',mtime=0) as compressed:
            shutil.copyfileobj(source,compressed)
    (destination/'app_info.json').write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8',newline='\n')
    (destination/'iot.container').write_text(quadlet(image_id),encoding='utf-8',newline='\n')
    return data


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--images',type=Path,default=ROOT/'.local/images/0.2.0')
    parser.add_argument('--output',type=Path,default=ROOT/'.local/wbm/0.2.0')
    parser.add_argument('--version',default='0.2.0')
    parser.add_argument('--axcf2152-app-id')
    parser.add_argument('--vplc-app-id')
    parser.add_argument('--vplc-targets',help='Exact comma-separated WBM Information > Type values')
    parser.add_argument('--web-port',type=int,default=8080)
    parser.add_argument('--min-firmware',default='25.6.0',
                        help='WBM compares the short form, e.g. 25.6.0 not 2025.6.0')
    args=parser.parse_args()
    validate_version(args.version)
    output=args.output.resolve()
    if output.exists():
        parser.error('Output already exists; select a new directory')
    output.mkdir(parents=True)
    records=[]
    for key, original in TARGETS.items():
        profile=dict(original)
        if key=='vplc' and args.vplc_targets:
            profile['target']=args.vplc_targets
        supplied=getattr(args,key+'_app_id')
        identifier=supplied or profile['developmentAppId']
        directory=output/('source-'+key)
        info=stage(profile,args.images/profile['archive'],directory,identifier,args.version,args.web_port,args.min_firmware)
        name=f'plcnext-iot-{args.version}-{key}-unsigned.app'
        records.append(dict(file=name,target=profile['target'],platform=profile['platform'],appId=identifier,
            developmentId=supplied is None,signed=False,
            minFirmware=info['plcnextapp']['minfirmware_version'],
            imageId=info['ocicontainer']['images'][0]['id'],source=directory.name))
    subprocess.run(['docker','build','-t',PACKAGER,'-f',str(ROOT/'deploy/plcnext-app/Dockerfile'),str(ROOT)],check=True)
    for record in records:
        subprocess.run(['docker','run','--rm','--network','none',
            '--mount',f'type=bind,source={output/record["source"]},target=/input,readonly',
            '--mount',f'type=bind,source={output},target=/output',PACKAGER,
            '/input','/output/'+record['file'],'-noappend','-comp','gzip','-force-uid','1001','-force-gid','1002',
            '-processors','1','-no-progress'],check=True)
        # Read metadata back from the actual SquashFS, not merely its staging tree.
        result=subprocess.run(['docker','run','--rm','--network','none','--entrypoint','unsquashfs',
            '--mount',f'type=bind,source={output},target=/output,readonly',PACKAGER,
            '-cat','/output/'+record['file'],'app_info.json'],check=True,capture_output=True)
        recovered=json.loads(result.stdout)
        assert recovered['plcnextapp']['identifier']==record['appId']
        assert recovered['plcnextapp']['minfirmware_version']==record['minFirmware']
        assert firmware_tuple(record['minFirmware'])[0] < 2000
        artifact=output/record['file']
        assert artifact.read_bytes()[:4]==b'hsqs'
        record['sha256']=hashlib.sha256(artifact.read_bytes()).hexdigest()
        record['bytes']=artifact.stat().st_size
    (output/'manifest.json').write_text(json.dumps(records,indent=2)+'\n',encoding='utf-8')
    (output/'SHA256SUMS').write_text(''.join(r['sha256']+'  '+r['file']+'\n' for r in records),encoding='ascii')
    print(json.dumps(records,indent=2))


if __name__=='__main__':
    main()
