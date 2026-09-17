"""Run each image's target Python, native dependencies and durable lifecycle."""
import argparse
import subprocess

from tools.build_images import PLATFORMS

CHECK = r'''
import json, os, platform, subprocess, sys, tempfile
from pathlib import Path
import rpds, pymodbus, paho.mqtt.client, jsonschema
assert os.getuid() == 10001
with tempfile.TemporaryDirectory() as temporary:
    root=Path(temporary)
    (root/'bootstrap.json').write_text(json.dumps(dict(gatewayId='PLC-image',dataDirectory=str(root/'data'),operationTimeoutSeconds=5,queueCapacity=8)))
    (root/'config.json').write_text(json.dumps(dict(schemaVersion=1,gatewayId='PLC-image',timestamp=1788748123123,messageId='image-1',configVersion=1,
        config=dict(enabled=True,report=dict(batchIntervalMs=1000,maxBatchPoints=500),devices=[]))))
    command=[sys.executable,'-m','tools.agent','--bootstrap',str(root/'bootstrap.json'),'--driver','modbus-tcp','--quiet-samples','--run-seconds','0']
    for extra in (['--config',str(root/'config.json')], []):
        result=subprocess.run(command+extra,capture_output=True,text=True,timeout=20)
        assert result.returncode==0, result.stderr+result.stdout
        events=[json.loads(line) for line in result.stdout.splitlines()]
        assert events[-1]['state']=='STOPPED'
        assert events[-1]['activeConfigVersion']==1
    print(json.dumps(dict(result='PASS',architecture=platform.machine(),bits=platform.architecture()[0],uid=os.getuid(),checks=['native_imports','config_apply','restart_restore','read_only_root'])))
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',required=True)
    parser.add_argument('--platforms',default=','.join(PLATFORMS))
    args=parser.parse_args()
    platforms=args.platforms.split(',')
    if any(p not in PLATFORMS for p in platforms):
        parser.error('Unsupported platform')
    for target in platforms:
        suffix=target.removeprefix('linux/').replace('/','')
        subprocess.run(['docker','run','--rm','--platform',target,'--read-only','--tmpfs','/tmp:rw,size=32m,mode=1777',
            '--cap-drop','ALL','--security-opt','no-new-privileges:true','--entrypoint','python',
            args.image+'-'+suffix,'-c',CHECK],check=True,timeout=90)


if __name__=='__main__':
    main()
