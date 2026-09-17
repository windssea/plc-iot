"""Test a built release in disposable Linux; does not access PLCs or host services."""
import asyncio
from pathlib import Path
import uuid

from tools.build_release import build, ROOT


SCRIPT = r'''
set -eu
cd /tmp
tar xzf /input/release.tar.gz
cd plcnext-iot-smoke
sh -n deploy/linux/install.sh
sh -n deploy/linux/activate.sh
addgroup -S plcnext-iot
adduser -S -G plcnext-iot plcnext-iot
sh deploy/linux/install.sh smoke /usr/local/bin/python3.12
cd /opt/plcnext-iot/releases/smoke
.venv/bin/python - <<'PY'
import json
from pathlib import Path
p = Path('/etc/plcnext-iot/bootstrap.json')
data = json.loads(p.read_text()); data['gatewayId'] = 'PLC-test'; p.write_text(json.dumps(data))
Path('/etc/plcnext-iot/mqtt.json').write_text(json.dumps(dict(host='127.0.0.1',port=1883,tls=False)))
Path('/tmp/config.json').write_text(json.dumps(dict(schemaVersion=1,gatewayId='PLC-test',timestamp=1788748123123,
    messageId='deployment-1',configVersion=1,config=dict(enabled=True,
    report=dict(batchIntervalMs=1000,maxBatchPoints=500),devices=[]))))
PY
su -s /bin/sh plcnext-iot -c '.venv/bin/python -m tools.preflight --bootstrap /etc/plcnext-iot/bootstrap.json --mqtt /etc/plcnext-iot/mqtt.json --release-root .'
su -s /bin/sh plcnext-iot -c '.venv/bin/python -' <<'PY'
import json, subprocess, time, signal
command = ['.venv/bin/python','-m','tools.agent','--bootstrap','/etc/plcnext-iot/bootstrap.json','--driver','modbus-tcp','--quiet-samples']
child = subprocess.Popen(command+['--config','/tmp/config.json'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
try:
    events=[]
    while True:
        line=child.stdout.readline()
        assert line, 'Agent exited before configuration'
        event=json.loads(line); events.append(event)
        if event['event']=='config_result':
            assert event['status']=='APPLIED'; break
    child.send_signal(signal.SIGTERM)
    out,err=child.communicate(timeout=15)
    assert child.returncode==0, err
    assert any(json.loads(line)['event']=='stopped' for line in out.splitlines())
    restored=subprocess.run(command+['--run-seconds','0'],capture_output=True,text=True,timeout=15)
    assert restored.returncode==0, restored.stderr
    events=[json.loads(line) for line in restored.stdout.splitlines()]
    assert next(e for e in events if e['event']=='started')['activeConfigVersion']==1
    print('PASS: Linux install, service-user preflight, SIGTERM and persisted configuration recovery')
finally:
    if child.poll() is None:
        child.kill(); child.wait()
PY
'''


async def main():
    directory = ROOT/'.local'/('deployment-'+uuid.uuid4().hex[:10])
    directory.mkdir(parents=True)
    build('smoke', directory/'release.tar.gz')
    (directory/'smoke.sh').write_text(SCRIPT, encoding='utf-8', newline='\n')
    name = 'plcnext-iot-deploy-'+uuid.uuid4().hex[:10]
    child = await asyncio.create_subprocess_exec('docker','run','--rm','--name',name,
        '--mount',f'type=bind,source={directory},target=/input,readonly',
        'python:3.12-alpine','sh','/input/smoke.sh')
    try:
        code = await asyncio.wait_for(child.wait(), 180)
        if code:
            raise RuntimeError('Linux deployment smoke failed')
    finally:
        cleanup = await asyncio.create_subprocess_exec('docker','rm','-f',name,
            stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
        await cleanup.wait()
        await child.wait()


if __name__ == '__main__':
    asyncio.run(main())
