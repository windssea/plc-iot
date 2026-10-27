"""Run the normal Agent against existing preview stores while Hub UI is tested.

No audit recorder, synthetic samples/ACKs, process crash, new database or OS-clock
change. A stop file ends capture, permits normal ACK drain, then closes the Agent.
"""
import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import sqlite3
import sys
from urllib.request import url2pathname
from urllib.parse import urlsplit
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'src')]
from tools.agent import run
from tools.verify_platform_live import DATA, GATEWAY, inspect
from plcnext_iot.core.boot import ProcessBootContext
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.messaging.settings import MqttSettings


async def trial(args):
    if args.output.exists() or args.stop_file.exists():
        raise FileExistsError('Use new evidence and stop paths')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    context = ProcessBootContext()
    stop = asyncio.Event()
    holder = []
    observations = []
    versions = set()
    failure = None
    code = 1

    def observed(agent):
        if agent is not None:
            holder.append(agent)

    def save_status(row):
        status = args.output.with_suffix('.status.json')
        temporary = status.with_suffix('.tmp')
        temporary.write_text(json.dumps(row, ensure_ascii=False, indent=2), encoding='utf-8')
        temporary.replace(status)

    async def coordinate():
        until = asyncio.get_running_loop().time() + args.seconds
        while asyncio.get_running_loop().time() < until and not args.stop_file.exists():
            if holder and holder[0].telemetry is not None:
                agent = holder[0]
                stats = await agent.telemetry.stats()
                row = dict(bootId=context.boot_id, state=agent.state,
                           activeConfigVersion=agent.active_config_version,
                           time=context.time_provider.diagnostics(),
                           captured=stats.get('capture_events'), eligible=stats.get('eligible_events'),
                           pending=stats.get('pending'), dead=stats.get('dead'),
                           stored=stats.get('diagnostics', {}).get('counters', {}).get('storedConfirmedEventsTotal'))
                observations.append(row)
                save_status(row)
                version = agent.active_config_version
                if version is not None and version not in versions and agent.runtime.snapshot is not None:
                    versions.add(version)
                    args.output.with_name(args.output.stem + f'.config-v{version}.json').write_text(
                        json.dumps(asdict(agent.runtime.snapshot), ensure_ascii=False, indent=2), encoding='utf-8')
                    print(json.dumps(dict(event='mainline_config_observed', version=version,
                                          bootId=context.boot_id)), flush=True)
            await asyncio.sleep(2)
        if holder and holder[0].telemetry is not None:
            agent = holder[0]
            await asyncio.gather(*(runner.stop() for runner in agent.runtime.runners.values()))
            deadline = asyncio.get_running_loop().time() + 80
            empty_since = None
            while asyncio.get_running_loop().time() < deadline:
                # The UI stats cache can still say zero while a final ingress
                # sample is being committed. Query the existing store and wait
                # for ingress, selection and its worker to remain empty.
                stats = await agent.telemetry.storage.call('stats')
                empty = (not stats['pending'] and not stats['dead']
                         and not agent.telemetry.samples.used_bytes
                         and not agent.telemetry.engine._owned
                         and not agent.telemetry.storage.jobs)
                now = asyncio.get_running_loop().time()
                if empty:
                    if empty_since is None:
                        empty_since = now
                    elif now - empty_since >= 2:
                        stop.set()
                        return
                else:
                    empty_since = None
                await asyncio.sleep(.25)
            raise RuntimeError('Pending batches retained after bounded drain; do not restore old backup')
        stop.set()

    settings = AgentSettings(GATEWAY, DATA)
    mqtt = MqttSettings('192.168.0.5', 1883, False, time_mode='HUB_MQTT',
                        time_basis_ref='site-hub-1', drift_ppm=1000,
                        drift_evidence='DEV_FUNCTIONAL_ONLY_NOT_PRODUCTION_DRIFT_CERTIFICATION')
    coordinator = asyncio.create_task(coordinate())
    worker = asyncio.create_task(run(settings, [], args.seconds + 100, driver='modbus-tcp',
        mqtt_settings=mqtt, quiet_samples=True, stop_event=stop, boot_context=context,
        runtime_observer=observed))
    try:
        done, _ = await asyncio.wait((worker, coordinator), return_when=asyncio.FIRST_COMPLETED)
        if coordinator in done:
            await coordinator
        else:
            raise RuntimeError('Agent ended before requested UI window')
        code = await worker
    except Exception as exc:
        failure = str(exc)
        stop.set()
        code = await worker
    finally:
        coordinator.cancel()
        await asyncio.gather(coordinator, return_exceptions=True)
        after = inspect()
        if after['telemetry']['batches']:
            failure = failure or 'Persistent batches remain after close; keep stores intact and do not restore backup'
        result = dict(scope='REAL_HUB_UI_NORMAL_AGENT_EXISTING_DATABASES', bootId=context.boot_id,
            exitCode=code, failure=failure, observations=observations, observedVersions=sorted(versions),
            finalTime=context.time_provider.diagnostics() if context.time_provider else None,
            finalStats=holder[0].telemetry.final_stats if holder else None, after=after,
            newDatabaseCreated=False, auditRecorderEnabled=False, syntheticDataOrAckPublished=False,
            sharedServicesStopped=False, operatingSystemClockChanged=False)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    return 0 if code == 0 and failure is None else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--stop-file', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=900, choices=range(1, 1201))
    args = parser.parse_args()
    allowed = {(DATA / name).resolve() for name in ('agent.db', 'telemetry.db')}
    if not all(path.is_file() for path in allowed):
        raise RuntimeError('Existing Agent and Outbox databases required')
    original_connect = sqlite3.connect

    def existing_only(database, *positional, **keywords):
        name = str(database)
        path = Path(url2pathname(urlsplit(name).path)) if name.startswith('file:') else Path(name)
        if path.resolve() not in allowed or not path.is_file():
            raise AssertionError('Only the two existing preview databases are allowed')
        return original_connect(database, *positional, **keywords)

    with patch.object(sqlite3, 'connect', existing_only):
        return asyncio.run(trial(args))


if __name__ == '__main__':
    raise SystemExit(main())
