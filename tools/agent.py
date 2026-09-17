"""Run the local Agent lifecycle with optional read-only Modbus TCP collection."""
import argparse
import asyncio
from dataclasses import asdict
import json
import math
from pathlib import Path
import signal

from tools import _source_path  # noqa: F401
from plcnext_iot.core.lifecycle import AgentLifecycle, LifecycleError
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.config.store import StoreError


def _emit(event, agent, **fields):
    print(json.dumps({"event": event, "state": agent.state,
                      "activeConfigVersion": agent.active_config_version, **fields}), flush=True)


async def run(settings, configs, run_seconds, driver='none', mqtt_settings=None, quiet_samples=False,
              stop_event=None, boot_id=None):
    from plcnext_iot.points.samples import SampleBuffer
    samples = SampleBuffer()
    if driver == 'modbus-tcp':
        from plcnext_iot.devices.modbus import ModbusRunner
        runtime = DeviceRuntime(lambda device: ModbusRunner(device, runtime, samples))
    else:
        runtime = DeviceRuntime()
    agent = AgentLifecycle(settings, runtime)
    async def emit_samples():
        while True:
            sample = await samples.get()
            if not quiet_samples:
                _emit('sample', agent, sample=asdict(sample), droppedSamples=samples.dropped)
    consumer = None
    mqtt_service = None
    telemetry = None
    stop = stop_event if stop_event is not None else asyncio.Event()
    loop = asyncio.get_running_loop()
    previous = {}
    for sig in (() if stop_event is not None else (signal.SIGINT, signal.SIGTERM)):
        previous[sig] = signal.signal(sig, lambda *_args: loop.call_soon_threadsafe(stop.set))
    code = 0
    try:
        await agent.start()
        if mqtt_settings is not None:
            from plcnext_iot.messaging.config_service import MqttConfigService
            from plcnext_iot.reporting.service import TelemetryPipeline
            telemetry=TelemetryPipeline(runtime,samples,settings.data_directory,settings.gateway_id,
                boot_id=boot_id,
                on_sample=None if quiet_samples else lambda sample: _emit('sample',agent,sample=asdict(sample),droppedSamples=samples.dropped))
            await telemetry.start()
            mqtt_service = MqttConfigService(agent,mqtt_settings,telemetry=telemetry)
            await mqtt_service.start()
        _emit("started", agent, driverInstalled=driver != 'none',
              mqttConnected=mqtt_service.connected if mqtt_service is not None else False)
        if driver != 'none' and telemetry is None:
            consumer = asyncio.create_task(emit_samples())
        for path in configs:
            if stop.is_set():
                break
            with path.open("rb") as stream:
                payload = stream.read(2 * 1024 * 1024 + 1)
            result = await agent.submit(payload)
            _emit("config_result", agent, status=result.status, code=result.code,
                  issues=[asdict(issue) for issue in result.issues])
            if result.status != "APPLIED":
                code = 1
                break
        if code == 0:
            waits = [asyncio.create_task(stop.wait()), asyncio.create_task(agent.wait_failed())]
            if mqtt_service is not None:
                waits.append(asyncio.create_task(mqtt_service.wait_failed()))
            if telemetry is not None:
                waits.append(asyncio.create_task(telemetry.wait_failed()))
            try:
                await asyncio.wait(waits, timeout=run_seconds, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for task in waits:
                    task.cancel()
                await asyncio.gather(*waits, return_exceptions=True)
            if mqtt_service is not None and mqtt_service.last_error == 'MQTT_SERVICE_FAILED':
                code = 1
            if telemetry is not None and telemetry.last_error is not None:
                code = 1
    except LifecycleError:
        _emit("startup_failed", agent, code=agent.last_error)
        code = 1
    except OSError:
        _emit("input_failed", agent, code="INPUT_READ_ERROR")
        code = 2
    except StoreError:
        _emit('storage_failed',agent,code='TELEMETRY_STORAGE_ERROR')
        code=1
    finally:
        try:
            try:
                if mqtt_service is not None:
                    await mqtt_service.close()
            finally:
                try:
                    await agent.close()
                finally:
                    if telemetry is not None:
                        await telemetry.close()
                        _emit('telemetry_stopped',agent,stats=telemetry.final_stats,code=telemetry.last_error)
                        if telemetry.last_error is not None:
                            code=1
        finally:
            if consumer is not None:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
            for sig, handler in previous.items():
                signal.signal(sig, handler)
        _emit("stopped", agent, code=agent.last_error)
    return 1 if agent.state == "FAILED" else code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", type=Path, required=True)
    parser.add_argument("--config", type=Path, action="append", default=[])
    parser.add_argument('--driver', choices=('none', 'modbus-tcp'), default='none')
    parser.add_argument('--mqtt',type=Path,help='Optional separate MQTT settings JSON')
    parser.add_argument('--quiet-samples', action='store_true', help='Suppress per-point diagnostic logs; telemetry is unaffected')
    parser.add_argument("--run-seconds", type=float, help="Optional local smoke-test duration; 0 stops after configuration")
    args = parser.parse_args()
    if args.run_seconds is not None and (not math.isfinite(args.run_seconds) or args.run_seconds < 0):
        parser.error("--run-seconds must be finite and nonnegative")
    try:
        settings = AgentSettings.from_file(args.bootstrap)
        mqtt_settings = None
        if args.mqtt is not None:
            from plcnext_iot.messaging.settings import MqttSettings
            mqtt_settings = MqttSettings.from_file(args.mqtt)
    except ValueError:
        print(json.dumps({"event": "bootstrap_failed", "code": "INVALID_BOOTSTRAP"}))
        return 2
    return asyncio.run(run(settings, args.config, args.run_seconds, args.driver, mqtt_settings, args.quiet_samples))


if __name__ == "__main__":
    raise SystemExit(main())
