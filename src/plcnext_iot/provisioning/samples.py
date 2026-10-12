"""Request-only view of the existing runner samples, on their owning loop.

No cache, background task, storage access, or protocol read is introduced.
The lifecycle's steady state and durable version fence candidate/old runners.
"""
import math


def read_samples(agent, gateway_id, device_id, expected_version, now):
    if (agent is None or agent.settings.gateway_id != gateway_id
            or agent.state not in ('RUNNING', 'DISABLED')):
        return 503, dict(code='RUNTIME_UNAVAILABLE')
    runtime = agent.runtime
    config = runtime.snapshot
    if config is None or config.config_version != agent.active_config_version:
        return 503, dict(code='RUNTIME_UNAVAILABLE')
    identity = dict(gatewayId=gateway_id, configVersion=config.config_version)
    if config.config_version != expected_version:
        return 409, dict(code='CONFIG_CHANGED', **identity)
    device = next((d for d in config.devices if d.device_id == device_id), None)
    if device is None:
        return 404, dict(code='DEVICE_NOT_FOUND', **identity)
    runner = runtime.runners.get(device_id)
    task = getattr(runner, '_task', None)
    stopped = runner is not None and (task is None or task.done())
    latest = getattr(runner, 'latest', {})
    points = []
    for point in device.points:
        sample = latest.get(point.point_id)
        # A reused runner can still contain samples from the previous report
        # version. Never attach them to the new committed configuration.
        if sample is not None and (sample.config_version != config.config_version
                                   or sample.device_id != device_id
                                   or sample.point_id != point.point_id):
            sample = None
        disabled = not config.enabled or not device.enabled or not point.enabled
        if disabled:
            quality, sample = 'DISABLED', None
        elif runner is None or stopped:
            quality = 'BAD_INTERNAL_ERROR'
        elif sample is None:
            quality = 'UNKNOWN'
        else:
            quality = sample.quality
            valid_value = (type(sample.value) is bool if point.data_type == 'bool' else
                           type(sample.value) in (int, float) and math.isfinite(sample.value))
            if quality == 'GOOD' and not valid_value:
                quality = 'BAD_TYPE_MISMATCH'
            if quality == 'GOOD' and (sample.monotonic_time is None
                    or now - sample.monotonic_time >= point.stale_after_ms / 1000):
                quality = 'STALE'
        points.append(dict(pointId=point.point_id, value=sample.value if sample and quality == 'GOOD' else None,
                           timestamp=sample.timestamp if sample else None, quality=quality))
    return 200, dict(**identity, deviceId=device_id, points=points)
