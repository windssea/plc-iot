"""Materialize a validated device command against its exact committed base.

This runs only on the existing configuration worker, never the sampling path.
The persisted runtime snapshot stays a complete configSet for all consumers.
"""
import json

from plcnext_iot.config.models import ConfigSnapshot, ConfigValidationError
from plcnext_iot.contracts import Issue, validate_message


def parse_command(payload, gateway_id):
    issues = validate_message('configPatch', payload, expected_gateway_id=gateway_id)
    if issues:
        raise ConfigValidationError(issues)
    return json.loads(payload.decode('utf-8'))


def materialize(command, active, gateway_id):
    def reject(code, path):
        raise ConfigValidationError([Issue(code, path, 'Device command cannot be applied to its base.')])
    base_version = active.config_version if active else 0
    base_message = active.message_id if active else None
    if (command['baseConfigVersion'] != base_version or command['baseMessageId'] != base_message):
        reject('BASE_CONFIG_MISMATCH', '/baseConfigVersion')
    config = json.loads(active.content_json) if active else dict(devices=[])
    config.update(command.get('gateway', {}))
    devices = {d['deviceId']: d for d in config['devices']}
    for index, operation in enumerate(command['operations']):
        key, op = operation['deviceId'], operation['op']
        path = f'/operations/{index}/deviceId'
        if op == 'add' and key in devices:
            reject('DEVICE_EXISTS', path)
        if op in ('replace', 'remove') and key not in devices:
            reject('DEVICE_NOT_FOUND', path)
        if op == 'remove':
            del devices[key]
        else:
            devices[key] = operation['device']
    config['devices'] = list(devices.values())
    full = {k: command[k] for k in ('schemaVersion', 'gatewayId', 'timestamp', 'messageId', 'configVersion')}
    full['config'] = config
    return ConfigSnapshot.parse(json.dumps(full, ensure_ascii=False, separators=(',', ':'),
                                          allow_nan=False).encode('utf-8'), gateway_id)
