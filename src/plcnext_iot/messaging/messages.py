"""Sanitized v1 configuration responses. Never guess an unknown active version."""
import json
import math
import re
import time
import uuid

from plcnext_iot.contracts import validate_message


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate key')
        result[key] = value
    return result


def identity(payload, gateway):
    if len(payload) > 2 * 1024 * 1024:
        return None
    try:
        def number(text):
            value = float(text)
            if not math.isfinite(value):
                raise ValueError('Nonfinite')
            return value
        value = json.loads(payload.decode('utf-8'), object_pairs_hook=_unique, parse_float=number,
                           parse_constant=lambda text: number(text))
        if (not isinstance(value, dict) or value.get('gatewayId') != gateway
                or not isinstance(value.get('messageId'), str)
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', value['messageId'])
                or type(value.get('configVersion')) is not int
                or not 1 <= value['configVersion'] <= 9007199254740991):
            return None
        return value['messageId'], value['configVersion']
    except (ValueError, UnicodeError, RecursionError):
        return None


def _base(gateway):
    return {'schemaVersion':1,'gatewayId':gateway,'timestamp':time.time_ns() // 1_000_000}


def config_ack(payload, result, gateway):
    request = identity(payload, gateway)
    if request is None or result.active_config_version is None:
        return None
    message_id, version = request
    if result.status == 'APPLIED' and result.active_config_version != version:
        return None
    known = {'SCHEMA_INVALID','INVALID_JSON','PAYLOAD_TOO_LARGE','GATEWAY_MISMATCH',
             'DUPLICATE_DEVICE_ID','DUPLICATE_POINT_ID','DUPLICATE_NODE_ID','INVALID_ADDRESS',
             'INVALID_STALE_INTERVAL','POINT_LIMIT_EXCEEDED','VERSION_CONFLICT','STALE_VERSION',
             'RESOURCE_LIMIT','STORAGE_ERROR','APPLY_FAILED','UNKNOWN_CONFIG_VERSION',
             'UNKNOWN_DEVICE','UNKNOWN_POINT','EXPIRED_DATA','INVALID_TIMESTAMP','DUPLICATE_VALUE'}
    def error(code, path=''):
        code = 'VERSION_CONFLICT' if code == 'MESSAGE_ID_CONFLICT' else code
        code = code if code in known else 'APPLY_FAILED'
        return {'code':code,'path':path[:512] if path.startswith('/') else '',
                'message':'Configuration could not be applied.'}
    errors = [] if result.status == 'APPLIED' else (
        [error(issue.code, issue.path) for issue in result.issues[:32]] or [error(result.code)])
    wire = encode(dict(_base(gateway),messageId=message_id,configVersion=version,
                       activeConfigVersion=result.active_config_version,status=result.status,errors=errors))
    return None if validate_message('configAck',wire,expected_gateway_id=gateway) else wire


def config_get(gateway, version, session):
    return encode(dict(_base(gateway),messageId='get-' + uuid.uuid4().hex,
                       activeConfigVersion=version,sessionId=session))


def status(gateway, session, online, reason):
    result = dict(_base(gateway),sessionId=session,online=online,reason=reason)
    if reason == 'connection_lost':
        result['timestamp'] = None
    return encode(result)
