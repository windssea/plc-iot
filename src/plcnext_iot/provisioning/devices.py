"""Read the last committed configuration without becoming its writer/owner.

The appliance agent owns ConfigStore. The Web surface opens a separate read-only
connection for a short consistent transaction and never initializes or recovers
the store. DTOs are allowlists, so credential references stay private.
"""
from contextlib import closing
from pathlib import Path
import sqlite3

from plcnext_iot.config.models import ConfigSnapshot, ConfigValidationError
from plcnext_iot.config.store import APPLICATION_ID, SCHEMA_VERSION


class ConfigUnavailable(RuntimeError):
    """Stable public failure; no filesystem or stored values in its message."""


def read_active(directory, gateway_id):
    path = Path(directory) / 'agent.db'
    try:
        path.stat()
    except FileNotFoundError:
        return None
    except OSError:
        raise ConfigUnavailable('CONFIG_UNAVAILABLE') from None
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True,
                                    timeout=0.25, isolation_level=None)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA query_only=ON')
            conn.execute('BEGIN')
            if (conn.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID
                    or conn.execute('PRAGMA user_version').fetchone()[0] != SCHEMA_VERSION):
                raise ConfigUnavailable('CONFIG_UNAVAILABLE')
            meta = conn.execute('SELECT gateway_id,active_version FROM metadata WHERE id=1').fetchone()
            if meta is None or meta['gateway_id'] != gateway_id:
                raise ConfigUnavailable('CONFIG_UNAVAILABLE')
            active = meta['active_version']
            if active is None:
                return None
            if type(active) is not int or active < 1:
                raise ConfigUnavailable('CONFIG_UNAVAILABLE')
            row = conn.execute('SELECT version,payload,content_hash,state FROM snapshots WHERE version=?',
                               (active,)).fetchone()
            if row is None or row['state'] != 'COMMITTED':
                raise ConfigUnavailable('CONFIG_UNAVAILABLE')
            # Release the SQLite read transaction before validating potentially
            # large contracts; subsequent reads get their own fresh snapshot.
            conn.execute('COMMIT')
        snapshot = ConfigSnapshot.parse(row['payload'].encode('utf-8'), gateway_id)
        if snapshot.config_version != row['version'] or snapshot.content_hash != row['content_hash']:
            raise ConfigUnavailable('CONFIG_UNAVAILABLE')
        return snapshot
    except (sqlite3.Error, OSError, ConfigValidationError, UnicodeError, TypeError,
            AttributeError, ValueError, RecursionError):
        raise ConfigUnavailable('CONFIG_UNAVAILABLE') from None


def metadata(gateway_id, snapshot):
    return dict(configured=gateway_id is not None, gatewayId=gateway_id,
                configVersion=snapshot.config_version if snapshot else 0,
                messageId=snapshot.message_id if snapshot else None,
                timestamp=snapshot.timestamp if snapshot else None,
                enabled=snapshot.enabled if snapshot else None,
                report=dict(batchIntervalMs=snapshot.report.batch_interval_ms,
                            maxBatchPoints=snapshot.report.max_batch_points) if snapshot else None)


def summary(device):
    return dict(deviceId=device.device_id, name=device.name, enabled=device.enabled,
                protocol=device.protocol, pointCount=len(device.points))


def detail(device):
    connection = device.connection
    safe_connection = dict(host=connection.host, port=connection.port,
                           connectTimeoutMs=connection.connect_timeout_ms,
                           requestTimeoutMs=connection.request_timeout_ms,
                           retryCount=connection.retry_count)
    if device.protocol == 'modbus_tcp':
        safe_connection['unitId'] = connection.unit_id
    else:
        safe_connection.update(path=connection.path, securityPolicy=connection.security_policy,
                               securityMode=connection.security_mode,
                               usernameSet=bool(connection.username),
                               passwordSet=bool(connection.password_env))
    points = []
    for point in device.points:
        safe_point = dict(pointId=point.point_id, name=point.name, enabled=point.enabled,
                          dataType=point.data_type, pollIntervalMs=point.poll_interval_ms,
                          scale=point.scale, offset=point.offset, unit=point.unit,
                          reportMode=point.report_mode, reportIntervalMs=point.report_interval_ms,
                          deadband=point.deadband, staleAfterMs=point.stale_after_ms)
        if point.modbus:
            safe_point['modbus'] = dict(area=point.modbus.area, address=point.modbus.address,
                                       byteOrder=point.modbus.byte_order)
        elif point.opcua:
            safe_point['opcua'] = dict(nodeId=point.opcua.node_id)
        points.append(safe_point)
    return dict(deviceId=device.device_id, name=device.name, enabled=device.enabled,
                protocol=device.protocol, connection=safe_connection, points=points)
