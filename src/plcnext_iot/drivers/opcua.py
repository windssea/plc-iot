"""Read-only scheduled OPC UA results; no MQTT or SQLite."""
import asyncio
import math
import os

from asyncua import Client, ua
from asyncua.ua.uaerrors import UaError, UaStatusCodeError
from plcnext_iot.points.slots import skipped_after


RANGES = {
    'int16': (-32768, 32767),
    'uint16': (0, 65535),
    'int32': (-2147483648, 2147483647),
    'uint32': (0, 4294967295),
}


def endpoint_url(connection):
    return f'opc.tcp://{connection.host}:{connection.port}{connection.path or ""}'


def decode(point, value):
    if point.data_type == 'bool':
        if type(value) is not bool:
            raise ValueError('Expected bool')
        return value
    if type(value) is bool or not isinstance(value, (int, float)):
        raise ValueError('Expected number')
    if point.data_type in RANGES:
        lo, hi = RANGES[point.data_type]
        if type(value) is not int or not lo <= value <= hi:
            raise ValueError('Out of range')
    result = value * point.scale + point.offset
    if isinstance(result, float) and not math.isfinite(result):
        raise ValueError('Nonfinite value')
    return result


def quality_of(status):
    if status is None or status.is_good():
        return 'GOOD'
    name = getattr(status, 'name', '') or str(status)
    if 'Timeout' in name:
        return 'BAD_TIMEOUT'
    if any(token in name for token in ('Connection', 'Session', 'SecureChannel', 'Communication')):
        return 'BAD_CONNECTION'
    return 'BAD_PROTOCOL'


class _Handler:
    def __init__(self, points, emit, lost):
        self.points, self.emit, self.lost = points, emit, lost

    async def datachange_notification(self, node, val, data):
        point = self.points.get(node.nodeid)
        if point is None:
            return
        try:
            status = data.monitored_item.Value.StatusCode
        except AttributeError:
            status = None
        quality = quality_of(status)
        value = None
        if quality == 'GOOD':
            try:
                value = decode(point, val)
            except (ValueError, TypeError, OverflowError):
                quality = 'BAD_DECODE'
        self.emit(point, value, quality)

    def status_change_notification(self, status):
        code = getattr(status, 'Status', status)
        if code is not None and hasattr(code, 'is_good') and not code.is_good():
            self.lost.set()


class OpcUaReader:
    def __init__(self, connection, points):
        self.connection, self.points = connection, points
        self._client = None
        self._lost = asyncio.Event()

    async def close(self):
        client, self._client = self._client, None
        if client is None:
            return
        try:
            await client.disconnect()
        except (OSError, UaError, asyncio.CancelledError):
            pass

    async def run(self, emit, *, before_read=None, skipped=None, deadlines=None, retired_at=None, refresh_plan=None):
        """Connect and read nominal slots until the session is lost or cancelled."""
        connection = self.connection
        if connection.security_policy != 'None' or connection.security_mode != 'None':
            raise RuntimeError('BAD_CONFIGURATION')
        if connection.username:
            password = os.environ.get(connection.password_env or '')
            if not password:
                raise RuntimeError('BAD_CONFIGURATION')
        self._lost = asyncio.Event()
        timeout = connection.request_timeout_ms / 1000
        client = Client(endpoint_url(connection), timeout=timeout, auto_reconnect=False)
        client.session_timeout = max(connection.connect_timeout_ms, 10000)
        if connection.username:
            client.set_user(connection.username)
            client.set_password(os.environ[connection.password_env])
        async def on_lost(_exc):
            self._lost.set()
        client.connection_lost_callback = on_lost
        self._client = client
        last_error = None
        enabled = tuple(p for p in self.points if p.enabled)
        loop = asyncio.get_running_loop()
        next_read = deadlines if deadlines is not None else {p.point_id: loop.time() for p in enabled}
        initial = {p.point_id: (before_read(p, next_read[p.point_id], dict(next_read))
                               if before_read else None) for p in enabled}
        connected = False
        def advance(point, nominal, context):
            missed = skipped_after(nominal,loop.time(),point.poll_interval_ms/1000,
                retired_at=retired_at(context) if retired_at else None)
            if skipped is not None:
                skipped(missed)
            next_read[point.point_id] = nominal + (missed + 1) * point.poll_interval_ms / 1000
        def connection_failure(quality):
            if connected:
                return
            for point in enabled:
                nominal = next_read[point.point_id]
                if nominal <= loop.time():
                    emit(point, None, quality, nominal, initial[point.point_id])
                    advance(point, nominal, initial[point.point_id])
        try:
            for attempt in range(connection.retry_count + 1):
                try:
                    await asyncio.wait_for(client.connect(), connection.connect_timeout_ms / 1000)
                    connected = True
                    last_error = None
                    break
                except (TimeoutError, OSError, UaError, asyncio.CancelledError) as exc:
                    last_error = exc
                    if isinstance(exc, asyncio.CancelledError) or attempt == connection.retry_count:
                        raise
                    await asyncio.sleep(min(0.1 * 2 ** attempt, 1))
            # One scheduled read is one physical result. Subscription notifications
            # cannot create additional physical slots or refresh sample UTC.
            polled = [(p, client.get_node(p.opcua.node_id)) for p in enabled]
            while not self._lost.is_set():
                if refresh_plan is not None and not refresh_plan(next_read):
                    await asyncio.sleep(0.01)
                    continue
                if not polled:
                    await self._lost.wait()
                    break
                due_at = min(next_read.values())
                try:
                    await asyncio.wait_for(self._lost.wait(), max(0, due_at - loop.time()))
                    break
                except TimeoutError:
                    pass
                for point, node in polled:
                    if refresh_plan is not None and not refresh_plan(next_read):
                        break
                    if next_read[point.point_id] > loop.time():
                        continue
                    nominal = next_read[point.point_id]
                    context = before_read(point, nominal, dict(next_read)) if before_read else None
                    try:
                        data = await asyncio.wait_for(node.read_data_value(), timeout)
                        quality = quality_of(data.StatusCode)
                        value = None
                        if quality == 'GOOD':
                            try:
                                value = decode(point, data.Value.Value)
                            except (ValueError, TypeError, OverflowError):
                                quality = 'BAD_DECODE'
                        emit(point, value, quality, nominal, context)
                    except TimeoutError:
                        emit(point, None, 'BAD_TIMEOUT', nominal, context)
                    except UaStatusCodeError:
                        emit(point, None, 'BAD_PROTOCOL', nominal, context)
                    except (OSError, UaError) as exc:
                        emit(point, None, 'BAD_CONNECTION', nominal, context)
                        raise ConnectionError('BAD_CONNECTION') from exc
                    finally:
                        advance(point, nominal, context)
        except TimeoutError as exc:
            connection_failure('BAD_TIMEOUT')
            raise ConnectionError('BAD_TIMEOUT') from exc
        except asyncio.CancelledError:
            raise
        except (OSError, UaError, ConnectionError) as exc:
            connection_failure('BAD_CONNECTION')
            raise ConnectionError('BAD_CONNECTION') from last_error or exc
        finally:
            await self.close()
