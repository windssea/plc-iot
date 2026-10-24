"""Small bounded HTTP commissioning surface; broker settings may be revised.

The PLC identity stays immutable after the first save, but the broker address,
port, TLS mode, credentials and private CA can be updated. A revision is
reported to the supervisor through a monotonic counter, never a level: an
`Event` would stay set and re-trigger, and clearing one would swallow a change
that landed between two rounds.
"""
import asyncio
import json
from pathlib import Path
import secrets
import re
from urllib.parse import unquote, urlsplit, parse_qs

from plcnext_iot.messaging.transport import PahoConnection
from plcnext_iot.provisioning.store import SettingsError, parse, parse_update
from plcnext_iot.provisioning.devices import ConfigUnavailable, read_active, metadata, summary, detail
from plcnext_iot.provisioning.samples import read_samples


CONFLICT_CODES = ('IDENTITY_IMMUTABLE', 'NOT_CONFIGURED')


class SetupServer:
    def __init__(self, store, value=None):
        self.store, self.value = store, value
        self.ready = asyncio.Event()
        if value is not None:
            self.ready.set()
        self.token = secrets.token_urlsafe(32)
        # change_seq bumps on every saved revision; applied_seq trails it until
        # the supervisor hands that revision to the agent.
        self.change_seq = 0
        self.applied_seq = 0
        self._mutation = asyncio.Lock()
        self._slots = asyncio.Semaphore(16)
        self._clients = set()
        self.server = None
        # One borrowed lifecycle pointer; its runners already own latest values.
        self._runtime_agent = None

    def set_runtime(self, agent):
        """Called synchronously on this loop at agent start/end, never per sample."""
        self._runtime_agent = agent

    async def start(self, host, port, ssl=None):
        self.server = await asyncio.start_server(self.handle, host, port, limit=32768, ssl=ssl)

    async def close(self):
        self.set_runtime(None)
        self.server.close()
        for task in tuple(self._clients):
            task.cancel()
        await asyncio.gather(*self._clients, return_exceptions=True)
        await self.server.wait_closed()

    def _public(self):
        """Saved settings without either secret: credentials are never echoed."""
        counters = dict(changeSeq=self.change_seq, appliedSeq=self.applied_seq)
        if self.value is None:
            return dict(configured=False, **counters)
        return dict(configured=True, gatewayId=self.value['gatewayId'], host=self.value['host'],
                    port=self.value['port'], tls=self.value['tls'], username=self.value['username'],
                    passwordSet=bool(self.value['password']), caSet=bool(self.value['caPem']),
                    **counters)

    def _candidate(self, body, existing):
        """What a request would produce, validated but not written."""
        return parse(body) if existing is None else parse_update(body, existing)

    async def _devices(self, path):
        """Only locally committed snapshots; this endpoint never connects southbound."""
        parts = urlsplit(path)
        device_id = None
        expected = None
        if parts.path != '/api/devices':
            encoded = parts.path[len('/api/devices/'):]
            # The wire contract constrains device IDs to one safe identifier.
            # Decode exactly once and reject separators, malformed escapes and
            # suffixes instead of using any request value as a filesystem path.
            if re.search(r'%(?![0-9A-Fa-f]{2})', encoded):
                return 404, None, dict(code='NOT_FOUND')
            try:
                device_id = unquote(encoded, errors='strict')
            except UnicodeError:
                return 404, None, dict(code='NOT_FOUND')
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', device_id):
                return 404, None, dict(code='NOT_FOUND')
        query = parse_qs(parts.query, keep_blank_values=True)
        if query:
            if (device_id is None or set(query) != {'configVersion'}
                    or len(query['configVersion']) != 1
                    or not re.fullmatch(r'[1-9][0-9]{0,15}', query['configVersion'][0])):
                return 400, None, dict(code='INVALID_REQUEST')
            expected = int(query['configVersion'][0])
        if parts.fragment:
            return 400, None, dict(code='INVALID_REQUEST')
        gateway_id = self.value['gatewayId'] if self.value is not None else None
        try:
            snapshot = (await asyncio.to_thread(read_active, self.store.directory, gateway_id)
                        if gateway_id is not None else None)
        except ConfigUnavailable:
            return 503, None, dict(code='CONFIG_UNAVAILABLE')
        result = metadata(gateway_id, snapshot)
        if expected is not None and result['configVersion'] != expected:
            return 409, None, dict(code='CONFIG_CHANGED', **result)
        if device_id is None:
            result['devices'] = [summary(d) for d in snapshot.devices] if snapshot else []
            return 200, None, result
        device = next((d for d in snapshot.devices if d.device_id == device_id), None) if snapshot else None
        if device is None:
            return 404, None, dict(code='DEVICE_NOT_FOUND', **result)
        result['device'] = detail(device)
        return 200, None, result

    def _samples(self, path):
        parts = urlsplit(path)
        encoded = parts.path[len('/api/devices/'):-len('/samples')]
        if re.search(r'%(?![0-9A-Fa-f]{2})', encoded):
            return 404, None, dict(code='NOT_FOUND')
        try:
            device_id = unquote(encoded, errors='strict')
        except UnicodeError:
            return 404, None, dict(code='NOT_FOUND')
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', device_id):
            return 404, None, dict(code='NOT_FOUND')
        query = parse_qs(parts.query, keep_blank_values=True)
        if (parts.fragment or set(query) != {'configVersion'}
                or len(query['configVersion']) != 1
                or not re.fullmatch(r'[1-9][0-9]{0,15}', query['configVersion'][0])):
            return 400, None, dict(code='INVALID_REQUEST')
        gateway_id = self.value['gatewayId'] if self.value is not None else None
        # This call intentionally contains no await: snapshot/version/runners
        # are observed in one owning-loop turn, with zero SQLite/southbound I/O.
        code, result = read_samples(self._runtime_agent, gateway_id, device_id,
                                    int(query['configVersion'][0]), asyncio.get_running_loop().time())
        return code, None, result

    async def route(self, method, path, headers, body):
        if method == 'GET' and path == '/':
            return 200, 'text/html; charset=utf-8', (Path(__file__).parent/'web/index.html').read_bytes()
        if method == 'GET' and path == '/app.js':
            return 200, 'text/javascript; charset=utf-8', (Path(__file__).parent/'web/app.js').read_bytes()
        if method == 'GET' and path == '/style.css':
            return 200, 'text/css; charset=utf-8', (Path(__file__).parent/'web/style.css').read_bytes()
        if method == 'GET' and path == '/api/status':
            return 200, None, dict(configured=self.value is not None, token=self.token,
                                   changeSeq=self.change_seq, appliedSeq=self.applied_seq)
        if method == 'GET' and path == '/api/config':
            return 200, None, self._public()
        if method == 'GET' and path == '/api/diagnostics/buffer':
            telemetry=getattr(self._runtime_agent,'telemetry',None)
            if telemetry is None:
                return 503,None,dict(code='DIAGNOSTICS_UNAVAILABLE')
            return 200,None,telemetry.diagnostics_snapshot()
        if (method == 'GET' and path.startswith('/api/devices/')
                and re.fullmatch(r'/api/devices/[^/]+/samples', urlsplit(path).path)):
            return self._samples(path)
        if method == 'GET' and (path == '/api/devices' or path.startswith('/api/devices/')
                                or path.startswith('/api/devices?')):
            return await self._devices(path)
        if method != 'POST' or path not in ('/api/test','/api/setup'):
            return 404, None, dict(code='NOT_FOUND')
        if not secrets.compare_digest(headers.get('x-setup-token',''), self.token):
            return 403, None, dict(code='FORBIDDEN')
        if not headers.get('content-type','').startswith('application/json'):
            return 415, None, dict(code='JSON_REQUIRED')
        origin = headers.get('origin')
        if origin and origin not in ('http://'+headers.get('host',''), 'https://'+headers.get('host','')):
            return 403, None, dict(code='FORBIDDEN')
        if self._mutation.locked():
            return 409, None, dict(code='BUSY')
        async with self._mutation:
            existing = self.value
            try:
                if path == '/api/test':
                    # Test the values a save would produce, with a distinct
                    # non-persistent client ID and a CA file of its own.
                    with self.store.test_settings(self._candidate(body, existing)) as (_, mqtt):
                        connection = PahoConnection(mqtt, 'setup-'+secrets.token_hex(8), [])
                        try:
                            await connection.open()
                        finally:
                            await connection.close()
                    return 200, None, dict(code='CONNECTED')
                if existing is None:
                    value = self.store.save(body)
                    self.store.write_ca(value)
                    self.value = value
                    self.ready.set()
                    return 201, None, dict(code='SAVED')
                # No await between the atomic replace and the counter bump, so a
                # saved revision is one indivisible step to the supervisor. Saving
                # the values already in force is not a revision: no reload.
                value = self.store.update(body)
                self.store.write_ca(value)
                self.value = value
                if value != existing:
                    self.change_seq += 1
                return 200, None, dict(code='UPDATED', changeSeq=self.change_seq)
            except SettingsError as exc:
                return (409 if exc.code in CONFLICT_CODES else 400), None, dict(code=exc.code)
            except FileExistsError:
                # Unreachable while the mutation lock serializes writers; kept so
                # a lost first-save race answers instead of reporting a save error.
                return 409, None, dict(code='ALREADY_CONFIGURED')
            except (OSError, TimeoutError, ConnectionError):
                return 503, None, dict(code='CONNECTION_FAILED' if path == '/api/test' else 'SAVE_FAILED')

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self._clients.add(task)
        acquired = False
        try:
            if self._slots.locked():
                return
            await self._slots.acquire()
            acquired = True
            async with asyncio.timeout(15):
                raw = await reader.readuntil(b'\r\n\r\n')
                lines = raw.decode('ascii').split('\r\n')
                method, path, protocol = lines[0].split(' ')
                headers = {}
                for line in lines[1:-2]:
                    key, value = line.split(':',1)
                    key = key.lower()
                    if key in headers:
                        raise ValueError()
                    headers[key] = value.strip()
                length = int(headers.get('content-length','0'))
                if not 0 <= length <= 32768 or 'transfer-encoding' in headers or protocol != 'HTTP/1.1':
                    raise ValueError()
                body = await reader.readexactly(length)
                code, kind, result = await self.route(method,path,headers,body)
                payload = json.dumps(result).encode() if kind is None else result
                kind = kind or 'application/json'
                writer.write((f'HTTP/1.1 {code} Response\r\nContent-Type: {kind}\r\nContent-Length: {len(payload)}\r\n'
                    'Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n'
                    "Content-Security-Policy: default-src 'self'; frame-ancestors 'none'; base-uri 'none'\r\n"
                    'Connection: close\r\n\r\n').encode()+payload)
                await writer.drain()
        except (ValueError, UnicodeError, asyncio.IncompleteReadError, asyncio.LimitOverrunError,
                TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass
            if acquired:
                self._slots.release()
            self._clients.discard(task)
