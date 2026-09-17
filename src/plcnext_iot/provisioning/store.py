"""Single durable commissioning record; the PLC identity is never rewritten.

Broker settings may be updated in place, but the record always holds *effective*
values: an update request may say "keep the saved password", and the merge result
is what gets written. Persisting a request verbatim would store a blank password
next to a username, which the runtime settings refuse to load.
"""
import json
import os
from contextlib import contextmanager
from pathlib import Path
import ssl
import tempfile

from plcnext_iot.core.settings import AgentSettings, _unique
from plcnext_iot.messaging.settings import MqttSettings


FIELDS = {'gatewayId','host','port','tls','username','password','caPem'}


class SettingsError(ValueError):
    """Rejection that carries a stable code the setup page can explain."""

    def __init__(self, code='INVALID_SETTINGS'):
        super().__init__(code)
        self.code = code


def parse(payload):
    """Strict first-time record. Field set, and validation depth, stay as shipped."""
    if len(payload) > 32768:
        raise SettingsError()
    try:
        value = json.loads(payload.decode('utf-8'), object_pairs_hook=_unique)
        if set(value) != FIELDS:
            raise SettingsError()
        for field, limit in [('username',256),('password',4096),('caPem',16384)]:
            if not isinstance(value[field],str) or len(value[field]) > limit:
                raise SettingsError()
        AgentSettings(value['gatewayId'], Path('.'))
        MqttSettings(value['host'],value['port'],value['tls'],
                     username=value['username'] or None, password=value['password'] or None)
        if value['caPem']:
            if not value['tls']:
                raise SettingsError()
            ssl.create_default_context(cadata=value['caPem'])
        return value
    except (ValueError, TypeError, KeyError, UnicodeError, ssl.SSLError, RecursionError):
        raise SettingsError() from None


def parse_update(payload, existing):
    """Merge an update request against the saved record and validate the result.

    A blank password keeps the saved one (only meaningful alongside a username,
    because the runtime settings require the pair to appear together). A blank
    CA keeps the saved one unless `removeCa` is set; TLS off always clears it,
    since a CA without TLS is not a loadable setting.
    """
    if len(payload) > 32768:
        raise SettingsError()
    try:
        value = json.loads(payload.decode('utf-8'), object_pairs_hook=_unique)
        if not isinstance(value, dict) or not FIELDS <= set(value) or set(value) - FIELDS - {'removeCa'}:
            raise SettingsError()
        for field, limit in [('username',256),('password',4096),('caPem',16384)]:
            if not isinstance(value[field],str) or len(value[field]) > limit:
                raise SettingsError()
        if type(value['tls']) is not bool:
            raise SettingsError()
        if 'removeCa' in value and type(value['removeCa']) is not bool:
            raise SettingsError()
        if value['gatewayId'] != existing['gatewayId']:
            raise SettingsError('IDENTITY_IMMUTABLE')

        username = value['username'] or None
        if username is None:
            if value['password']:
                # Dropping a typed secret silently would be worse than refusing it.
                raise SettingsError('PASSWORD_WITHOUT_USERNAME')
            password = None
        elif value['password']:
            password = value['password']
        elif existing['password']:
            password = existing['password']
        else:
            raise SettingsError('PASSWORD_REQUIRED')

        if not value['tls']:
            if value['caPem']:
                raise SettingsError('CA_REQUIRES_TLS')
            ca = ''
        elif value.get('removeCa'):
            ca = ''
        else:
            ca = value['caPem'] or existing['caPem']

        AgentSettings(existing['gatewayId'], Path('.'))
        MqttSettings(value['host'],value['port'],value['tls'],username=username,password=password)
        if ca:
            ssl.create_default_context(cadata=ca)
    except SettingsError:
        raise   # already carries the code the page needs to explain it
    except (ValueError, TypeError, KeyError, UnicodeError, ssl.SSLError, RecursionError):
        raise SettingsError() from None
    return {'gatewayId':existing['gatewayId'],'host':value['host'],'port':value['port'],
            'tls':value['tls'],'username':username or '','password':password or '','caPem':ca}


def atomic_write(path, payload):
    fd, name = tempfile.mkstemp(prefix='.setup-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        if os.name != 'nt':
            descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        if os.path.exists(name):
            os.unlink(name)


class SetupStore:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = self.directory/'commissioning.json'
        self.ca_path = self.directory/'broker-ca.pem'

    def load(self):
        if not self.path.exists():
            # Never commission over databases belonging to a legacy installation.
            if any((self.directory/name).exists() for name in ('agent.db','telemetry.db')):
                raise ValueError('EXISTING_DATA_REQUIRES_MIGRATION')
            return None
        with self.path.open('rb') as stream:
            return parse(stream.read(32769))

    def save(self, payload):
        value = parse(payload)
        if self.path.exists():
            raise FileExistsError('ALREADY_CONFIGURED')
        atomic_write(self.path, json.dumps(value, ensure_ascii=False).encode('utf-8'))
        return value

    def update(self, payload):
        existing = self.load()
        if existing is None:
            raise SettingsError('NOT_CONFIGURED')
        value = parse_update(payload, existing)
        atomic_write(self.path, json.dumps(value, ensure_ascii=False).encode('utf-8'))
        return value

    def write_ca(self, value):
        """Materialise the private CA beside the record; idempotent by content.

        Called at commit time and before every supervised run, so a restored or
        hand-repaired directory converges without rewriting the file each round.
        A record without a CA must not leave a stale one behind.
        """
        pem = value['caPem']
        if not pem:
            if self.ca_path.exists():
                self.ca_path.unlink()
            return None
        payload = pem.encode('ascii')
        try:
            if self.ca_path.read_bytes() == payload:
                return str(self.ca_path)
        except OSError:
            pass
        atomic_write(self.ca_path, payload)
        return str(self.ca_path)

    def settings(self, value):
        """Pure: compute runtime settings without touching the filesystem."""
        ca = str(self.ca_path) if value['caPem'] else None
        return (AgentSettings(value['gatewayId'], self.directory),
                MqttSettings(value['host'], value['port'], value['tls'], ca,
                             value['username'] or None, value['password'] or None))

    @contextmanager
    def test_settings(self, value):
        """Runtime settings for a one-off connection test.

        The candidate CA goes to its own file: writing it over `broker-ca.pem`
        would change the certificate the running agent presents on its next
        reconnect, i.e. an uncommitted form would rewrite live state.
        """
        if not value['caPem']:
            yield self.settings(value)
            return
        fd, name = tempfile.mkstemp(prefix='.setup-ca-', dir=self.directory)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(value['caPem'].encode('ascii'))
            yield (AgentSettings(value['gatewayId'], self.directory),
                   MqttSettings(value['host'], value['port'], value['tls'], name,
                                value['username'] or None, value['password'] or None))
        finally:
            if os.path.exists(name):
                os.unlink(name)
