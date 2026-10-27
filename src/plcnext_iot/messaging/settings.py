"""Separate northbound bootstrap, with environment-referenced credentials."""
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import math

from plcnext_iot.messaging.messages import _unique


@dataclass(frozen=True)
class MqttSettings:
    host: str
    port: int = 8883
    tls: bool = True
    ca_file: str | None = None
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    keepalive: int = 30
    time_mode: str = 'SYSTEM_RTC'
    time_basis_ref: str | None = None
    drift_ppm: float | None = None
    drift_evidence: str | None = None

    def __post_init__(self):
        if not isinstance(self.host, str) or not self.host.strip() or len(self.host) > 253 or any(c.isspace() for c in self.host):
            raise ValueError('Invalid broker host')
        if type(self.port) is not int or not 1 <= self.port <= 65535 or type(self.tls) is not bool:
            raise ValueError('Invalid broker port/TLS setting')
        if type(self.keepalive) is not int or not 10 <= self.keepalive <= 300:
            raise ValueError('Invalid keepalive')
        if bool(self.username) != (self.password is not None):
            raise ValueError('Both username and password reference are required')
        if self.ca_file is not None and not self.tls:
            raise ValueError('CA requires TLS')
        if self.time_mode not in ('SYSTEM_RTC','HUB_MQTT'):
            raise ValueError('Invalid time mode')
        if self.time_mode=='HUB_MQTT':
            if (not isinstance(self.time_basis_ref,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}',self.time_basis_ref)
                    or type(self.drift_ppm) not in (int,float) or not math.isfinite(self.drift_ppm)
                    or not 0<=self.drift_ppm<=1000 or not isinstance(self.drift_evidence,str)
                    or not self.drift_evidence.strip() or len(self.drift_evidence)>256):
                raise ValueError('Platform time requires a basis and evidenced drift bound')

    @classmethod
    def from_file(cls, path):
        path = Path(path).resolve()
        try:
            with path.open('rb') as stream:
                payload = stream.read(16385)
            if len(payload) > 16384:
                raise ValueError('Oversize')
            value = json.loads(payload.decode('utf-8'),object_pairs_hook=_unique)
            allowed = {'host','port','tls','caFile','username','passwordEnv','keepalive','time'}
            if not isinstance(value,dict) or set(value) - allowed or 'host' not in value:
                raise ValueError('Invalid fields')
            password = None
            if 'passwordEnv' in value:
                env = value['passwordEnv']
                if not isinstance(env,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',env):
                    raise ValueError('Invalid environment reference')
                password = os.environ[env]
            ca = value.get('caFile')
            if ca is not None and (not isinstance(ca,str) or not ca.strip()):
                raise ValueError('Invalid CA path')
            username = value.get('username')
            if username is not None and (not isinstance(username,str) or not username or len(username)>256):
                raise ValueError('Invalid username')
            timing=value.get('time',{})
            if not isinstance(timing,dict) or set(timing)-{'mode','timeBasisRef','maxDriftPpm','driftEvidence'}:
                raise ValueError('Invalid time settings')
            return cls(value['host'],value.get('port',8883),value.get('tls',True),
                       str(path.parent / ca) if ca else None,username,password,value.get('keepalive',30),
                       timing.get('mode','SYSTEM_RTC'),timing.get('timeBasisRef'),timing.get('maxDriftPpm'),timing.get('driftEvidence'))
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            raise ValueError('MQTT settings could not be read or failed validation') from None
