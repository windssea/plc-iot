"""Process-owned logical UTC. No OS clock writes; monotonic clocks never persist."""
from dataclasses import dataclass
import json
import math
import random
import re
import threading
import time
import uuid

from plcnext_iot.contracts import validate_message

MAX_SAFE = 9007199254740991
ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}')


class TimeUnavailable(ValueError):
    pass


@dataclass(frozen=True)
class TimeToken:
    boot_id: str
    clock_revision: int
    connection_revision: int
    issued_mono_ms: int


@dataclass(frozen=True)
class TimeSnapshot:
    boot_id: str
    mono_ms: int
    utc_ms: int | None
    system_utc_ms: int | None
    uncertainty_ms: int | None
    source: str
    state: str
    time_valid: bool
    sync_age_ms: int | None
    clock_revision: int
    time_basis_ref: str | None
    connection_revision: int


class PlatformTime:
    def __init__(self, gateway, boot_id, *, mode='HUB_MQTT', basis=None, drift_ppm=None,
                 drift_evidence=None, mono=None, system=None, jitter=None):
        if not ID.fullmatch(gateway) or not ID.fullmatch(boot_id) or mode not in ('HUB_MQTT', 'SYSTEM_RTC'):
            raise ValueError('Invalid time identity/mode')
        if mode == 'HUB_MQTT' and (not isinstance(basis, str) or not ID.fullmatch(basis)
                or type(drift_ppm) not in (int, float) or not math.isfinite(drift_ppm)
                or not 0 <= drift_ppm <= 1000 or not isinstance(drift_evidence, str)
                or not drift_evidence.strip() or len(drift_evidence) > 256):
            raise ValueError('Platform time requires a basis and an evidenced drift bound')
        self.gateway, self.boot_id, self.mode = gateway, boot_id, mode
        self.basis, self.drift_ppm = basis, drift_ppm or 0
        self.drift_evidence = drift_evidence
        self._mono = mono or (lambda: time.monotonic_ns() // 1_000_000)
        self._system = system or (lambda: time.time_ns() // 1_000_000)
        self._jitter = jitter or random.uniform
        self._lock = threading.RLock()
        self.revision = self.connection_revision = self.generation = 0
        self.generation_started = self._mono()
        self.session = None
        self.session_synced = self.heartbeat_submitted = self.get_submitted = False
        self.state, self.reason = 'UNSYNCED', None
        self._utc = None
        self._last_mono = self._mono()
        self._sync_mono = None
        self._uncertainty = 0
        self._residual = 0.0
        self._floor = None
        self._last_observed_mono = self._last_mono
        self._pending = None
        self._round_start = None
        self._attempts = 0
        self._candidates = []
        self._next_sync = 0
        self._failures = 0

    @property
    def platform(self):
        return self.mode == 'HUB_MQTT'

    def read_mono(self):
        return self._mono()

    def _bump(self):
        if max(self.revision, self.connection_revision, self.generation) >= MAX_SAFE:
            raise TimeUnavailable('Time revision exhausted')
        self.revision += 1
        self.generation += 1
        self.generation_started = self._mono()

    def _invalidate(self, state, reason):
        if self.state not in ('UNSYNCED', 'EXPIRED', 'SUSPECT', 'MAINTENANCE'):
            self._bump()
        self.state, self.reason = state, reason
        self.session_synced = self.heartbeat_submitted = self.get_submitted = False
        self._pending = None
        self._next_sync = 0

    def connect(self, session):
        if not isinstance(session, str) or not ID.fullmatch(session):
            raise ValueError('Invalid session')
        with self._lock:
            if self.connection_revision >= MAX_SAFE:
                raise TimeUnavailable('Connection revision exhausted')
            self.connection_revision += 1
            self.session = session
            self.session_synced = not self.platform
            self.heartbeat_submitted = self.get_submitted = False
            self._pending = None
            self._round_start = None
            self._attempts = 0
            self._candidates = []
            self._next_sync = 0

    def disconnect(self):
        with self._lock:
            self.connect('disconnected')
            self.session = None
            self.session_synced = self.heartbeat_submitted = self.get_submitted = False

    def require_maintenance(self):
        with self._lock:
            self._invalidate('MAINTENANCE', 'ROLLBACK_REQUIRES_MAINTENANCE')

    def load_history_boundary(self, basis, utc_lower):
        with self._lock:
            if utc_lower is None:
                return
            if basis != self.basis:
                self.require_maintenance()
                return
            self._floor = max(self._floor or 0, utc_lower)
            if self._utc is not None and self._utc + self._uncertainty < self._floor:
                self.require_maintenance()

    def _advance(self, now):
        if now < self._last_observed_mono:
            # A decreasing monotonic source cannot safely extrapolate history.
            # Keep the maintenance fence across subsequent resynchronization.
            self._invalidate('MAINTENANCE', 'CLOCK_CONTINUITY_REQUIRES_MAINTENANCE')
            self._utc = None
        self._last_observed_mono = now
        if self._utc is not None:
            elapsed = max(0, now - self._last_mono)
            correction = max(-elapsed * .005, min(elapsed * .005, self._residual))
            self._utc += elapsed + correction
            self._residual -= correction
        self._last_mono = now

    def snapshot(self):
        with self._lock:
            now = self._mono()
            self._advance(now)
            age = None if self._sync_mono is None else max(0, now - self._sync_mono)
            error = None if age is None else math.ceil(self._uncertainty + age * self.drift_ppm / 1_000_000 + abs(self._residual))
            valid = (self.platform and self._utc is not None and self.state in ('SYNCED', 'HOLDOVER')
                     and age < 3_600_000 and error < 1000 and 0 <= self._utc - error
                     and self._utc + error <= MAX_SAFE)
            if self.platform and self.state in ('SYNCED', 'HOLDOVER') and not valid:
                self._invalidate('EXPIRED', 'HOLDOVER_EXPIRED')
            if valid:
                self.state = 'SYNCED' if self.session_synced else 'HOLDOVER'
            system = self._system()
            if type(system) is not int or not 0 <= system <= MAX_SAFE:
                system = None
            return TimeSnapshot(self.boot_id, now, math.floor(self._utc) if valid else None,
                                system, error, self.mode, self.state, valid, age,
                                self.revision, self.basis if self.platform else None, self.connection_revision)

    def token(self):
        s = self.snapshot()
        return TimeToken(s.boot_id, s.clock_revision, s.connection_revision, s.mono_ms)

    def valid(self, token, purpose):
        with self._lock:
            s = self.snapshot()
            if token is not None and (token.boot_id != self.boot_id or token.clock_revision != self.revision
                    or token.connection_revision != self.connection_revision
                    or not 0 <= s.mono_ms - token.issued_mono_ms < 2000):
                return False
            if purpose == 'CAPTURE':
                return True
            if purpose == 'ACK_APPLY':
                return self.session is not None
            if not (s.time_valid if self.platform else s.system_utc_ms is not None):
                return False
            if purpose == 'ENQUEUE':
                return True
            if self.session is None or not self.session_synced:
                return False
            if purpose == 'CONTROL_SEND':
                return True
            if purpose == 'SEND':
                return self.heartbeat_submitted and self.get_submitted
            raise ValueError('Unknown time purpose')

    def require(self, token, purpose):
        with self._lock:
            if self.platform and token is None and purpose in ('ENQUEUE','CONTROL_SEND','SEND'):
                raise TimeUnavailable('Explicit current time token required')
            if not self.valid(token, purpose):
                raise TimeUnavailable('Time context invalid for ' + purpose)
            return self.snapshot()

    def control_submitted(self, kind, token):
        with self._lock:
            self.require(token, 'CONTROL_SEND')
            if kind == 'heartbeat':
                self.heartbeat_submitted = True
            elif kind == 'config/get' and self.heartbeat_submitted:
                self.get_submitted = True

    def read(self, completed_mono_ms):
        from plcnext_iot.core.sample_time import CaptureTime
        with self._lock:
            s = self.snapshot()
            # Capture happens synchronously on the owner loop. Never backfill old invalid samples.
            valid = s.time_valid and self.generation_started <= completed_mono_ms <= s.mono_ms + 1
            return CaptureTime(s.utc_ms if valid else s.system_utc_ms if not self.platform else None,
                               valid, self.revision, self.generation,
                               'HUB_MQTT' if self.platform else 'SYSTEM_RTC_COMPAT', self.generation_started)

    def _finish_round(self, now):
        if self._candidates:
            aligned = [c[0] + now - c[1] for c in self._candidates]
            consistent = max(aligned) - min(aligned) <= 500
            good = [c for c in self._candidates if c[3] <= 500]
            if consistent and (len(self._candidates) >= 3 or good):
                chosen = min(self._candidates if len(self._candidates) >= 3 else good, key=lambda c: c[2])
                self._adopt(chosen[0] + now - chosen[1],
                            chosen[4]+(now-chosen[1])*self.drift_ppm/1_000_000,now,len(self._candidates)<3)
                self._round_start = None
                self._pending = None
                self._candidates = []
                self._failures = 0
                return
            self.reason = 'INCONSISTENT_SAMPLES'
        else:
            self.reason = 'TIMEOUT'
        self._failures = min(self._failures + 1, 5)
        delay = (5000, 10000, 20000, 40000, 60000)[self._failures - 1]
        self._next_sync = now + min(60000, round(delay * self._jitter(1, 1.2)))
        self._round_start = None
        self._pending = None
        self._candidates = []
        if self._utc is None and self.state != 'MAINTENANCE':
            self.state = 'UNSYNCED'

    def _adopt(self, target, error, now, single):
        self._advance(now)
        if self._floor is not None and target + error < self._floor:
            self.require_maintenance()
            return
        if self._utc is not None and abs(target - self._utc) > 1000:
            self._invalidate('SUSPECT', 'CLOCK_DISCONTINUITY')
            # First large deviation only closes gates; a following bounded round confirms it.
            if not getattr(self, '_suspect_confirm', False):
                self._suspect_confirm = True
                self._next_sync = now
                return
            if target + error < self._utc:
                self.require_maintenance()
                return
            self._bump()
            self._utc = None
        if self._utc is None or self.state in ('EXPIRED', 'UNSYNCED', 'SYNCING'):
            self._bump()
            self._utc = target
            self._residual = 0
        else:
            self._residual = target - self._utc
        self._suspect_confirm = False
        self._uncertainty, self._sync_mono = error, now
        self.state, self.reason = 'SYNCED', None
        self.session_synced = self.session is not None
        self._next_sync = now + (10000 if single else round(300000 * self._jitter(.9, 1.1)))

    def request(self):
        """Called immediately before synchronous SDK submit; m1 includes SDK/network queueing."""
        with self._lock:
            now = self.snapshot().mono_ms
            if not self.platform or self.session is None or self.state == 'MAINTENANCE':
                return None
            if self._round_start is not None and (now >= self._round_start + 30000
                    or (self._attempts >= 10 and self._pending is None)):
                self._finish_round(now)
            if self._pending is not None:
                if now < self._pending['clientMonoMs'] + 2000:
                    return None
                self._pending = None
                if self._attempts >= 10:
                    self._finish_round(now)
            if now < self._next_sync:
                return None
            if self._round_start is None:
                self._round_start, self._attempts, self._candidates = now, 0, []
                if self._utc is None:
                    self.state = 'SYNCING'
            if now >= self._round_start + 30000 or self._attempts >= 10:
                self._finish_round(now)
                return None
            self._pending = dict(schemaVersion=1, gatewayId=self.gateway, requestId='time-' + uuid.uuid4().hex,
                                 bootId=self.boot_id, sessionId=self.session, clientMonoMs=now)
            self._attempts += 1
            return json.dumps(self._pending, separators=(',', ':')).encode()

    def response(self, payload, *, received_mono_ms, session, retained=False):
        with self._lock:
            now = self._mono()
            request = self._pending
            if retained or session != self.session or not request:
                self.reason = 'SOURCE_MISMATCH' if retained or session != self.session else 'STALE_REQUEST'
                return False
            if now >= self._round_start + 30000 or now >= request['clientMonoMs'] + 2000:
                self._pending = None
                self.reason = 'STALE_REQUEST'
                return False
            if validate_message('timeResponse', payload, expected_gateway_id=self.gateway):
                self.reason = 'INVALID_RESPONSE'
                return False
            message = json.loads(payload)
            if any(message[k] != v for k, v in request.items()):
                self.reason = 'STALE_REQUEST'
                return False
            rtt = received_mono_ms - request['clientMonoMs']
            processing = message['serverSentAt'] - message['serverReceivedAt']
            path = rtt - processing
            if not (0 <= rtt < 2000 and 0 <= processing <= 500 and path >= -50
                    and received_mono_ms <= now):
                self.reason = 'HIGH_DELAY'
                self._pending = None
                return False
            target = message['serverSentAt'] + max(0, path) / 2
            # Cover asymmetry, tolerated negative path, timer quantization and callback/dispatch sampling.
            error = max(0, path) / 2 + max(0, -path) + 22 + (now - received_mono_ms) * self.drift_ppm / 1_000_000
            if not (0 <= target - error and target + error <= MAX_SAFE):
                self.reason = 'INVALID_RESPONSE'
                self._pending = None
                return False
            self._candidates.append((target, received_mono_ms, max(0, path), rtt, error))
            self._pending = None
            if len(self._candidates) >= 3:
                self._finish_round(now)
            return True

    def diagnostics(self):
        from dataclasses import asdict
        with self._lock:
            return dict(asdict(self.snapshot()), reason=self.reason, session_synced=self.session_synced,
                        control_ready=self.heartbeat_submitted and self.get_submitted,
                        attempts=self._attempts, pending_request=self._pending is not None,
                        drift_evidence=self.drift_evidence)
