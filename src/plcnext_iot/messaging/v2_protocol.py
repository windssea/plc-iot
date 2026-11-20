"""Phase 3 capability-negotiated v2 protocol helpers, revision clocks, ACK queue, and jittered backoff."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import hashlib
import time

V2_CAPABILITY_TOKENS = frozenset({
    'stateRevisionV2',
    'heartbeatRevisionV2',
    'collectionEpochV2',
    'stateEventAckV2',
    'sourceSampleSeqV2',
    'clockRevisionV2',
})


def negotiate_capabilities(agent_caps: set[str] | frozenset[str], hub_caps: set[str] | frozenset[str]) -> frozenset[str]:
    """Intersection of advertised capabilities; stays cleanly on v1 when peer has no v2 tokens."""
    return frozenset(set(agent_caps) & set(hub_caps))


def backoff_with_jitter(
    attempt: int,
    *,
    base_s: float = 0.5,
    cap_s: float = 30.0,
    device_key: str = '',
    jitter_ratio: float = 0.25,
) -> float:
    """Deterministic jittered exponential backoff preventing multi-meter reconnect storms."""
    step = max(0, int(attempt))
    raw = min(cap_s, base_s * (2 ** min(step, 16)))
    if not jitter_ratio or jitter_ratio <= 0:
        return raw
    digest = hashlib.sha256(f'{device_key}:{step}'.encode('utf-8')).digest()
    unit = int.from_bytes(digest[:4], 'big') / 0xFFFFFFFF  # [0.0, 1.0]
    offset = (unit * 2.0 - 1.0) * jitter_ratio
    return max(0.1, min(cap_s, raw * (1.0 + offset)))


class V2RevisionClock:
    """Monotonic source-side revisions decoupling ordering from wall-clock millisecond ties."""

    def __init__(self, *, boot_id: str) -> None:
        self.boot_id = boot_id
        self.heartbeat_revision = 0
        self.clock_revision = 0
        self.time_quality = 'SYNCED'
        self._state_rev: dict[str, int] = {}
        self._collection_epoch: dict[str, int] = {}
        self._sample_seq: dict[tuple[str, str], int] = {}

    def next_heartbeat_revision(self) -> int:
        self.heartbeat_revision += 1
        return self.heartbeat_revision

    def next_state_revision(self, device_id: str) -> int:
        nxt = self._state_rev.get(device_id, 0) + 1
        self._state_rev[device_id] = nxt
        return nxt

    def current_collection_epoch(self, device_id: str) -> int:
        return self._collection_epoch.get(device_id, 1)

    def advance_collection_epoch(self, device_id: str) -> int:
        nxt = self._collection_epoch.get(device_id, 1) + 1
        self._collection_epoch[device_id] = nxt
        return nxt

    def next_sample_sequence(self, device_id: str, point_id: str) -> int:
        key = (device_id, point_id)
        nxt = self._sample_seq.get(key, 0) + 1
        self._sample_seq[key] = nxt
        return nxt

    def bump_clock_revision(self, time_quality: str = 'STEPPED') -> int:
        self.clock_revision += 1
        self.time_quality = time_quality
        return self.clock_revision


@dataclass(frozen=True)
class PendingStateEvent:
    state_event_id: str
    device_id: str
    state_revision: int
    payload: dict
    created_at_ms: int
    attempts: int = 0


class StateEventAckQueue:
    """Bounded source-side queue holding critical state transitions until end-to-end Hub ACK."""

    def __init__(self, *, max_entries: int = 512) -> None:
        if max_entries < 1:
            raise ValueError('max_entries must be positive')
        self.max_entries = max_entries
        self._queue: deque[PendingStateEvent] = deque()
        self._by_id: dict[str, PendingStateEvent] = {}
        self.dropped_oldest = 0

    def enqueue(self, device_id: str, state_revision: int, payload: dict, *, now_ms: int | None = None) -> PendingStateEvent:
        event_id = f'se-{device_id}-{state_revision}'
        if event_id in self._by_id:
            return self._by_id[event_id]
        ts = int(time.time() * 1000) if now_ms is None else int(now_ms)
        if len(self._queue) >= self.max_entries:
            evicted = self._queue.popleft()
            self._by_id.pop(evicted.state_event_id, None)
            self.dropped_oldest += 1
        entry = PendingStateEvent(event_id, device_id, state_revision, dict(payload), ts)
        self._queue.append(entry)
        self._by_id[event_id] = entry
        return entry

    def ack(self, state_event_id: str) -> bool:
        entry = self._by_id.pop(state_event_id, None)
        if entry is None:
            return False
        self._queue = deque(item for item in self._queue if item.state_event_id != state_event_id)
        return True

    def pending(self) -> tuple[PendingStateEvent, ...]:
        return tuple(self._queue)

    def __len__(self) -> int:
        return len(self._queue)


def build_heartbeat_message(
    *,
    gateway_id: str,
    session_id: str,
    agent_state: str,
    active_config_version: int,
    timestamp_ms: int,
    capabilities: frozenset[str],
    clock: V2RevisionClock,
    agent_version: str = '0.9.0',
) -> dict:
    if 'heartbeatRevisionV2' in capabilities:
        return {
            'schemaVersion': 2,
            'gatewayId': gateway_id,
            'sessionId': session_id,
            'timestamp': timestamp_ms,
            'agentVersion': agent_version,
            'agentState': agent_state,
            'activeConfigVersion': active_config_version,
            'heartbeatRevision': clock.next_heartbeat_revision(),
            'clockRevision': clock.clock_revision,
            'timeQuality': clock.time_quality,
            'capabilities': sorted(capabilities),
        }
    return {
        'schemaVersion': 1,
        'gatewayId': gateway_id,
        'sessionId': session_id,
        'timestamp': timestamp_ms,
        'agentVersion': agent_version,
        'agentState': agent_state,
        'activeConfigVersion': active_config_version,
    }


def build_device_status_message(
    *,
    gateway_id: str,
    session_id: str,
    config_version: int,
    device_id: str,
    status: str,
    timestamp_ms: int,
    capabilities: frozenset[str],
    clock: V2RevisionClock,
    last_error: str | None = None,
    last_good_ts: int | None = None,
) -> dict:
    device = {
        'deviceId': device_id,
        'status': status,
        'lastError': last_error,
        'lastGoodTimestamp': last_good_ts,
    }
    if 'stateRevisionV2' in capabilities:
        rev = clock.next_state_revision(device_id)
        device['stateRevision'] = rev
        if 'collectionEpochV2' in capabilities:
            device['collectionEpoch'] = clock.current_collection_epoch(device_id)
        msg = {
            'schemaVersion': 2,
            'gatewayId': gateway_id,
            'sessionId': session_id,
            'configVersion': config_version,
            'timestamp': timestamp_ms,
            'clockRevision': clock.clock_revision,
            'timeQuality': clock.time_quality,
            'device': device,
        }
        if 'stateEventAckV2' in capabilities:
            msg['stateEventId'] = f'se-{device_id}-{rev}'
        return msg
    return {
        'schemaVersion': 1,
        'gatewayId': gateway_id,
        'sessionId': session_id,
        'configVersion': config_version,
        'timestamp': timestamp_ms,
        'device': device,
    }


def build_data_sample_entry(
    *,
    device_id: str,
    point_id: str,
    value,
    quality: str,
    timestamp_ms: int,
    capabilities: frozenset[str],
    clock: V2RevisionClock,
) -> dict:
    entry = {
        'deviceId': device_id,
        'pointId': point_id,
        'value': value,
        'quality': quality,
        'timestamp': timestamp_ms,
    }
    if 'sourceSampleSeqV2' in capabilities:
        seq = clock.next_sample_sequence(device_id, point_id)
        epoch = clock.current_collection_epoch(device_id)
        entry['collectionEpoch'] = epoch
        entry['sourceSampleSequence'] = seq
        entry['sampleId'] = f'{clock.boot_id}-{device_id}-{point_id}-{seq}'
    return entry
