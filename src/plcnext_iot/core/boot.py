"""Identity and counters owned by one Agent process, across local reloads."""
import time
import uuid


MAX_SEQUENCE = 9007199254740991


class ProcessBootContext:
    def __init__(self, boot_id=None, *, started_at=None):
        self.boot_id = boot_id or 'boot-' + uuid.uuid4().hex
        self.started_at = time.monotonic() if started_at is None else started_at
        self._sequence = 0
        self._sample_sequence = 0
        self.sampling_snapshot = None
        self.sampling_generation = 0
        self.sampling_published_at = 0
        self.point_sources = {}
        self.point_sources_owner = None
        self.report_checkpoint = None
        self.outbox_session = None
        self.telemetry_closing = None
        self.telemetry_owner = None

    @property
    def sequence(self):
        return self._sequence

    def next_sample_sequence(self):
        if self._sample_sequence >= MAX_SEQUENCE:
            raise ValueError('Sample sequence exhausted for this process boot')
        self._sample_sequence += 1
        return self._sample_sequence

    def next_sequence(self):
        # Allocated when a new immutable payload is prepared. Failed durable
        # enqueue may leave gaps; retrying existing outbox bytes never allocates.
        if self._sequence >= MAX_SEQUENCE:
            raise ValueError('Telemetry sequence exhausted for this process boot')
        self._sequence += 1
        return self._sequence
