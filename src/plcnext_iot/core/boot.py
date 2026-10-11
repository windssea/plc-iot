"""Identity and counters owned by one Agent process, across local reloads."""
import time
import uuid


MAX_SEQUENCE = 9007199254740991


class ProcessBootContext:
    def __init__(self, boot_id=None, *, started_at=None):
        self.boot_id = boot_id or 'boot-' + uuid.uuid4().hex
        self.started_at = time.monotonic() if started_at is None else started_at
        self._sequence = 0

    @property
    def sequence(self):
        return self._sequence

    def next_sequence(self):
        # Allocated when a new immutable payload is prepared. Failed durable
        # enqueue may leave gaps; retrying existing outbox bytes never allocates.
        if self._sequence >= MAX_SEQUENCE:
            raise ValueError('Telemetry sequence exhausted for this process boot')
        self._sequence += 1
        return self._sequence
