"""Injectable capture-time boundary; MQTT synchronization is a later phase."""
from dataclasses import dataclass
import time


@dataclass(frozen=True)
class CaptureTime:
    utc_ms: int | None
    time_valid: bool
    clock_revision: int = 0
    generation: int = 0
    source: str = 'HUB_MQTT'
    generation_started_mono_ms: int | None = None


class SystemRtcCompatibility:
    """Explicit legacy source, never claims platform synchronization."""
    def read(self, completed_mono_ms):
        return CaptureTime(time.time_ns() // 1_000_000, False, source='SYSTEM_RTC_COMPAT')
