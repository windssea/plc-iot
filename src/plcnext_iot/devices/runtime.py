"""Transactional ownership of restartable device runners, without protocol I/O.

Factories must be side-effect-free. Runner start/stop must be cancellation-
cooperative; stop is idempotent and a stopped old runner must be restartable.
start schedules local work, not necessarily a successful remote connection.
"""
import asyncio
from dataclasses import dataclass
from types import MappingProxyType
from typing import Callable, Protocol

from plcnext_iot.config.models import ConfigSnapshot, DeviceConfig
from plcnext_iot.devices.budget import ReadBudget


class DeviceRunner(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class UnsupportedDriver(RuntimeError):
    pass


class RuntimeRecoveryError(RuntimeError):
    pass


@dataclass(frozen=True)
class SampleContext:
    config_version: int
    device: DeviceConfig


def _execution_key(device):
    # Reporting/name/unit belong to the published snapshot, not driver lifetime.
    return (device.protocol, device.connection,
            tuple((p.point_id, p.data_type, p.poll_interval_ms, p.scale, p.offset, p.modbus)
                  for p in device.points if p.enabled))


class DeviceRuntime:
    def __init__(self, factory: Callable[[DeviceConfig], DeviceRunner] | None = None):
        self._factory = factory
        self.read_budget = ReadBudget()
        self._runners = {}
        self._managed = {}
        self._pending = None
        self.snapshot: ConfigSnapshot | None = None

    @property
    def runners(self):
        return MappingProxyType(self._runners)

    def sample_context(self, device_id: str, runner: DeviceRunner) -> SampleContext | None:
        """Return the published configuration only for its current runner.

        Call on the owning event loop. Capture with the sample and recheck before
        enqueueing; candidates and replaced runners have no publication rights.
        """
        if self.snapshot is None or self._runners.get(device_id) is not runner:
            return None
        device = next(d for d in self.snapshot.devices if d.device_id == device_id)
        return SampleContext(self.snapshot.config_version, device)

    def prepare(self, snapshot: ConfigSnapshot):
        if self._pending is not None:
            raise RuntimeError("A runtime transition is already pending.")
        devices = {d.device_id: d for d in snapshot.devices
                   if snapshot.enabled and d.enabled and any(p.enabled for p in d.points)}
        old = {d.device_id: d for d in self.snapshot.devices} if self.snapshot else {}
        proposed = {}
        for key, device in devices.items():
            if key in self._runners and _execution_key(old[key]) == _execution_key(device):
                proposed[key] = self._runners[key]
            else:
                if self._factory is None:
                    raise UnsupportedDriver("No device driver factory is installed.")
                proposed[key] = self._factory(device)
        if len({id(runner) for runner in proposed.values()}) != len(proposed):
            raise RuntimeError("Each device requires its own runner instance.")
        for runner in proposed.values():
            self._managed[id(runner)] = runner
        transition = RuntimeTransition(self, snapshot, proposed)
        self._pending = transition
        return transition

    async def shutdown(self, timeout):
        failures = []
        for key, runner in list(self._managed.items()):
            try:
                await asyncio.wait_for(runner.stop(), timeout)
                self._managed.pop(key, None)
            except Exception:
                failures.append(key)
        if failures:
            raise RuntimeRecoveryError("One or more device runners could not stop.")
        self._runners = {}
        self._pending = None
        self.snapshot = None


class RuntimeTransition:
    def __init__(self, runtime, snapshot, proposed):
        self.runtime, self.snapshot, self.proposed = runtime, snapshot, proposed
        self.old = dict(runtime._runners)
        self.stopped = []
        self.started = []
        self.activated = False

    async def activate(self):
        for key, runner in self.old.items():
            if self.proposed.get(key) is not runner:
                self.stopped.append(runner)  # stop may partially succeed before failing.
                await runner.stop()
        for key, runner in self.proposed.items():
            if self.old.get(key) is not runner:
                self.started.append(runner)  # includes partially successful start.
                await runner.start()
        self.activated = True

    async def rollback(self):
        # Candidate must be fully stopped before any old runner restarts.
        for runner in reversed(self.started):
            await runner.stop()
        for runner in self.stopped:
            await runner.stop()
            await runner.start()
        for key, runner in self.proposed.items():
            if self.old.get(key) is not runner:
                self.runtime._managed.pop(id(runner), None)
        self.runtime._pending = None

    def publish(self):
        """Synchronous commit barrier: no awaits, callbacks or external I/O."""
        if not self.activated or self.runtime._pending is not self:
            raise RuntimeError("Runtime transition is not ready for publication.")
        self.runtime._runners = self.proposed
        self.runtime.snapshot = self.snapshot
        for runner in self.stopped:
            self.runtime._managed.pop(id(runner), None)
        self.runtime._pending = None
