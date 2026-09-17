"""Serial configuration coordinator and owned startup/shutdown lifecycle."""
import asyncio
from dataclasses import dataclass

from plcnext_iot.config.store import StoreError, StoreFormatError
from plcnext_iot.contracts import Issue
from plcnext_iot.core.settings import AgentSettings
from plcnext_iot.core.storage_worker import StoreWorker
from plcnext_iot.devices.runtime import DeviceRuntime, UnsupportedDriver


class LifecycleError(RuntimeError):
    pass


@dataclass(frozen=True)
class ApplyResult:
    status: str
    active_config_version: int | None
    code: str | None = None
    issues: tuple[Issue, ...] = ()


class AgentLifecycle:
    def __init__(self, settings: AgentSettings, runtime: DeviceRuntime, *, store_factory=None):
        self.settings, self.runtime = settings, runtime
        self.state = "NEW"
        self.last_error = None
        self._active = None
        self._durable_version = 0
        self._store_factory = store_factory
        self._storage = None
        self._queue = asyncio.Queue(maxsize=settings.queue_capacity)
        self._start_task = None
        self._close_task = None
        self._worker = None
        self._closing = False
        self._failed = asyncio.Event()

    @property
    def active_config_version(self):
        return self._durable_version

    async def wait_failed(self):
        await self._failed.wait()

    def _result(self, status, code=None, issues=()):
        return ApplyResult(status, self._durable_version, code, tuple(issues))

    def _steady(self):
        if self.state == "FAILED":
            return
        if self._closing:
            self.state = "STOPPING"
        elif self._active is None:
            self.state = "WAITING_CONFIG"
        else:
            self.state = "RUNNING" if self._active.enabled else "DISABLED"

    async def _bounded(self, operation):
        return await asyncio.wait_for(operation, self.settings.operation_timeout_seconds)

    async def start(self):
        if self.state != "NEW" or self._closing:
            raise LifecycleError("A lifecycle instance can only start once.")
        self.state = "STARTING"
        self._start_task = asyncio.create_task(self._start(), name="iot-startup")
        try:
            await asyncio.shield(self._start_task)
        except asyncio.CancelledError:
            # Finish owned startup/cleanup rather than leaking an opened SQLite lock.
            await self.close()
            raise

    async def _start(self):
        self._storage = StoreWorker(self.settings.database_path, self.settings.gateway_id, self._store_factory)
        try:
            await self._storage.open()
            snapshot = await self._storage.call("load_active")
            self._durable_version = snapshot.config_version if snapshot else 0
            if snapshot is not None:
                transition = self.runtime.prepare(snapshot)
                await self._bounded(transition.activate())
                transition.publish()
                self._active = snapshot
            self._steady()
            self._worker = asyncio.create_task(self._serve(), name="iot-config-coordinator")
        except BaseException:
            await self._fatal("STARTUP_FAILED")
            await self._storage.close()
            raise LifecycleError("Agent startup failed; no running state was announced.") from None

    async def submit(self, payload: bytes) -> ApplyResult:
        if self._closing:
            return self._result("REJECTED", "STOPPING")
        if self.state not in {"WAITING_CONFIG", "RUNNING", "DISABLED", "APPLYING"}:
            return self._result("REJECTED", "NOT_RUNNING")
        if not isinstance(payload, bytes):
            return self._result("REJECTED", "INVALID_JSON")
        if len(payload) > 2 * 1024 * 1024:
            return self._result("REJECTED", "PAYLOAD_TOO_LARGE")
        future = asyncio.get_running_loop().create_future()
        try:
            self._queue.put_nowait((payload, future))
        except asyncio.QueueFull:
            return self._result("REJECTED", "RESOURCE_LIMIT")
        # Accepted work belongs to the actor, not to a network caller's lifetime.
        return await asyncio.shield(future)

    def _drain(self, code):
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            if item is not None:
                _, future = item
                if not future.done():
                    future.set_result(self._result("REJECTED", code))
            self._queue.task_done()

    async def _serve(self):
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.task_done()
                return
            payload, future = item
            try:
                result = await self._apply(payload)
            except asyncio.CancelledError:
                self._durable_version = None
                await self._fatal("COORDINATOR_CANCELLED")
                result = self._result("FAILED", "COMMIT_OUTCOME_UNKNOWN")
            except Exception:
                await self._fatal("COORDINATOR_FAILED")
                result = self._result("FAILED", "INTERNAL_ERROR")
            if not future.done():
                future.set_result(result)
            self._queue.task_done()
            if self.state == "FAILED":
                self._drain("NOT_RUNNING")
                return

    async def _apply(self, payload):
        self.state = "APPLYING" if not self._closing else "STOPPING"
        try:
            prepared = await self._storage.call("prepare", payload)
        except StoreFormatError:
            await self._fatal("STORAGE_CORRUPT")
            return self._result("FAILED", "STORAGE_ERROR")
        except StoreError:
            self._steady()
            return self._result("FAILED", "STORAGE_ERROR")
        if prepared.status == "REJECTED":
            self._steady()
            return self._result("REJECTED", prepared.code, prepared.issues)
        if prepared.status == "APPLIED":
            # Startup has already restored runtime; storage alone is insufficient.
            if self._active is None or self._active.config_version != prepared.active_config_version:
                await self._fatal("RUNTIME_VERSION_MISMATCH")
                return self._result("FAILED", "RUNTIME_VERSION_MISMATCH")
            self._steady()
            return self._result("APPLIED")

        snapshot = await self._storage.parse(payload)
        try:
            transition = self.runtime.prepare(snapshot)
        except Exception as exc:
            try:
                await self._storage.call("fail", prepared.token)
            except Exception:
                await self._fatal("STORAGE_ERROR")
                return self._result("FAILED", "STORAGE_ERROR")
            self._steady()
            code = "UNSUPPORTED_DRIVER" if isinstance(exc, UnsupportedDriver) else "PREFLIGHT_FAILED"
            return self._result("REJECTED", code)
        try:
            await self._bounded(transition.activate())
        except Exception as exc:
            code = "APPLY_TIMEOUT" if isinstance(exc, TimeoutError) else "APPLY_FAILED"
            return await self._restore(transition, prepared.token, code)

        try:
            committed = await self._storage.call("commit", prepared.token)
        except Exception:
            # A commit notification failure need not imply a failed DB commit.
            try:
                committed = await self._storage.call("load_active")
            except Exception:
                self._durable_version = None
                await self._fatal("COMMIT_OUTCOME_UNKNOWN")
                return self._result("FAILED", "COMMIT_OUTCOME_UNKNOWN")
            if (committed is None or committed.config_version != snapshot.config_version
                    or committed.content_json != snapshot.content_json):
                if committed == self._active:
                    return await self._restore(transition, prepared.token, "STORAGE_ERROR")
                self._durable_version = committed.config_version if committed else 0
                await self._fatal("UNEXPECTED_COMMITTED_VERSION")
                return self._result("FAILED", "COMMIT_OUTCOME_UNKNOWN")
        self._durable_version = committed.config_version
        transition.publish()  # No await: publish runtime and version as one loop turn.
        self._active = committed
        self._steady()
        return self._result("APPLIED")

    async def _restore(self, transition, token, code):
        try:
            await self._bounded(transition.rollback())
        except Exception:
            try:
                await self._storage.call("fail", token)
            except Exception:
                pass
            await self._fatal("ROLLBACK_FAILED")
            return self._result("FAILED", "ROLLBACK_FAILED")
        try:
            await self._storage.call("fail", token)
        except Exception:
            await self._fatal("STORAGE_ERROR")
            return self._result("FAILED", "STORAGE_ERROR")
        self._steady()
        return self._result("FAILED", code)

    async def _fatal(self, code):
        self.state, self.last_error = "FAILED", code
        try:
            await self.runtime.shutdown(self.settings.operation_timeout_seconds)
        except Exception:
            self.last_error = "SHUTDOWN_FAILED"
        self._failed.set()

    async def close(self):
        if self._close_task is None:
            self._closing = True
            if self.state != "FAILED":
                self.state = "STOPPING"
            self._close_task = asyncio.create_task(self._close(), name="iot-shutdown")
        await asyncio.shield(self._close_task)

    async def _close(self):
        if self._start_task is not None:
            try:
                await asyncio.shield(self._start_task)
            except LifecycleError:
                pass
        self._drain("STOPPING")
        if self._worker is not None and not self._worker.done():
            self._queue.put_nowait(None)
            await self._worker
        try:
            await self.runtime.shutdown(self.settings.operation_timeout_seconds)
        except Exception:
            self.state, self.last_error = "FAILED", "SHUTDOWN_FAILED"
        finally:
            if self._storage is not None:
                try:
                    await self._storage.close()
                except Exception:
                    self.state, self.last_error = "FAILED", "STORAGE_CLOSE_FAILED"
        if self.state != "FAILED":
            self.state = "STOPPED"
        else:
            self._failed.set()
