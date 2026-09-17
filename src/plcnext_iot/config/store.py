"""Durable preparation/commit boundary, separate from actual device activation.

One owner, one thread. A future asyncio service must run this store on a dedicated
worker thread, not on the event loop. Never send APPLIED solely after prepare().
"""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
import uuid

from plcnext_iot.contracts import Issue
from plcnext_iot.config.locking import StoreInUse, StoreLock
from plcnext_iot.config.models import ConfigSnapshot, ConfigValidationError

APPLICATION_ID = 0x50494F54
SCHEMA_VERSION = 1


class StoreError(RuntimeError):
    pass


class StoreIdentityError(StoreError):
    pass


class StoreFormatError(StoreError):
    pass


class InvalidCandidate(StoreError):
    pass


@dataclass(frozen=True)
class PrepareResult:
    status: str
    active_config_version: int
    token: str | None = None
    code: str | None = None
    issues: tuple[Issue, ...] = ()


class ConfigStore:
    def __init__(self, path: Path, gateway_id: str, *, max_snapshots: int = 32,
                 max_requests: int = 4096, max_payload_bytes: int = 64 * 1024 * 1024):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", gateway_id):
            raise ValueError("Invalid local gateway identity.")
        if any(type(v) is not int or v < 1 for v in (max_snapshots, max_requests, max_payload_bytes)):
            raise ValueError("Configuration storage limits must be positive integers.")
        self.gateway_id = gateway_id
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._limits = (max_snapshots, max_requests, max_payload_bytes)
        self._lock = StoreLock(self.path.with_suffix(self.path.suffix + ".lock"))
        self._conn = None
        try:
            self._conn = sqlite3.connect(self.path, isolation_level=None, timeout=5)
            self._conn.row_factory = sqlite3.Row
            self._initialize()
        except BaseException as exc:
            self.close()
            if isinstance(exc, sqlite3.Error):
                raise StoreError("Configuration database could not be opened.") from None
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def close(self):
        try:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
        finally:
            self._lock.close()

    @contextmanager
    def _transaction(self):
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            yield
            self._conn.execute("COMMIT")
        except BaseException as exc:
            if self._conn.in_transaction:
                self._conn.rollback()
            if isinstance(exc, sqlite3.Error):
                raise StoreError("Configuration transaction failed; it was not committed.") from None
            raise

    def _initialize(self):
        c = self._conn
        application_id = c.execute("PRAGMA application_id").fetchone()[0]
        version = c.execute("PRAGMA user_version").fetchone()[0]
        tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        new = not tables and application_id == 0 and version == 0
        if not new:
            if application_id != APPLICATION_ID or version != SCHEMA_VERSION:
                raise StoreFormatError("Unknown configuration database format/version.")
            row = c.execute("SELECT gateway_id FROM metadata WHERE id=1").fetchone()
            if row is None or row[0] != self.gateway_id:
                raise StoreIdentityError("Database belongs to another PLC identity.")
        c.execute("PRAGMA foreign_keys=ON")
        if c.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise StoreFormatError("Configuration database integrity check failed.")
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=FULL")
        with self._transaction():
            if new:
                c.execute("""CREATE TABLE snapshots (
                    version INTEGER PRIMARY KEY CHECK(version>0), payload TEXT NOT NULL,
                    content_hash TEXT NOT NULL, state TEXT NOT NULL
                    CHECK(state IN ('PREPARED','COMMITTED','FAILED','INTERRUPTED')),
                    token TEXT UNIQUE)""")
                c.execute("""CREATE TABLE metadata (
                    id INTEGER PRIMARY KEY CHECK(id=1), gateway_id TEXT NOT NULL,
                    active_version INTEGER REFERENCES snapshots(version),
                    previous_version INTEGER REFERENCES snapshots(version))""")
                c.execute("""CREATE TABLE requests (
                    message_id TEXT PRIMARY KEY, version INTEGER NOT NULL REFERENCES snapshots(version),
                    status TEXT NOT NULL CHECK(status IN ('PREPARED','APPLIED','FAILED','INTERRUPTED')))""")
                c.execute("INSERT INTO metadata(id,gateway_id) VALUES(1,?)", (self.gateway_id,))
                c.execute(f"PRAGMA application_id={APPLICATION_ID}")
                c.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            # Only the OS-lock owner may recover interrupted preparations.
            c.execute("UPDATE requests SET status='INTERRUPTED' WHERE status='PREPARED'")
            c.execute("UPDATE snapshots SET state='INTERRUPTED',token=NULL WHERE state='PREPARED'")
            self.load_active()  # Validate committed content before allowing any new writes.

    def _versions(self):
        row = self._conn.execute("SELECT active_version,previous_version FROM metadata WHERE id=1").fetchone()
        return row[0] or 0, row[1] or 0

    @property
    def previous_version(self) -> int:
        return self._versions()[1]

    def _decode(self, row) -> ConfigSnapshot:
        try:
            snapshot = ConfigSnapshot.parse(row["payload"].encode("utf-8"), self.gateway_id)
        except (ConfigValidationError, UnicodeError):
            raise StoreFormatError("Stored configuration is not valid; recovery stopped.") from None
        if snapshot.config_version != row["version"] or snapshot.content_hash != row["content_hash"]:
            raise StoreFormatError("Stored configuration identity/content check failed.")
        return snapshot

    def load_active(self) -> ConfigSnapshot | None:
        active, _ = self._versions()
        if not active:
            return None
        row = self._conn.execute("SELECT * FROM snapshots WHERE version=?", (active,)).fetchone()
        if row is None or row["state"] != "COMMITTED":
            raise StoreFormatError("Active pointer does not reference committed configuration.")
        return self._decode(row)

    def prepare(self, payload: bytes) -> PrepareResult:
        try:
            snapshot = ConfigSnapshot.parse(payload, self.gateway_id)
        except ConfigValidationError as exc:
            return PrepareResult("REJECTED", self._versions()[0], code=exc.issues[0].code, issues=exc.issues)
        with self._transaction():
            return self._prepare(snapshot)

    def _prepare(self, snapshot):
        c = self._conn
        active, _ = self._versions()

        def reject(code):
            return PrepareResult("REJECTED", active, code=code)

        request = c.execute("SELECT version FROM requests WHERE message_id=?", (snapshot.message_id,)).fetchone()
        if request and request[0] != snapshot.config_version:
            return reject("MESSAGE_ID_CONFLICT")
        if snapshot.config_version < active:
            return reject("STALE_VERSION")
        saved = c.execute("SELECT * FROM snapshots WHERE version=?", (snapshot.config_version,)).fetchone()
        if saved and self._decode(saved).content_json != snapshot.content_json:
            return reject("VERSION_CONFLICT")
        if snapshot.config_version != active and c.execute(
                "SELECT 1 FROM snapshots WHERE state='PREPARED' AND version<>?",
                (snapshot.config_version,)).fetchone():
            return reject("RESOURCE_LIMIT")
        max_snapshots, max_requests, max_bytes = self._limits
        if not request and c.execute("SELECT COUNT(*) FROM requests").fetchone()[0] >= max_requests:
            return reject("RESOURCE_LIMIT")
        if not saved:
            count, size = c.execute("SELECT COUNT(*),COALESCE(SUM(length(CAST(payload AS BLOB))),0) FROM snapshots").fetchone()
            if count >= max_snapshots or size + len(snapshot.wire_json.encode("utf-8")) > max_bytes:
                return reject("RESOURCE_LIMIT")
        applied = snapshot.config_version == active
        status = "APPLIED" if applied else "PREPARED"
        token = None if applied else (saved["token"] if saved and saved["state"] == "PREPARED" else uuid.uuid4().hex)
        if not saved:
            c.execute("INSERT INTO snapshots VALUES(?,?,?,?,?)",
                      (snapshot.config_version, snapshot.wire_json, snapshot.content_hash, "PREPARED", token))
        elif not applied:
            c.execute("UPDATE snapshots SET state='PREPARED',token=? WHERE version=?", (token, snapshot.config_version))
        c.execute("INSERT INTO requests VALUES(?,?,?) ON CONFLICT(message_id) DO UPDATE SET status=excluded.status",
                  (snapshot.message_id, snapshot.config_version, status))
        return PrepareResult(status, active, token)

    def _candidate(self, token):
        if not isinstance(token, str) or not token:
            raise InvalidCandidate("A valid preparation token is required.")
        row = self._conn.execute("SELECT * FROM snapshots WHERE token=?", (token,)).fetchone()
        if row is None:
            raise InvalidCandidate("Preparation token is no longer active.")
        return row

    def commit(self, token: str) -> ConfigSnapshot:
        """Called only after the future runtime coordinator confirms activation."""
        with self._transaction():
            row = self._candidate(token)
            active, _ = self._versions()
            if row["state"] == "COMMITTED" and row["version"] == active:
                return self._decode(row)
            if row["state"] != "PREPARED" or row["version"] <= active:
                raise InvalidCandidate("Only a newer prepared candidate can be committed.")
            snapshot = self._decode(row)
            self._conn.execute("UPDATE snapshots SET state='COMMITTED' WHERE version=?", (row["version"],))
            self._conn.execute("UPDATE metadata SET previous_version=active_version,active_version=? WHERE id=1",
                               (row["version"],))
            self._conn.execute("UPDATE requests SET status='APPLIED' WHERE version=?", (row["version"],))
            return snapshot

    def fail(self, token: str) -> None:
        with self._transaction():
            row = self._candidate(token)
            if row["state"] != "PREPARED":
                raise InvalidCandidate("Only a prepared candidate can fail.")
            self._conn.execute("UPDATE snapshots SET state='FAILED',token=NULL WHERE version=?", (row["version"],))
            self._conn.execute("UPDATE requests SET status='FAILED' WHERE version=?", (row["version"],))
