"""A sibling advisory lock held for the complete lifetime of one config store."""
import os
from pathlib import Path


class StoreInUse(RuntimeError):
    pass


class StoreLock:
    def __init__(self, path: Path):
        self._file = path.open("a+b")
        try:
            self._file.seek(0, os.SEEK_END)
            if self._file.tell() == 0:
                self._file.write(b"\0")
                self._file.flush()
            self._file.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._file.close()
            raise StoreInUse("Configuration store already has an owner.") from None

    def close(self):
        # Closing releases the OS lock, including on process exit. Do not unlink
        # the lock file: replacing the inode permits two independent owners.
        self._file.close()
