import contextlib
import threading
import time
from typing import Any, Generator, Optional


class SharedLock:
    """
    Shared lock. Supports shared and exclusive locking modes.
    """

    def __init__(self) -> None:
        self._lock = threading.Condition(threading.Lock())
        self._shared_cnt = 0
        self._exclusive = False
        self._exclusive_owner: Optional[int] = None

    def __repr__(self) -> str:
        if self._exclusive:
            state = "locked exclusively"
        elif self._shared_cnt:
            state = "locked for share"
        else:
            state = "unlocked"

        return "%s<status=%s, owner=%r, shared_by=%d>" % (
            self.__class__.__qualname__,
            state,
            self._exclusive_owner,
            self._shared_cnt,
        )

    def __enter__(self) -> None:
        self.acquire()

    def __exit__(self, *args: Any) -> None:
        self.release()

    @contextlib.contextmanager
    def acquired(
            self,
            exclusive: bool = True,
            blocking: bool = True,
            timeout: Optional[float] = None,
    ) -> Generator[None, None, None]:
        """
        Opens the lock acquiring context.
        """

        if not self.acquire(exclusive, blocking, timeout):
            raise TimeoutError
        try:
            yield
        finally:
            self.release(exclusive)

    def acquire(self, exclusive: bool = True, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquires the lock.

        :param exclusive: acquire the lock in exclusive mode
        :param blocking: when `True` blocks until the lock is acquired
        :param timeout: number of seconds during which the lock is tried to be acquired
        """

        if exclusive:
            return self._acquire_exclusive(blocking, timeout)
        else:
            return self._acquire_shared(blocking, timeout)

    def release(self, exclusive: bool = True) -> None:
        """
        Releases the acquired lock.

        :param exclusive: release the lock acquired in exclusive mode
        """

        if exclusive:
            self._release_exclusive()
        else:
            self._release_shared()

    def _acquire_shared(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        if not self._lock.acquire(blocking, timeout if timeout is not None else -1):
            return False

        try:
            self._shared_cnt += 1
        finally:
            self._lock.release()

        return True

    def _release_shared(self) -> None:
        if self._shared_cnt == 0:
            raise RuntimeError("lock is not acquired")

        try:
            self._lock.acquire()
        finally:
            self._shared_cnt -= 1

        if not self._shared_cnt:
            self._lock.notify()

        self._lock.release()

    def _acquire_exclusive(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        started_at = time.monotonic()

        if not self._lock.acquire(blocking, timeout if timeout is not None else -1):
            return False

        try:
            while self._shared_cnt > 0:
                if not blocking:
                    self._lock.release()
                    return False

                if not self._lock.wait(timeout - (time.monotonic() - started_at) if timeout is not None else None):
                    self._lock.release()
                    return False

        except BaseException:
            self._lock.release()
            raise

        self._exclusive = True
        self._exclusive_owner = threading.get_ident()

        return True

    def _release_exclusive(self) -> None:
        self._exclusive = False
        self._exclusive_owner = None
        self._lock.release()

    def _is_owned(self) -> bool:  # to be compatible with threading.Condition
        return self._exclusive_owner == threading.get_ident()
