import asyncio
import contextlib
import time
from typing import Any, AsyncGenerator, Optional

from .utils import guard


def get_ident() -> Optional[str]:
    """
    Returns asyncio task identifier.
    """

    if task := asyncio.current_task():
        return task.get_name()

    return None


class SharedLock:
    """
    Asynchronous shared lock. Supports shared and exclusive locking modes.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Condition()
        self._shared_cnt = 0
        self._exclusive = False
        self._exclusive_owner: Optional[str] = None

    def __repr__(self) -> str:
        if self._exclusive:
            state = "locked exclusively"
        elif self._shared_cnt:
            state = "locked for share"
        else:
            state = "unlocked"

        return "%s<status=%s, owner=%s, shared_by=%d>" % (
            self.__class__.__qualname__,
            state,
            self._exclusive_owner,
            self._shared_cnt,
        )

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(self, *args: Any) -> None:
        await self.release()

    @contextlib.asynccontextmanager
    async def acquired(
            self,
            exclusive: bool = True,
            timeout: Optional[float] = None,
    ) -> AsyncGenerator[None, None]:
        """
        Opens the lock acquiring context.
        """

        if not await self.acquire(exclusive, timeout):
            raise asyncio.TimeoutError
        try:
            yield
        finally:
            await self.release(exclusive)

    async def acquire(self, exclusive: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquires the lock.

        :param exclusive: acquire the lock in exclusive mode
        :param timeout: number of seconds during which the lock is tried to be acquired
        """

        if exclusive:
            return await self._acquire_exclusive(timeout)
        else:
            return await self._acquire_shared(timeout)

    async def release(self, exclusive: bool = True) -> None:
        """
        Releases the acquired lock.

        :param exclusive: release the lock acquired in exclusive mode
        """

        if exclusive:
            self._release_exclusive()
        else:
            await self._release_shared()

    async def _acquire_shared(self, timeout: Optional[float] = None) -> bool:
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
        except asyncio.TimeoutError:
            return False

        try:
            self._shared_cnt += 1
        finally:
            self._lock.release()

        return True

    async def _release_shared(self) -> None:
        if self._shared_cnt == 0:
            raise RuntimeError("lock is not acquired")

        cancelled = False
        try:
            await guard(self._lock.acquire())
        except asyncio.CancelledError:
            cancelled = True
        finally:
            self._shared_cnt -= 1

        if not self._shared_cnt:
            self._lock.notify()

        self._lock.release()

        if cancelled:
            raise asyncio.CancelledError

    async def _acquire_exclusive(self, timeout: Optional[float] = None) -> bool:
        started_at = time.monotonic()

        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
        except asyncio.TimeoutError:
            return False

        try:
            while self._shared_cnt > 0:
                timeout = timeout - (time.monotonic() - started_at) if timeout is not None else None
                await asyncio.wait_for(self._lock.wait(), timeout=timeout)

        except asyncio.TimeoutError:
            self._lock.release()
            return False

        except BaseException:
            self._lock.release()
            raise

        self._exclusive = True
        self._exclusive_owner = get_ident()

        return True

    def _release_exclusive(self) -> None:
        self._exclusive = False
        self._exclusive_owner = None
        self._lock.release()
