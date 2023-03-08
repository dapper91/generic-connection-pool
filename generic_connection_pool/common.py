import dataclasses as dc
import functools as ft
import logging
import time
import typing
from collections import OrderedDict, defaultdict
from typing import Callable, Dict, Generic, Hashable, Optional, Tuple, TypeVar

from .heap import ExtHeap

logger = logging.getLogger(__package__)

EndpointT = TypeVar('EndpointT', bound=Hashable)
ConnectionT = TypeVar('ConnectionT', bound=Hashable)


class Timer:
    """
    Timer.

    :param timeout: timer timeout
    """

    def __init__(self, timeout: Optional[float]):
        self._timeout = timeout
        self._started_at = time.monotonic()

    @property
    def elapsed(self) -> float:
        """
        Returns number of seconds since the timer was created.
        """

        return time.monotonic() - self._started_at

    @property
    def remains(self) -> Optional[float]:
        """
        Returns number of seconds until the timeout.
        """

        if self._timeout is None:
            return None

        return self._timeout - self.elapsed

    @property
    def timedout(self) -> bool:
        """
        Returns if the timer timed-out.
        """

        if self._timeout is None:
            return False

        return self.elapsed > self._timeout


@dc.dataclass
class ConnectionInfo(Generic[EndpointT, ConnectionT]):
    endpoint: EndpointT
    conn: ConnectionT
    accessed_at: float
    created_at: float
    acquires: int = 0

    @property
    def lifetime(self) -> float:
        return time.monotonic() - self.created_at

    @property
    def idletime(self) -> float:
        return time.monotonic() - self.accessed_at


@ft.total_ordering
@dc.dataclass(frozen=True)
class Event:
    timestamp: float = dc.field(hash=False)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return NotImplemented

        return self.timestamp == other.timestamp

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return NotImplemented

        return self.timestamp < other.timestamp


@dc.dataclass(frozen=True)
class LifetimeExpiredEvent(Generic[EndpointT, ConnectionT], Event):
    endpoint: EndpointT = dc.field(hash=True, compare=False)
    conn: ConnectionT = dc.field(hash=True, compare=False)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LifetimeExpiredEvent):
            return False

        return (self.__class__, self.endpoint, self.conn) == (other.__class__, other.endpoint, other.conn)


@dc.dataclass(frozen=True)
class PoolSizeExceededEvent(Generic[EndpointT], Event):
    endpoint: EndpointT = dc.field(hash=True, compare=False)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PoolSizeExceededEvent):
            return False

        return (self.__class__, self.endpoint) == (other.__class__, other.endpoint)


class NotificationP(typing.Protocol):
    def set(self) -> None: ...
    def clear(self) -> None: ...
    def is_set(self) -> bool: ...


NotificationT = TypeVar('NotificationT', bound=NotificationP)


@dc.dataclass
class EndpointPool(Generic[NotificationT, EndpointT, ConnectionT]):
    notification: NotificationT
    queue: OrderedDict[ConnectionT, ConnectionInfo[EndpointT, ConnectionT]] = dc.field(default_factory=OrderedDict)
    acquired: Dict[ConnectionT, ConnectionInfo[EndpointT, ConnectionT]] = dc.field(default_factory=dict)
    access_queue: ExtHeap[Tuple[float, ConnectionT]] = dc.field(default_factory=ExtHeap)
    in_progress: int = 0

    def __len__(self) -> int:
        return len(self.queue) + len(self.acquired) + self.in_progress

    def acquire(self) -> ConnectionInfo[EndpointT, ConnectionT]:
        conn, conn_info = self.queue.popitem(last=False)
        self.acquired[conn] = conn_info
        self.access_queue.remove((conn_info.accessed_at, conn))

        conn_info.acquires += 1
        conn_info.accessed_at = time.monotonic()

        self.notification.set()

        return conn_info

    def release(self, conn: ConnectionT) -> ConnectionInfo[EndpointT, ConnectionT]:
        conn_info = self.acquired.pop(conn)
        conn_info.accessed_at = time.monotonic()

        self.access_queue.push((conn_info.accessed_at, conn))
        self.queue[conn] = conn_info

        self.notification.set()

        return conn_info

    def detach(self, conn: ConnectionT, acquired: bool = False) -> ConnectionInfo[EndpointT, ConnectionT]:
        if acquired:
            conn_info = self.acquired.pop(conn)
        else:
            conn_info = self.queue.pop(conn)
            self.access_queue.remove((conn_info.accessed_at, conn))

        self.notification.set()

        return conn_info

    def attach(self, conn_info: ConnectionInfo[EndpointT, ConnectionT], acquired: bool = False) -> None:
        if acquired:
            self.acquired[conn_info.conn] = conn_info
        else:
            self.queue[conn_info.conn] = conn_info
            self.access_queue.push((conn_info.accessed_at, conn_info.conn))

        self.notification.set()

    def least_recently_used(self) -> Optional[Tuple[float, ConnectionT]]:
        return self.access_queue.top()


class BaseConnectionPool(Generic[NotificationT, EndpointT, ConnectionT]):
    """
    Asynchronous connection pool.

    :param idle_timeout: number of seconds after which a connection will be closed respecting min_idle parameter
                         (the connection will be closed only if the connection number exceeds min_idle)
    :param max_lifetime: number of seconds after which a connection will be closed (min_idle parameter will be ignored)
    :param min_idle: minimum number of connections the pool tries to hold (for each endpoint)
    :param max_size: maximum number of connections (for each endpoint)
    :param total_max_size: maximum number of connections (for all endpoints)
    """

    def __init__(
            self,
            pool_factory: Callable[[], EndpointPool[NotificationT, EndpointT, ConnectionT]],
            *,
            idle_timeout: float = 60.0,
            max_lifetime: float = 3600.0,
            min_idle: int = 1,
            max_size: int = 10,
            total_max_size: int = 100,
    ):
        assert min_idle <= max_size, "min_idle can't be greater than max_size"
        assert max_size <= total_max_size, "max_size can't be greater than total_max_size"
        assert idle_timeout <= max_lifetime, "idle_timeout can't be greater than max_lifetime"

        self._idle_timeout = idle_timeout
        self._max_lifetime = max_lifetime
        self._min_idle = min_idle
        self._max_size = max_size
        self._total_max_size = total_max_size

        self._pool_size = 0
        self._pools: Dict[EndpointT, EndpointPool[NotificationT, EndpointT, ConnectionT]] = defaultdict(pool_factory)
        self._event_queue: ExtHeap[Event] = ExtHeap()

    def __len__(self) -> int:
        return self._pool_size

    def get_endpoint_pool_size(self, endpoint: EndpointT, acquired: Optional[bool] = None) -> int:
        result = 0
        if (pool := self._pools.get(endpoint)) is not None:
            if acquired is None:
                result = len(pool)
            elif acquired is True:
                result = len(pool.acquired)
            elif acquired is False:
                result = len(pool.queue)
            else:
                raise AssertionError("unreachable")

        return result

    @property
    def idle_timeout(self) -> float:
        return self._idle_timeout

    @property
    def max_lifetime(self) -> float:
        return self._max_lifetime

    @property
    def min_idle(self) -> int:
        return self._min_idle

    @property
    def max_size(self) -> int:
        return self._max_size

    @property
    def total_max_size(self) -> int:
        return self._total_max_size

    def _has_available_slot(self, endpoint: EndpointT) -> bool:
        if self._pool_size >= self._total_max_size:
            return False

        pool = self._pools[endpoint]
        if len(pool) >= self._max_size:
            return False

        return True

    def _try_acquire(self, endpoint: EndpointT) -> Optional[ConnectionT]:
        pool = self._pools[endpoint]

        if len(pool.queue) != 0:
            conn_info = pool.acquire()
            self._event_queue.remove(
                LifetimeExpiredEvent(
                    timestamp=conn_info.created_at + self._max_lifetime,
                    endpoint=endpoint,
                    conn=conn_info.conn,
                ),
            )
            return conn_info.conn

        else:
            return None

    def _release(self, conn: ConnectionT, endpoint: EndpointT) -> None:
        if (pool := self._pools.get(endpoint)) is None:
            raise RuntimeError("endpoint mismatched")

        try:
            conn_info = pool.release(conn)
        except KeyError:
            raise RuntimeError("connection not acquired")

        self._event_queue.push(
            LifetimeExpiredEvent(
                timestamp=conn_info.created_at + self._max_lifetime,
                endpoint=endpoint,
                conn=conn,
            ),
        )

        if len(pool) > self._min_idle:
            event = PoolSizeExceededEvent(
                timestamp=time.monotonic(),
                endpoint=endpoint,
            )
            if event not in self._event_queue:
                self._event_queue.push(event)

    def _get_disposable_connection(self) -> Tuple[float, Optional[ConnectionInfo[EndpointT, ConnectionT]]]:
        conn_info: Optional[ConnectionInfo[EndpointT, ConnectionT]] = None
        backoff: float = 60.0  # why not

        if (event := self._event_queue.top()) is not None:
            now = time.monotonic()
            if now >= event.timestamp:
                self._event_queue.pop()  # remove top event
                conn_info = self._handle_event(event)
                backoff = 0.0
            else:
                conn_info = None
                backoff = event.timestamp - now

        return backoff, conn_info

    def _handle_event(self, event: Event) -> Optional[ConnectionInfo[EndpointT, ConnectionT]]:
        if isinstance(event, LifetimeExpiredEvent):
            return self._on_lifetime_expired_event(event)
        elif isinstance(event, PoolSizeExceededEvent):
            return self._on_pool_size_exceeded_event(event)
        else:
            raise AssertionError('unreachable')

    def _on_lifetime_expired_event(
            self,
            event: LifetimeExpiredEvent[EndpointT, ConnectionT],
    ) -> ConnectionInfo[EndpointT, ConnectionT]:
        pool = self._pools[event.endpoint]

        conn_info = pool.detach(event.conn)
        if len(pool) == 0:
            self._pools.pop(event.endpoint)

        self._pool_size -= 1

        return conn_info

    def _on_pool_size_exceeded_event(
            self,
            event: PoolSizeExceededEvent[EndpointT],
    ) -> Optional[ConnectionInfo[EndpointT, ConnectionT]]:
        pool = self._pools[event.endpoint]

        if len(pool) > self._min_idle:
            if (access := pool.least_recently_used()) is not None:
                accessed_at, conn = access

                now = time.monotonic()
                if now >= accessed_at + self._idle_timeout:
                    conn_info = pool.detach(conn)
                    self._pool_size -= 1

                    self._event_queue.remove(
                        LifetimeExpiredEvent(
                            timestamp=conn_info.created_at + self._max_lifetime,
                            endpoint=event.endpoint,
                            conn=conn,
                        ),
                    )
                    if len(pool) > self._min_idle:  # pool size still exceeds max value
                        event = PoolSizeExceededEvent(
                            timestamp=time.monotonic(),
                            endpoint=event.endpoint,
                        )
                        if event not in self._event_queue:
                            self._event_queue.push(event)

                    return conn_info

                else:
                    event = PoolSizeExceededEvent(
                        timestamp=accessed_at + self._idle_timeout,
                        endpoint=event.endpoint,
                    )
                    if event not in self._event_queue:
                        self._event_queue.push(event)

        if len(pool) == 0:
            self._pools.pop(event.endpoint)

        return None
