import dataclasses as dc
import logging
import time
from collections import OrderedDict
from enum import IntEnum
from typing import Dict, Generic, Hashable, Optional, Tuple, TypeVar

from . import exceptions
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

        return max(0.0, self._timeout - self.elapsed)

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
    accessed_at: float = dc.field(default_factory=time.monotonic)
    created_at: float = dc.field(default_factory=time.monotonic)
    acquires: int = 0

    @property
    def lifetime(self) -> float:
        return time.monotonic() - self.created_at

    @property
    def idletime(self) -> float:
        return time.monotonic() - self.accessed_at


class BaseEndpointPool(Generic[EndpointT, ConnectionT]):
    """
    Base endpoint pool. Contains information about connections related to a particular endpoint.
    """

    def __init__(self, max_pool_size: int, max_extra_size: int):
        """
        :param max_pool_size: connection pool max size
        :param max_extra_size: extra connection pool max size
        """

        self._max_pool_size = max_pool_size
        self._max_extra_size = max_extra_size

        # connections from this pool are kept open util theirs lifetime expires;
        # they are acquired using FIFO (round-robin) strategy
        self._pool: OrderedDict[ConnectionT, ConnectionInfo[EndpointT, ConnectionT]] = OrderedDict()
        # connections from this pool are closed after idle-time expires; they are acquired using LIFO strategy
        # to recycle extra connections as soon as possible since they are recycled based on last access time
        self._extra: OrderedDict[ConnectionT, ConnectionInfo[EndpointT, ConnectionT]] = OrderedDict()
        # acquired connections
        self._acquired: Dict[ConnectionT, ConnectionInfo[EndpointT, ConnectionT]] = dict()
        # number of reserved connections (to be established)
        self._reserved = 0

    @property
    def max_size(self) -> int:
        """
        Returns pool max size.
        """

        return self._max_pool_size + self._max_extra_size

    def _has_available_slot(self) -> bool:
        """
        Returns `True` if the pool has available connection slots otherwise `False`.
        """

        return self._size() < self.max_size

    def _is_overflowed(self) -> bool:
        """
        Returns `True` if the pool has extra connections otherwise `False`.
        """

        return self._size() > self._max_pool_size

    def _size(self) -> int:
        return len(self._pool) + len(self._extra) + len(self._acquired) + self._reserved

    def _get_size(self, acquired: Optional[bool] = None) -> int:
        """
        Returns the number of connections.

        :param acquired: if `True` - return the number of acquired connections
                         if `False` - return the number of free connections
                         if `None` - return all connections number (including reserved)

        return: number of connections
        """

        if acquired is None:
            result = self._size()
        elif acquired is True:
            result = len(self._acquired)
        elif acquired is False:
            result = len(self._pool) + len(self._extra)
        else:
            raise AssertionError("unreachable")

        return result

    def _reserve(self) -> bool:
        """
        Reserve one slot in the pool.
        """

        if not self._has_available_slot():
            return False

        self._reserved += 1

        return True

    def _unreserve(self) -> None:
        """
        Un-reserve a pool slot.
        """

        assert self._reserved != 0
        self._reserved -= 1

    def _acquire(self) -> Tuple[Optional[ConnectionInfo[EndpointT, ConnectionT]], bool]:
        """
        Acquires a connection from the pool.
        """

        if self._pool:
            extra = False
            conn, conn_info = self._pool.popitem(last=False)
        elif self._extra:
            extra = True
            conn, conn_info = self._extra.popitem(last=True)
        else:
            return None, False

        self._acquired[conn] = conn_info

        conn_info.acquires += 1
        conn_info.accessed_at = time.monotonic()

        return conn_info, extra

    def _release(self, conn: ConnectionT) -> Tuple[ConnectionInfo[EndpointT, ConnectionT], bool]:
        """
        Releases a connection.

        :param conn: connection to be released
        """

        assert conn in self._acquired, "connection is not acquired"

        conn_info = self._acquired.pop(conn)
        conn_info.accessed_at = time.monotonic()

        free_pool_slots = self._max_pool_size - len(self._pool)
        if len(self._acquired) >= free_pool_slots:
            extra = True
            self._extra[conn] = conn_info
        elif len(self._pool) < self._max_pool_size:
            extra = False
            self._pool[conn] = conn_info
        else:
            raise AssertionError("unreachable")

        return conn_info, extra

    def _detach(self, conn: ConnectionT, acquired: bool = False) -> ConnectionInfo[EndpointT, ConnectionT]:
        """
        Detaches off a connection.

        :param conn: connection to be detached
        :param acquired: is the connection acquired
        """

        if acquired:
            conn_info = self._acquired.pop(conn)
        else:
            if conn in self._pool:
                conn_info = self._pool.pop(conn)
            else:
                conn_info = self._extra.pop(conn)

        return conn_info

    def _attach(self, conn_info: ConnectionInfo[EndpointT, ConnectionT], acquired: bool = False) -> None:
        """
        Attaches a connection to the pool.

        :param conn_info: connection information to be attached
        :param acquired: acquire the connection
        """

        if not self._has_available_slot():
            raise exceptions.ConnectionPoolIsFull

        if acquired:
            self._acquired[conn_info.conn] = conn_info
        else:
            free_pool_slots = self._max_pool_size - len(self._pool)
            if len(self._acquired) >= free_pool_slots:
                self._extra[conn_info.conn] = conn_info
            elif len(self._pool) < self._max_pool_size:
                self._pool[conn_info.conn] = conn_info
            else:
                raise AssertionError("unreachable")


KeyType = TypeVar('KeyType', bound=Hashable)


@dc.dataclass(frozen=True, order=True)
class Event(Generic[KeyType]):
    """
    Connection pool event.

    :param timestamp: event raise time
    :param key: event key (must be equal for the same event)
    """

    timestamp: float = dc.field(hash=False, compare=True)
    key: KeyType = dc.field(hash=True, compare=False)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Event):
            return False

        return self.key == other.key


class BaseEventQueue(Generic[KeyType]):
    """
    Connection pool event queue.
    """

    def __init__(self) -> None:
        self._queue: ExtHeap[Event[KeyType]] = ExtHeap()

    def _insert(self, timestamp: float, key: KeyType) -> None:
        """
        Adds a new event to the queue.
        """

        self._queue.insert_or_replace(Event(timestamp, key))

    def _remove(self, key: KeyType) -> None:
        """
        Remove an event from the queue.
        """

        self._queue.remove(Event(0.0, key))

    def _clear(self) -> None:
        """
        Clears the queue.
        """

        self._queue.clear()

    def _try_get_next_event(self) -> Tuple[Optional[KeyType], Optional[float]]:
        """
        Tries to pop the next event from the queue.
        If the queue is empty returns `None `
        If the first event has not occurred returns `None` and backoff timeout.

        :return: event data, backoff timeout
        """

        if (event := self._queue.top()) and event.timestamp <= time.monotonic():
            return event.key, 0.0

        backoff = event.timestamp - time.monotonic() if event is not None else None

        return None, backoff

    def _top(self) -> Optional[KeyType]:
        """
        Pops an event from the queue regardless whether the event occurred or not.
        """

        if (event := self._queue.top()) is None:
            return None

        return event.key


class EventType(IntEnum):
    LIFETIME = 1
    IDLETIME = 2


class BaseConnectionPool(Generic[EndpointT, ConnectionT]):
    """
    Asynchronous connection pool.

    :param idle_timeout: inactivity time (in seconds) after which an extra connection will be disposed
                         (a connection considered as extra if the number of endpoint connection exceeds ``min_idle``).
    :param max_lifetime: number of seconds after which any connection will be disposed.
    :param min_idle: minimum number of connections the pool tries to hold for each endpoint. Connections that exceed
                     that number will be considered as extra and will be disposed after ``idle_timeout`` of inactivity.
    :param max_size: maximum number of endpoint connections.
    :param total_max_size: maximum number of all connections in the pool.
    """

    def __init__(
            self,
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

    def get_size(self) -> int:
        return self._pool_size

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

    @property
    def is_full(self) -> bool:
        return self._pool_size >= self._total_max_size
