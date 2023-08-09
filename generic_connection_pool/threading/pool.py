import abc
import contextlib
import logging
import math
import threading
from collections import defaultdict
from typing import Any, Callable, DefaultDict, Generator, Generic, Hashable, List, Optional, Tuple, TypeVar

from generic_connection_pool import exceptions
from generic_connection_pool.common import BaseConnectionPool, BaseEndpointPool, BaseEventQueue, ConnectionInfo
from generic_connection_pool.common import EventType, Timer

from .locks import SharedLock

logger = logging.getLogger(__package__)

EndpointT = TypeVar('EndpointT', bound=Hashable)
ConnectionT = TypeVar('ConnectionT', bound=Hashable)


class BaseConnectionManager(Generic[EndpointT, ConnectionT], abc.ABC):
    """
    Abstract synchronous connection factory.
    """

    @abc.abstractmethod
    def create(self, endpoint: EndpointT, timeout: Optional[float] = None) -> ConnectionT:
        """
        Creates a new connection.

        :param endpoint: endpoint to connect to
        :param timeout: operation timeout
        :return: new connection
        """

    @abc.abstractmethod
    def dispose(self, endpoint: EndpointT, conn: ConnectionT, timeout: Optional[float] = None) -> None:
        """
        Disposes the connection.

        :param endpoint: endpoint to connect to
        :param conn: connection to be disposed
        :param timeout: operation timeout
        """

    def check_aliveness(self, endpoint: EndpointT, conn: ConnectionT, timeout: Optional[float] = None) -> bool:
        """
        Checks that the connection is alive.

        :param endpoint: endpoint to connect to
        :param conn: connection to be checked
        :param timeout: operation timeout
        :return: ``True`` if connection is alive otherwise ``False``
        """

        return True

    def on_acquire(self, endpoint: EndpointT, conn: ConnectionT) -> None:
        """
        Callback invoked on connection acquire.

        :param endpoint: endpoint to connect to
        :param conn: connection to be acquired
        """

    def on_release(self, endpoint: EndpointT, conn: ConnectionT) -> None:
        """
        Callback invoked on connection on_release.

        :param endpoint: endpoint to connect to
        :param conn: connection to be acquired
        """

    def on_connection_dead(self, endpoint: EndpointT, conn: ConnectionT) -> None:
        """
        Callback invoked on when connection aliveness check failed.

        :param endpoint: endpoint to connect to
        :param conn: dead connection
        """


KeyType = TypeVar('KeyType', bound=Hashable)


class EventQueue(BaseEventQueue[KeyType], Generic[KeyType]):
    """
    Thread-safe event queue wrapper.
    """

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Condition(threading.Lock())
        self._stopped = False

    def get_size(self) -> int:
        with self._lock:
            return len(self._queue)

    def insert(self, timestamp: float, key: KeyType) -> None:
        with self._lock:
            self._insert(timestamp, key)
            self._lock.notify_all()

    def remove(self, key: KeyType) -> None:
        with self._lock:
            self._remove(key)

    def clear(self) -> None:
        with self._lock:
            self._clear()

    def wait(self, timeout: Optional[float] = None) -> KeyType:
        """
        Waits for the next event. The event is not removed from the queue.
        """

        timer = Timer(timeout)

        with self._lock:
            while True:
                if self._stopped:
                    raise exceptions.ConnectionPoolClosedError

                key, backoff = self._try_get_next_event()
                if key is not None:
                    return key
                elif timer.timedout:
                    raise TimeoutError
                else:
                    self._lock.wait(
                        timeout=min(backoff, timer.remains)
                        if backoff is not None and timer.remains is not None
                        else backoff or timer.remains,
                    )

    def top(self) -> Optional[KeyType]:
        """
        Returns top event.
        """

        with self._lock:
            return self._top()

    def stop(self) -> None:
        """
        Notifies the subscribers that the process in stopped.
        """

        with self._lock:
            self._stopped = True
            self._lock.notify_all()


class EndpointPool(BaseEndpointPool[EndpointT, ConnectionT], Generic[EndpointT, ConnectionT]):
    """
    Thread-safe endpoint pool wrapper.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._condvar = threading.Condition(threading.Lock())

    @property
    def empty(self) -> bool:
        return self._size() == 0

    def size(self) -> int:
        with self._condvar:
            return self._size()

    def has_available_slot(self) -> bool:
        with self._condvar:
            return self._has_available_slot()

    def is_overflowed(self) -> bool:
        with self._condvar:
            return self._is_overflowed()

    def get_size(self, acquired: Optional[bool] = None) -> int:
        with self._condvar:
            return self._get_size(acquired)

    def reserve(self) -> bool:
        with self._condvar:
            return self._reserve()

    def acquire(self) -> Tuple[Optional[ConnectionInfo[EndpointT, ConnectionT]], bool]:
        with self._condvar:
            return self._acquire()

    def release(self, conn: ConnectionT) -> Tuple[ConnectionInfo[EndpointT, ConnectionT], bool]:
        with self._condvar:
            result = self._release(conn)
            self._condvar.notify()

            return result

    def detach(self, conn: ConnectionT, acquired: bool = False) -> ConnectionInfo[EndpointT, ConnectionT]:
        with self._condvar:
            result = self._detach(conn, acquired)
            self._condvar.notify()

            return result

    def attach(self, conn_info: ConnectionInfo[EndpointT, ConnectionT], acquired: bool = False) -> None:
        with self._condvar:
            self._attach(conn_info, acquired)
            self._condvar.notify()

    def acquire_and_detach(self) -> Optional[ConnectionInfo[EndpointT, ConnectionT]]:
        with self._condvar:
            conn_info, extra = self._acquire()
            if conn_info is None:
                return None

            result = self._detach(conn_info.conn, acquired=True)
            self._condvar.notify()

            return result

    def try_acquire_or_reserve(
            self,
            timeout: Optional[float] = None,
    ) -> Tuple[Optional[ConnectionInfo[EndpointT, ConnectionT]], bool]:
        timer = Timer(timeout)

        with self._condvar:
            while True:
                conn_info, extra = self._acquire()
                if conn_info is not None:
                    return conn_info, extra

                elif self._reserve():
                    return None, False

                elif not self._condvar.wait(timer.remains):
                    raise TimeoutError

    def attach_and_unreserve(self, conn_info: ConnectionInfo[EndpointT, ConnectionT], acquired: bool = False) -> None:
        with self._condvar:
            self._unreserve()
            self._attach(conn_info, acquired)
            self._condvar.notify()

    def unreserve(self) -> None:
        with self._condvar:
            self._unreserve()
            self._condvar.notify()


class PoolManager(Generic[EndpointT, ConnectionT]):
    """
    Connection pool manager.
    Provides an api to work with connection pools safely.
    """

    def __init__(self, pool_factory: Callable[[], EndpointPool[EndpointT, ConnectionT]]) -> None:
        self._pools: DefaultDict[EndpointT, Tuple[SharedLock, EndpointPool[EndpointT, ConnectionT]]] = defaultdict(
            lambda: (SharedLock(), pool_factory()),
        )
        self._condvar = threading.Condition(lock=threading.Lock())

    def get_size(self) -> int:
        with self._condvar:
            return sum(pool.size() for lock, pool in self._pools.values())

    def endpoints(self) -> List[EndpointT]:
        """
        Returns available endpoints.
        """

        with self._condvar:
            return list(self._pools.keys())

    def wait_for(self, predicate: Callable[[], bool], timeout: Optional[float] = None) -> bool:
        """
        Waits for the pool manager state change.
        """

        with self._condvar:
            return self._condvar.wait_for(predicate, timeout=timeout)

    @contextlib.contextmanager
    def acquired(
            self,
            endpoint: EndpointT,
            exclusive: bool = False,
            blocking: bool = True,
            timeout: Optional[float] = None,
            setdefault: bool = False,
    ) -> Generator[EndpointPool[EndpointT, ConnectionT], None, None]:
        """
        Opens the endpoint pool acquiring context.

        :param endpoint: pool endpoint
        :param exclusive: pool access mode (shared or exclusive)
        :param blocking: acquiring mode
        :param timeout: pool acquiring timeout
        :param setdefault: create a new pool if it not exists

        :return: acquired pool
        """

        with self._condvar:
            if (lock_and_pool := self._pools[endpoint] if setdefault else self._pools.get(endpoint)) is None:
                raise exceptions.ConnectionPoolNotFound

            lock, pool = lock_and_pool
            if not lock.acquire(exclusive, blocking=blocking, timeout=timeout):
                raise TimeoutError

        try:
            yield pool
        finally:
            lock.release(exclusive)
            with self._condvar:
                self._condvar.notify()

    def try_delete(self, endpoint: EndpointT) -> bool:
        """
        Tries to delete the endpoint pool.
        Acquires the pool in exclusive mode and checks that pool is empty.

        :param endpoint: pool endpoint

        :return: `True` if the pool has been deleted otherwise `False`
        """

        with self._condvar:
            if (lock_and_pool := self._pools.get(endpoint)) is None:
                return True
            else:
                lock, pool = lock_and_pool

            try:
                with lock.acquired(exclusive=True, blocking=False):
                    if not pool.empty:
                        return False
                    else:
                        self._pools.pop(endpoint)
                        return True
            except TimeoutError:
                return False


class ConnectionPool(Generic[EndpointT, ConnectionT], BaseConnectionPool[EndpointT, ConnectionT]):
    """
    Synchronous connection pool.

    :param connection_manager: connection manager instance. Used to create, dispose or check connection aliveness.
    :param acquire_timeout: connection acquiring default timeout.
    :param background_collector: if ``True`` starts a background worker that disposes expired and idle connections
                                 maintaining requested pool state. If ``False`` the connections will be disposed
                                 on each connection release.
    :param dispose_batch_size: maximum number of expired and idle connections to be disposed on connection release
                               (if background collector is started the parameter is ignored).
    :param dispose_timeout: connection disposal timeout.
    :param min_idle: minimum number of connections in each endpoint the pool tries to hold. Connections that exceed
                     that number will be considered as extra and disposed after ``idle_timeout`` seconds of inactivity.
    :param max_size: maximum number of endpoint connections.
    :param kwargs: see :py:class:`generic_connection_pool.common.BaseConnectionPool`
    """

    def __init__(
            self,
            connection_manager: BaseConnectionManager[EndpointT, ConnectionT],
            *,
            acquire_timeout: Optional[float] = None,
            background_collector: bool = False,
            dispose_batch_size: int = 0,
            dispose_timeout: Optional[float] = None,
            min_idle: int = 1,
            max_size: int = 10,
            **kwargs: Any,
    ):
        super().__init__(min_idle=min_idle, max_size=max_size, **kwargs)

        self._stopped = False
        self._acquire_timeout = acquire_timeout
        self._dispose_batch_size = dispose_batch_size
        self._dispose_timeout = dispose_timeout
        self._connection_manager = connection_manager

        self._lock = threading.Lock()

        self._pools = PoolManager(
            pool_factory=lambda: EndpointPool[EndpointT, ConnectionT](
                max_pool_size=min_idle,
                max_extra_size=max_size - min_idle,
            ),
        )
        self._event_queue = EventQueue[Tuple[EventType, EndpointT, ConnectionT]]()

        self._collector: Optional[threading.Thread] = None
        if background_collector:
            self._collector = threading.Thread(
                target=self._start_collector,
                name='gcp-collector',
            )
            self._collector.start()

    def get_size(self) -> int:
        with self._lock:
            return super().get_size()

    def get_endpoint_pool_size(self, endpoint: EndpointT, acquired: Optional[bool] = None) -> int:
        """
        Returns endpoint pool size.

        :param endpoint: pool endpoint
        :param acquired: if `True` returns the number of acquired connections,
                         if `False` returns the number of free connections
                         otherwise returns total size
        """

        try:
            with self._pools.acquired(endpoint) as pool:
                return pool.get_size(acquired)
        except exceptions.ConnectionPoolNotFound:
            return 0

    @contextlib.contextmanager
    def connection(self, endpoint: EndpointT, timeout: Optional[float] = None) -> Generator[ConnectionT, None, None]:
        """
        Acquires a connection from the pool.

        :param endpoint: connection endpoint
        :param timeout: number of seconds to wait. If timeout is reached :py:class:`TimeoutError` is raised.
        :return: acquired connection
        """

        conn = self.acquire(endpoint, timeout=timeout)
        try:
            yield conn
        finally:
            self.release(conn, endpoint)

    def acquire(self, endpoint: EndpointT, timeout: Optional[float] = None) -> ConnectionT:
        """
        Acquires a connection from the pool.

        :param endpoint: connection endpoint
        :param timeout: number of seconds to wait. If timeout is reached :py:class:`TimeoutError` is raised.
        :return: acquired connection
        """

        timeout = self._acquire_timeout if timeout is None else timeout

        conn = self._acquire_connection(endpoint, timeout=timeout)
        try:
            self._connection_manager.on_acquire(endpoint, conn)
        except Exception:
            self._release_connection(endpoint, conn)
            raise

        return conn

    def release(self, conn: ConnectionT, endpoint: EndpointT) -> None:
        """
        Releases a connection.

        :param conn: connection to be released
        :param endpoint: connection endpoint
        """

        try:
            self._connection_manager.on_release(endpoint, conn)
        finally:
            self._release_connection(endpoint, conn)

        if self._collector is None:
            dispose_batch_size = self._dispose_batch_size or int(math.log2(self._pool_size + 1)) + 1
            self._collect_disposable_connections(dispose_batch_size)

    def close(self, timeout: Optional[float] = None) -> None:
        """
        Closes the connection pool.

        :param timeout: timeout after which the pool closes all connection despite they are released or not
        """

        timer = Timer(timeout)

        self._stopped = True
        self._event_queue.stop()

        if self._collector is not None:
            self._collector.join(timeout=timer.remains)

        self._close_connections(timeout=timer.remains)
        self._event_queue.clear()

    def _acquire_connection(self, endpoint: EndpointT, timeout: Optional[float]) -> ConnectionT:
        timer = Timer(timeout)

        while True:
            if self._stopped:
                raise exceptions.ConnectionPoolClosedError

            with self._pools.acquired(endpoint, setdefault=True) as pool:
                conn_info, extra = pool.try_acquire_or_reserve(timeout=timer.remains)
                if conn_info is not None:
                    # unsubscribe the connection since acquired connection can't be disposed
                    self._event_queue.remove((EventType.LIFETIME, conn_info.endpoint, conn_info.conn))
                    if extra:
                        self._event_queue.remove((EventType.IDLETIME, conn_info.endpoint, conn_info.conn))

                    try:
                        is_alive = self._connection_manager.check_aliveness(endpoint, conn_info.conn, timer.remains)
                    except Exception:
                        self._release_connection(endpoint, conn_info.conn)
                        raise

                    if not is_alive:
                        pool.detach(conn_info.conn, acquired=True)
                        self._decrease_pool_size()
                        self._connection_manager.on_connection_dead(endpoint, conn_info.conn)
                        continue
                else:
                    try:
                        if conn_info := self._create_connection(endpoint, timer.remains):
                            pool.attach_and_unreserve(conn_info, acquired=True)
                        else:
                            pool.unreserve()
                            continue
                    except Exception:
                        pool.unreserve()
                        raise

                    logger.debug("connection created: %s", endpoint)

                return conn_info.conn

    def _create_connection(
            self,
            endpoint: EndpointT,
            timeout: Optional[float] = None,
    ) -> ConnectionInfo[EndpointT, ConnectionT]:
        timer = Timer(timeout)

        while True:
            if self._increase_pool_size():
                try:
                    conn = self._connection_manager.create(endpoint, timeout=timer.remains)
                except Exception:
                    self._decrease_pool_size()
                    raise

                return ConnectionInfo(endpoint, conn)

            else:
                self._try_free_slot(timeout=timer.remains)

    def _try_free_slot(self, timeout: Optional[float] = None) -> bool:
        timer = Timer(timeout)

        if event := self._event_queue.top():
            ev, endpoint, conn = event
            return self._try_detach_connection(endpoint, conn, timeout=timer.remains)

        elif not self._pools.wait_for(predicate=lambda: not self.is_full, timeout=timer.remains):
            raise TimeoutError

        return True

    def _collect_disposable_connections(self, max_disposals: int) -> None:
        disposals = 0

        while disposals < max_disposals:
            try:
                ev, endpoint, conn = self._event_queue.wait(timeout=0.0)
            except TimeoutError:
                # no connections to dispose
                break

            if self._try_detach_connection(endpoint, conn):
                disposals += 1

        if disposals > 0:
            logger.debug("disposed %d connections", disposals)

    def _start_collector(self) -> None:
        logger.debug("collector started")

        while not self._stopped:
            try:
                ev, endpoint, conn = self._event_queue.wait()
                self._try_detach_connection(endpoint, conn)
            except exceptions.ConnectionPoolClosedError:
                break

    def _try_detach_connection(self, endpoint: EndpointT, conn: ConnectionT, timeout: Optional[float] = None) -> bool:
        with contextlib.suppress(exceptions.ConnectionPoolNotFound):
            with self._pools.acquired(endpoint, timeout=timeout) as pool:
                try:
                    conn_info = pool.detach(conn)
                except KeyError:
                    return False
                finally:
                    self._event_queue.remove((EventType.LIFETIME, endpoint, conn))
                    self._event_queue.remove((EventType.IDLETIME, endpoint, conn))

                self._dispose_connection(conn_info, timeout=self._dispose_timeout)
                self._decrease_pool_size()
                is_pool_empty = pool.empty

            if is_pool_empty:
                self._pools.try_delete(endpoint)

        return True

    def _dispose_connection(self, conn_info: ConnectionInfo[EndpointT, ConnectionT], timeout: Optional[float]) -> bool:
        try:
            self._connection_manager.dispose(conn_info.endpoint, conn_info.conn, timeout=timeout)
        except TimeoutError:
            logger.error("connection disposal timed-out: %s", conn_info.endpoint)
            return False
        except Exception as e:
            logger.error("connection disposal failed: %s", e)
            return False

        logger.debug("connection disposed: %s", conn_info.endpoint)
        return True

    def _release_connection(self, endpoint: EndpointT, conn: ConnectionT) -> None:
        with self._pools.acquired(endpoint) as pool:
            try:
                conn_info, extra = pool.release(conn)
            except KeyError:
                raise RuntimeError("connection not acquired")

            # subscribe the connection, it is disposable again
            self._event_queue.insert(
                conn_info.created_at + self.max_lifetime,
                (EventType.LIFETIME, conn_info.endpoint, conn_info.conn),
            )
            if extra:
                self._event_queue.insert(
                    conn_info.accessed_at + self._idle_timeout,
                    (EventType.IDLETIME, conn_info.endpoint, conn_info.conn),
                )

    def _close_connections(self, timeout: Optional[float]) -> None:
        timer = Timer(timeout)

        while self._pools.get_size() != 0:
            for endpoint in self._pools.endpoints():
                with contextlib.suppress(exceptions.ConnectionPoolNotFound):
                    with self._pools.acquired(endpoint, timeout=timer.remains) as pool:
                        conn_info = pool.acquire_and_detach()
                        if conn_info is None:
                            continue

                        self._dispose_connection(conn_info, timeout=timer.remains)
                        self._decrease_pool_size()
                        is_pool_empty = pool.empty

                    if is_pool_empty:
                        self._pools.try_delete(endpoint)

    def _increase_pool_size(self) -> bool:
        with self._lock:
            if self.is_full:
                return False
            else:
                self._pool_size += 1
                return True

    def _decrease_pool_size(self) -> None:
        with self._lock:
            self._pool_size -= 1
