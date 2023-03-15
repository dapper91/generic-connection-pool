import abc
import contextlib
import logging
import math
import threading
import time
from typing import Any, Generator, Generic, Hashable, List, Optional, TypeVar

from . import exceptions
from .common import BaseConnectionPool, ConnectionInfo, EndpointPool, Timer

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


class ConnectionPool(Generic[EndpointT, ConnectionT], BaseConnectionPool[threading.Event, EndpointT, ConnectionT]):
    """
    Synchronous connection pool.

    :param connection_manager: connection manager instance
    :param acquire_timeout: connection acquiring default timeout
    :param dispose_batch_size: number of connections to be disposed at once
                               (if background collector is started the parameter is ignored)
    :param dispose_timeout: connection disposal timeout
    :param background_collector: start worker that disposes timed-out connections in background maintain
                                 provided pool state otherwise they will be disposed on each connection release
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
            **kwargs: Any,
    ):
        super().__init__(lambda: EndpointPool(notification=threading.Event()), **kwargs)

        self._acquire_timeout = acquire_timeout
        self._dispose_batch_size = dispose_batch_size
        self._connection_manager = connection_manager

        self._stopped = threading.Event()
        self._lock = threading.Condition()

        self._dispose_timeout = dispose_timeout
        self._collector: Optional[threading.Thread]
        if background_collector:
            self._collector = threading.Thread(
                target=self._start_collector,
                name='connection-pool-collector',
            )
            self._collector.start()
        else:
            self._collector = None

    @contextlib.contextmanager
    def connection(self, endpoint: EndpointT, timeout: Optional[float] = None) -> Generator[ConnectionT, None, None]:
        """
        Acquires a connection form the pool.

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
        Acquires a connection form the pool.

        :param endpoint: connection endpoint
        :param timeout: number of seconds to wait. If timeout is reached :py:class:`TimeoutError` is raised.
        :return: acquired connection
        """

        timeout = self._acquire_timeout if timeout is None else timeout

        return self._acquire(endpoint, timeout=timeout)

    def release(self, conn: ConnectionT, endpoint: EndpointT) -> None:
        try:
            self._connection_manager.on_release(endpoint, conn)
        finally:
            self._release_connection(endpoint, conn)

        if self._collector is None:
            dispose_batch_size = self._dispose_batch_size or int(math.log2(self._pool_size + 1)) + 1
            self._collect_disposable_connections(dispose_batch_size)

    def close(self, graceful_timeout: Optional[float] = None, timeout: Optional[float] = None) -> None:
        """
        Closes the connection pool.

        :param graceful_timeout: timeout within which the pool waits all acquired connection to be released
        :param timeout: timeout after which the pool closes all connection despite they are released or not
        """

        if graceful_timeout is None:
            graceful_timeout = timeout

        if graceful_timeout is not None and timeout is not None:
            assert timeout >= graceful_timeout, "timeout can't be less than graceful_timeout"

        graceful_timer = Timer(graceful_timeout)
        global_timer = Timer(timeout)

        self._stopped.set()
        with self._lock:
            self._lock.notify_all()

        if self._collector is not None:
            self._collector.join(timeout=global_timer.remains)

        self._event_queue.clear()
        self._close_connections(graceful_timer.remains, timeout=global_timer.remains)

    def _acquire(self, endpoint: EndpointT, timeout: Optional[float]) -> ConnectionT:
        timer = Timer(timeout)

        while True:
            if (conn := self._acquire_connection(endpoint, timeout=timer.remains)) is not None:
                return conn

            if (conn := self._try_create_connection(endpoint, timeout=timer.remains)) is not None:
                try:
                    self._connection_manager.on_acquire(endpoint, conn)
                except BaseException:
                    self._release_connection(endpoint, conn)
                    raise

                return conn

            notification = self._pools[endpoint].notification
            notified = notification.wait(timeout=timer.remains)
            notification.clear()
            if not notified:
                raise TimeoutError

    def _collect_disposable_connections(self, max_disposals: int) -> None:
        disposals = 0

        while disposals != max_disposals:
            with self._lock:
                backoff, conn_info = self._get_disposable_connection()
                if conn_info is not None:
                    self._lock.notify(1)

            if backoff != 0.0:
                break

            if conn_info is not None:
                try:
                    self._dispose_connection(conn_info, timeout=self._dispose_timeout)
                except TimeoutError:
                    pass
                except BaseException:
                    self._attach_connection(conn_info)
                    raise

                disposals += 1

        if disposals > 0:
            logger.debug("disposed %d connections", disposals)

    def _start_collector(self) -> None:
        logger.debug("collector started")

        while not self._stopped.is_set():
            with self._lock:
                backoff, conn_info = self._get_disposable_connection()
                if conn_info is None:
                    self._lock.wait(timeout=backoff)
                else:
                    self._lock.notify(1)

            if conn_info is not None:
                try:
                    self._dispose_connection(conn_info, timeout=self._dispose_timeout)
                except TimeoutError:
                    pass
                except BaseException:
                    self._attach_connection(conn_info)
                    raise

    def _dispose_connection(self, conn_info: ConnectionInfo[EndpointT, ConnectionT], timeout: Optional[float]) -> bool:
        try:
            self._connection_manager.dispose(conn_info.endpoint, conn_info.conn, timeout=timeout)
        except TimeoutError:
            logger.error("connection disposal timed-out: %s", conn_info.endpoint)
            raise
        except Exception as e:
            logger.exception("connection disposal failed: %s", e)
            return False

        logger.debug("connection disposed: %s", conn_info.endpoint)
        return True

    def _acquire_connection(self, endpoint: EndpointT, timeout: Optional[float]) -> Optional[ConnectionT]:
        timer = Timer(timeout)

        while True:
            with self._lock:
                if self._stopped.is_set():
                    raise exceptions.ConnectionPoolClosedError

                if (conn := self._try_acquire(endpoint)) is not None:
                    self._lock.notify(1)
                else:
                    return None

            try:
                if is_alive := self._connection_manager.check_aliveness(endpoint, conn, timeout=timer.remains):
                    self._connection_manager.on_acquire(endpoint, conn)
            except BaseException:
                self._release_connection(endpoint, conn)
                raise

            if not is_alive:
                self._detach_connection(endpoint, conn)
                self._connection_manager.on_connection_dead(endpoint, conn)
            else:
                return conn

    def _release_connection(self, endpoint: EndpointT, conn: ConnectionT) -> None:
        with self._lock:
            self._release(conn, endpoint)
            self._lock.notify(1)

    def _attach_connection(self, conn_info: ConnectionInfo[EndpointT, ConnectionT]) -> None:
        with self._lock:
            pool = self._pools[conn_info.endpoint]
            pool.attach(conn_info)

            self._pool_size += 1
            self._lock.notify(1)

    def _detach_connection(self, endpoint: EndpointT, conn: ConnectionT) -> None:
        with self._lock:
            self._pools[endpoint].detach(conn, acquired=True)
            self._pool_size -= 1
            self._lock.notify(1)

    def _try_create_connection(self, endpoint: EndpointT, timeout: Optional[float]) -> Optional[ConnectionT]:
        timer = Timer(timeout)

        if not self._acquire_pool_slot(endpoint):
            return None

        try:
            conn = self._connection_manager.create(endpoint, timeout=timer.remains)
        except BaseException:
            self._release_pool_slot(endpoint)
            raise

        now = time.monotonic()
        conn_info = ConnectionInfo(endpoint, conn, created_at=now, accessed_at=now)

        try:
            self._attach_and_release_pool_slot(endpoint, conn_info, acquired=True)
        except BaseException:
            self._attach_and_release_pool_slot(endpoint, conn_info, acquired=False)
            raise

        logger.debug("connection created: %s", endpoint)

        return conn

    def _acquire_pool_slot(self, endpoint: EndpointT) -> bool:
        with self._lock:
            if self._stopped.is_set():
                raise exceptions.ConnectionPoolClosedError

            if not self._has_available_slot(endpoint):
                return False

            pool = self._pools[endpoint]
            pool.in_progress += 1
            self._pool_size += 1

        return True

    def _release_pool_slot(self, endpoint: EndpointT) -> None:
        with self._lock:
            pool = self._pools[endpoint]
            pool.in_progress -= 1
            self._pool_size -= 1
            self._lock.notify(1)

    def _attach_and_release_pool_slot(
            self,
            endpoint: EndpointT,
            conn_info: ConnectionInfo[EndpointT, ConnectionT],
            acquired: bool = False,
    ) -> None:
        with self._lock:
            pool = self._pools[endpoint]
            pool.attach(conn_info, acquired=acquired)
            pool.in_progress -= 1
            self._lock.notify(1)

    def _close_connections(self, graceful_timeout: Optional[float], timeout: Optional[float]) -> None:
        graceful_timer = Timer(graceful_timeout)
        global_timer = Timer(timeout)

        while self._pools:
            released: List[ConnectionInfo[EndpointT, ConnectionT]] = []
            acquired: List[ConnectionInfo[EndpointT, ConnectionT]] = []
            try:
                with self._lock:
                    for endpoint, pool in list(self._pools.items()):
                        if len(pool) == 0:
                            self._pools.pop(endpoint)

                        while pool.queue:
                            conn, conn_info = pool.queue.popitem()
                            pool.access_queue.remove((conn_info.accessed_at, conn))
                            self._pool_size -= 1
                            released.append(conn_info)

                        for conn, conn_info in pool.acquired.items():
                            acquired.append(conn_info)

                    if acquired and not released and not graceful_timer.timedout:
                        self._lock.wait(timeout=graceful_timer.remains)

                if not released and not acquired:
                    break

                try:
                    while released:
                        conn_info = released[-1]
                        self._dispose_connection(conn_info, timeout=graceful_timer.remains)
                        released.pop()
                except TimeoutError:
                    self._return_released_conns(released)

                if graceful_timer.timedout:
                    for conn_info in acquired:
                        self._dispose_connection(conn_info, timeout=global_timer.remains)
                    break

            except BaseException:
                self._return_released_conns(released)
                raise

    def _return_released_conns(self, released: List[ConnectionInfo[EndpointT, ConnectionT]]) -> None:
        with self._lock:
            for conn_info in released:
                pool = self._pools[conn_info.endpoint]
                pool.queue[conn_info.conn] = conn_info
                pool.access_queue.remove((conn_info.accessed_at, conn_info.conn))
                self._pool_size += 1
