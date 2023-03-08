import asyncio
import itertools as it
import random
from typing import Dict, List, Optional, Tuple

import pytest

from generic_connection_pool.asyncio import BaseConnectionManager, ConnectionPool


class TestConnection:
    def __init__(self, name: str):
        self._name = name

    def __str__(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)})"


class TestConnectionManager(BaseConnectionManager[int, TestConnection]):
    def __init__(self):
        self.creations: List[Tuple[int, TestConnection]] = []
        self.disposals: List[TestConnection] = []
        self.acquires: List[TestConnection] = []
        self.releases: List[TestConnection] = []
        self.dead: List[TestConnection] = []
        self.aliveness: Dict[TestConnection, bool] = {}

        self.create_err: Optional[Exception] = None
        self.dispose_err: Optional[Exception] = None
        self.check_aliveness_err: Optional[Exception] = None
        self.on_acquire_err: Optional[Exception] = None
        self.on_release_err: Optional[Exception] = None
        self.on_connection_dead_err: Optional[Exception] = None

        self._conn_cnt = 0

    async def create(self, endpoint: int) -> TestConnection:
        if err := self.create_err:
            raise err

        conn = TestConnection(f"{endpoint}:{self._conn_cnt}")
        self._conn_cnt += 1
        self.creations.append((endpoint, conn))

        return conn

    async def dispose(self, endpoint: int, conn: TestConnection) -> None:
        if err := self.dispose_err:
            raise err

        self.disposals.append(conn)

    async def check_aliveness(self, endpoint: int, conn: TestConnection) -> bool:
        if err := self.check_aliveness_err:
            raise err

        return self.aliveness.get(conn, True)

    async def on_acquire(self, endpoint: int, conn: TestConnection) -> None:
        if err := self.on_acquire_err:
            raise err

        self.acquires.append(conn)

    async def on_release(self, endpoint: int, conn: TestConnection) -> None:
        if err := self.on_release_err:
            raise err

        self.releases.append(conn)

    async def on_connection_dead(self, endpoint: int, conn: TestConnection) -> None:
        if err := self.on_connection_dead_err:
            raise err

        self.dead.append(conn)


@pytest.fixture
def connection_manager() -> TestConnectionManager:
    return TestConnectionManager()


async def test_params(connection_manager: TestConnectionManager):
    idle_timeout = 10.0
    max_lifetime = 60.0
    min_idle = 2
    max_size = 16
    total_max_size = 512

    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        idle_timeout=idle_timeout,
        max_lifetime=max_lifetime,
        min_idle=min_idle,
        max_size=max_size,
        total_max_size=total_max_size,
    )

    assert pool.idle_timeout == idle_timeout
    assert pool.max_lifetime == max_lifetime
    assert pool.min_idle == min_idle
    assert pool.max_size == max_size
    assert pool.total_max_size == total_max_size

    await pool.close()
    assert len(pool) == 0


async def test_pool_test_context_manager(connection_manager: TestConnectionManager):
    pool = ConnectionPool[int, TestConnection](connection_manager, min_idle=0, idle_timeout=0.0)

    async with pool.connection(endpoint=1) as conn1:
        assert len(pool) == 1

        async with pool.connection(endpoint=2) as conn2:
            assert len(pool) == 2

            async with pool.connection(endpoint=3) as conn3:
                assert len(pool) == 3

    assert connection_manager.creations == [(1, conn1), (2, conn2), (3, conn3)]
    assert len(pool) == 0
    assert connection_manager.disposals == [conn3, conn2, conn1]

    await pool.close()


async def test_pool_acquire_round_robin(connection_manager: TestConnectionManager):
    async def fill_endpoint_pool(pool: ConnectionPool, endpoint: int, size: int) -> List[TestConnection]:
        connections = [await pool.acquire(endpoint) for _ in range(size)]
        for conn in connections:
            await pool.release(conn, endpoint)

        return connections

    pool = ConnectionPool[int, TestConnection](connection_manager, min_idle=3)
    connections = {
        1: await fill_endpoint_pool(pool, endpoint=1, size=1),
        2: await fill_endpoint_pool(pool, endpoint=2, size=2),
        3: await fill_endpoint_pool(pool, endpoint=3, size=3),
    }

    for endpoint, connections in connections.items():
        for conn in it.chain(connections, connections):
            assert await pool.acquire(endpoint) == conn
            await pool.release(conn, endpoint)

    await pool.close()
    assert len(pool) == 0


async def test_connection_manager_callbacks(connection_manager: TestConnectionManager):
    pool = ConnectionPool[int, TestConnection](connection_manager)
    async with pool.connection(endpoint=1) as conn:
        pass

    assert connection_manager.acquires == [conn]
    assert connection_manager.releases == [conn]

    await pool.close()
    assert len(pool) == 0


async def test_connection_wait(sleep_delay: float, connection_manager: TestConnectionManager):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        max_size=1,
    )

    async def acquire_connection(timeout):
        async with pool.connection(endpoint=1, timeout=timeout):
            await asyncio.sleep(sleep_delay)
            assert len(pool) == 1

    workers_cnt = 10
    for worker in [
        asyncio.create_task(acquire_connection(timeout=2*workers_cnt*sleep_delay))
        for _ in range(workers_cnt)
    ]:
        await worker

    assert len(pool) == 1

    await pool.close()
    assert len(pool) == 0


async def test_pool_max_size(sleep_delay, connection_manager: TestConnectionManager):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        min_idle=0,
        idle_timeout=0.0,
        max_size=1,
        total_max_size=2,
    )

    conn1 = await pool.acquire(endpoint=1)
    with pytest.raises(asyncio.TimeoutError):
        await pool.acquire(endpoint=1, timeout=sleep_delay)
    assert len(pool) == 1

    await pool.release(conn1, endpoint=1)
    assert len(pool) == 0

    conn1 = await pool.acquire(endpoint=1)
    await pool.release(conn1, endpoint=1)
    assert len(pool) == 0

    await pool.close()


async def test_pool_total_max_size(sleep_delay, connection_manager: TestConnectionManager):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        min_idle=0,
        idle_timeout=0.0,
        max_size=1,
        total_max_size=1,
    )

    conn1 = await pool.acquire(1)
    with pytest.raises(asyncio.TimeoutError):
        await pool.acquire(endpoint=2, timeout=sleep_delay)
    assert len(pool) == 1

    await pool.release(conn1, endpoint=1)
    assert len(pool) == 0

    conn1 = await pool.acquire(endpoint=1)
    await pool.release(conn1, endpoint=1)
    assert len(pool) == 0

    await pool.close()


@pytest.mark.parametrize('background_collector', [True, False])
async def test_pool_disposable_connections_collection(
        sleep_delay: float,
        connection_manager: TestConnectionManager,
        background_collector: bool,
):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        idle_timeout=0.0,
        min_idle=0,
        background_collector=background_collector,
        dispose_batch_size=10,
    )

    connections = [
        (endpoint := n % 3, await pool.acquire(endpoint=endpoint))
        for n in range(20)
    ]
    assert len(pool) == len(connections)

    assert connection_manager.creations == connections

    for endpoint, connection in connections:
        await pool.release(connection, endpoint=endpoint)

    await asyncio.sleep(sleep_delay)

    assert len(pool) == 0
    assert connection_manager.disposals == [conn for ep, conn in connections]

    await pool.close()


@pytest.mark.parametrize('background_collector', [True, False])
async def test_pool_min_idle(
        sleep_delay: float,
        connection_manager: TestConnectionManager,
        background_collector: bool,
):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        idle_timeout=0.0,
        min_idle=1,
        background_collector=background_collector,
    )
    conn11 = await pool.acquire(endpoint=1)
    conn12 = await pool.acquire(endpoint=1)
    conn13 = await pool.acquire(endpoint=1)
    conn21 = await pool.acquire(endpoint=2)
    conn22 = await pool.acquire(endpoint=2)
    conn31 = await pool.acquire(endpoint=3)

    assert len(pool) == 6

    assert connection_manager.creations == [
        (1, conn11), (1, conn12), (1, conn13),
        (2, conn21), (2, conn22),
        (3, conn31),
    ]

    await pool.release(conn11, endpoint=1)
    await pool.release(conn12, endpoint=1)
    await pool.release(conn13, endpoint=1)
    await asyncio.sleep(sleep_delay)
    await pool.release(conn21, endpoint=2)
    await pool.release(conn22, endpoint=2)
    await asyncio.sleep(sleep_delay)
    await pool.release(conn31, endpoint=3)

    await asyncio.sleep(sleep_delay)

    assert len(pool) == 3
    assert connection_manager.disposals == [conn11, conn12, conn21]

    await pool.close()
    assert len(pool) == 0


@pytest.mark.parametrize('background_collector', [True, False])
async def test_pool_idle_timeout(
        sleep_delay: float,
        connection_manager: TestConnectionManager,
        background_collector: bool,
):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        idle_timeout=sleep_delay,
        min_idle=1,
        dispose_batch_size=5,
        background_collector=background_collector,
    )

    async with pool.connection(endpoint=1):
        async with pool.connection(endpoint=1):
            pass

    async with pool.connection(endpoint=2):
        async with pool.connection(endpoint=2):
            pass
    assert len(pool) == 4

    await asyncio.sleep(2*sleep_delay)

    # run disposal if background worker is not started
    async with pool.connection(endpoint=3):
        pass
    assert len(pool) == 3

    await pool.close()
    assert len(pool) == 0


@pytest.mark.parametrize('background_collector', [True, False])
async def test_pool_max_lifetime(
        sleep_delay: float,
        connection_manager: TestConnectionManager,
        background_collector: bool,
):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        idle_timeout=sleep_delay,
        max_lifetime=2*sleep_delay,
        min_idle=1,
        dispose_batch_size=4,
        background_collector=background_collector,
    )

    async with pool.connection(endpoint=1):
        async with pool.connection(endpoint=1):
            pass

    async with pool.connection(endpoint=2):
        async with pool.connection(endpoint=2):
            pass
    assert len(pool) == 4

    await asyncio.sleep(3*sleep_delay)

    # run disposal if background worker is not started
    async with pool.connection(endpoint=3):
        pass
    assert len(pool) == 1

    await pool.close()
    assert len(pool) == 0


async def test_pool_aliveness_check(connection_manager: TestConnectionManager):
    pool = ConnectionPool[int, TestConnection](connection_manager)

    async with pool.connection(endpoint=1) as conn1:
        pass

    connection_manager.aliveness[conn1] = False

    async with pool.connection(endpoint=1) as conn2:
        assert conn2 != conn1

    assert len(pool) == 1
    assert connection_manager.dead == [conn1]

    await pool.close()
    assert len(pool) == 0


async def test_pool_test_close(
        sleep_delay: float,
        connection_manager: TestConnectionManager,
):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        idle_timeout=10,
        max_lifetime=10,
        min_idle=10,
        background_collector=False,
    )

    async with pool.connection(endpoint=1):
        async with pool.connection(endpoint=1):
            pass

    async with pool.connection(endpoint=2):
        async with pool.connection(endpoint=2):
            pass

    assert len(pool) == 4
    await pool.close()
    assert len(pool) == 0


async def test_pool_test_close_wait(
        sleep_delay: float,
        connection_manager: TestConnectionManager,
):
    pool = ConnectionPool[int, TestConnection](
        connection_manager,
        idle_timeout=10,
        max_lifetime=10,
        min_idle=10,
        max_size=10,
        total_max_size=100,
        background_collector=False,
    )

    worker_cnt = 50
    endpoint_cnt = 6
    delay_factor = 0.2

    ready = 0
    event = asyncio.Event()

    async def acquire_connection(endpoint, delay):
        nonlocal ready

        async with pool.connection(endpoint=endpoint):
            ready += 1
            event.set()
            while ready < worker_cnt:
                await event.wait()
                event.clear()

            await asyncio.sleep(delay)

    workers = [
        asyncio.create_task(acquire_connection(endpoint=i % endpoint_cnt, delay=random.random() * delay_factor))
        for i in range(worker_cnt)
    ]

    while ready < worker_cnt:
        await event.wait()

    await pool.close(timeout=1.0)
    assert len(pool) == 0

    for worker in workers:
        await worker


async def test_pool_test_close_graceful_timeout(
        sleep_delay: float,
        connection_manager: TestConnectionManager,
):
    pool = ConnectionPool[int, TestConnection](connection_manager)

    acquired = asyncio.Event()

    async def acquire_connection():
        async with pool.connection(endpoint=1):
            acquired.set()
            await asyncio.sleep(sleep_delay)
            assert connection_manager.disposals == []

    worker = asyncio.create_task(acquire_connection())

    await acquired.wait()
    await pool.close(graceful_timeout=2*sleep_delay)

    await worker


async def test_pool_test_close_timeout(
        sleep_delay: float,
        connection_manager: TestConnectionManager,
):
    pool = ConnectionPool[int, TestConnection](connection_manager)

    acquired = asyncio.Event()

    async def acquire_connection():
        async with pool.connection(endpoint=1) as conn:
            acquired.set()
            await asyncio.sleep(2*sleep_delay)
            assert connection_manager.disposals == [conn]

    worker = asyncio.create_task(acquire_connection())

    await acquired.wait()
    await pool.close(graceful_timeout=0.0, timeout=sleep_delay)
    assert len(pool) == 1

    await worker


async def test_pool_connection_manager_creation_error(connection_manager: TestConnectionManager):
    class TestException(Exception):
        pass
    connection_manager.create_err = TestException()

    pool = ConnectionPool[int, TestConnection](connection_manager)
    with pytest.raises(TestException):
        async with pool.connection(endpoint=1):
            pass

    assert len(pool) == 0
    assert pool.get_endpoint_pool_size(endpoint=1) == 0


async def test_pool_connection_manager_release_error(connection_manager: TestConnectionManager):
    class TestException(Exception):
        pass
    connection_manager.on_release_err = TestException()

    pool = ConnectionPool[int, TestConnection](connection_manager, idle_timeout=0.0, min_idle=0)
    with pytest.raises(TestException):
        async with pool.connection(endpoint=1):
            pass

    assert len(pool) == 1
    assert pool.get_endpoint_pool_size(endpoint=1, acquired=False) == 1


async def test_pool_connection_manager_aliveness_error(connection_manager: TestConnectionManager):
    class TestException(Exception):
        pass
    connection_manager.check_aliveness_err = TestException()

    pool = ConnectionPool[int, TestConnection](connection_manager, idle_timeout=1.0, min_idle=1)
    async with pool.connection(endpoint=1):
        pass

    with pytest.raises(TestException):
        async with pool.connection(endpoint=1):
            pass

    assert len(pool) == 1
    assert pool.get_endpoint_pool_size(endpoint=1, acquired=False) == 1


async def test_pool_connection_manager_dead_connection_error(connection_manager: TestConnectionManager):
    class TestException(Exception):
        pass
    connection_manager.on_connection_dead_err = TestException()

    pool = ConnectionPool[int, TestConnection](connection_manager, idle_timeout=1.0, min_idle=1)
    async with pool.connection(endpoint=1) as conn:
        pass

    connection_manager.aliveness[conn] = False
    with pytest.raises(TestException):
        async with pool.connection(endpoint=1):
            pass

    assert len(pool) == 0
    assert pool.get_endpoint_pool_size(endpoint=1) == 0


async def test_pool_connection_manager_acquire_error(connection_manager: TestConnectionManager):
    class TestException(Exception):
        pass
    connection_manager.on_acquire_err = TestException()

    pool = ConnectionPool[int, TestConnection](connection_manager, idle_timeout=1.0, min_idle=1)
    with pytest.raises(TestException):
        async with pool.connection(endpoint=1):
            pass

    assert len(pool) == 1
    assert pool.get_endpoint_pool_size(endpoint=1, acquired=False) == 1


async def test_pool_connection_manager_dispose_error(connection_manager: TestConnectionManager):
    class TestException(Exception):
        pass
    connection_manager.dispose_err = TestException()

    pool = ConnectionPool[int, TestConnection](connection_manager, idle_timeout=0.0, min_idle=0)
    async with pool.connection(endpoint=1):
        pass

    assert len(pool) == 0
    assert pool.get_endpoint_pool_size(endpoint=1, acquired=False) == 0
