import asyncio

import pytest

from generic_connection_pool.asyncio.locks import SharedLock


async def test_shared_mode():
    lock = SharedLock()

    task_cnt = 5
    acquire_cnt = 0
    acquire_event = asyncio.Event()
    finished = asyncio.Event()

    async def acquire_connection():
        nonlocal acquire_cnt

        async with lock.acquired(exclusive=False):
            acquire_cnt += 1
            acquire_event.set()
            await finished.wait()

    tasks = [
        asyncio.create_task(acquire_connection())
        for _ in range(task_cnt)
    ]
    while acquire_cnt < task_cnt:
        await acquire_event.wait()
        acquire_event.clear()

    finished.set()
    await asyncio.gather(*tasks)


@pytest.mark.parametrize(
    'mode1, mode2', [
        (True, True),
        (True, False),
        (False, True),
    ],
)
async def test_exclusive_mode(delay: float, mode1: bool, mode2: bool):
    lock = SharedLock()

    locked = asyncio.Event()
    stopped = asyncio.Event()

    async def acquire_connection(exclusive):
        async with lock.acquired(exclusive=exclusive):
            locked.set()
            await stopped.wait()

    task1 = asyncio.create_task(acquire_connection(exclusive=mode1))

    await locked.wait()
    locked.clear()
    task2 = asyncio.create_task(acquire_connection(exclusive=mode2))

    await asyncio.sleep(delay)
    assert not locked.is_set()

    stopped.set()
    await locked.wait()
    assert locked.is_set()

    await task1
    await task2


@pytest.mark.parametrize(
    'timeout, mode1, mode2', [
        (0.00, True, False),
        (0.00, True, True),
        (0.00, False, True),
        (0.01, True, False),
        (0.01, True, True),
        (0.01, False, True),
    ],
)
async def test_nonblocking_and_timeout(timeout: float, mode1: bool, mode2: bool):
    lock = SharedLock()

    locked = asyncio.Event()
    stopped = asyncio.Event()

    async def acquire_blocking():
        async with lock.acquired(exclusive=mode1):
            locked.set()
            await stopped.wait()

    task1 = asyncio.create_task(acquire_blocking())
    await locked.wait()

    async def acquire_nonblocking():
        assert not await lock.acquire(exclusive=mode2, timeout=timeout)

    task2 = asyncio.create_task(acquire_nonblocking())
    await task2

    stopped.set()
    await task1


async def test_repr():
    task_name = asyncio.current_task().get_name()
    lock = SharedLock()

    assert repr(lock) == 'SharedLock<status=unlocked, owner=None, shared_by=0>'

    async with lock.acquired(exclusive=True):
        assert repr(lock) == f'SharedLock<status=locked exclusively, owner={task_name}, shared_by=0>'

    async with lock.acquired(exclusive=False):
        assert repr(lock) == 'SharedLock<status=locked for share, owner=None, shared_by=1>'
