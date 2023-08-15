import threading
import time

import pytest

from generic_connection_pool.threading.locks import SharedLock


@pytest.mark.timeout(5.0)
def test_shared_mode():
    lock = SharedLock()

    thread_cnt = 5
    barrier = threading.Barrier(thread_cnt)

    def acquire_connection() -> None:
        with lock.acquired(exclusive=False):
            barrier.wait()

    threads = [
        threading.Thread(target=acquire_connection)
        for _ in range(thread_cnt)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


@pytest.mark.parametrize(
    'mode1, mode2', [
        (True, True),
        (True, False),
        (False, True),
    ],
)
@pytest.mark.timeout(5.0)
def test_exclusive_mode(delay: float, mode1: bool, mode2: bool):
    lock = SharedLock()

    locked = threading.Event()
    stopped = threading.Event()

    def acquire_connection(exclusive: bool):
        with lock.acquired(exclusive=exclusive):
            locked.set()
            stopped.wait()

    thread1 = threading.Thread(target=acquire_connection, kwargs=dict(exclusive=mode1))
    thread1.start()

    locked.wait()
    locked.clear()
    thread2 = threading.Thread(target=acquire_connection, kwargs=dict(exclusive=mode2))
    thread2.start()

    time.sleep(delay)
    assert not locked.is_set()

    stopped.set()
    locked.wait()
    assert locked.is_set()

    thread1.join()
    thread2.join()


@pytest.mark.parametrize(
    'blocking, timeout, mode1, mode2', [
        (False, -1, True, False),
        (False, -1, True, True),
        (False, -1, False, True),

        (True, 0.01, True, False),
        (True, 0.01, True, True),
        (True, 0.01, False, True),
    ],
)
@pytest.mark.timeout(5.0)
def test_nonblocking_and_timeout(blocking: bool, timeout: float, mode1: bool, mode2: bool):
    lock = SharedLock()

    locked = threading.Event()
    stopped = threading.Event()

    def acquire_blocking() -> None:
        with lock.acquired(exclusive=mode1):
            locked.set()
            stopped.wait()

    thread1 = threading.Thread(target=acquire_blocking)
    thread1.start()

    locked.wait()

    def acquire_nonblocking() -> None:
        assert not lock.acquire(exclusive=mode2, blocking=blocking, timeout=timeout)

    thread2 = threading.Thread(target=acquire_nonblocking)
    thread2.start()
    thread2.join()

    stopped.set()
    thread1.join()


@pytest.mark.timeout(5.0)
def test_repr():
    thread_id = threading.get_ident()
    lock = SharedLock()

    assert repr(lock) == 'SharedLock<status=unlocked, owner=None, shared_by=0>'

    with lock.acquired(exclusive=True):
        assert repr(lock) == f'SharedLock<status=locked exclusively, owner={thread_id}, shared_by=0>'

    with lock.acquired(exclusive=False):
        assert repr(lock) == 'SharedLock<status=locked for share, owner=None, shared_by=1>'
