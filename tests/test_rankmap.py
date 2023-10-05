import pytest

from generic_connection_pool.rankmap import RankMap


def test_heap_top():
    rm = RankMap()

    rm.insert(3)
    rm.insert(1)
    rm.insert(2)

    assert rm.top() == 1
    rm.pop()

    assert rm.top() == 2
    rm.pop()

    assert rm.top() == 3
    rm.pop()

    assert rm.top() is None


def test_heap_push_pop():
    rm = RankMap()

    items = [0, 2, 1, 4, 3, 5, 6, 7, 8, 12, 11, 10, 9]
    for item in items:
        rm.insert(item)

    actual_result = []
    while rm:
        actual_result.append(rm.pop())

    expected_result = sorted(items)
    assert actual_result == expected_result
    assert rm.pop() is None
    assert len(rm) == 0


def test_heap_multiple_push_pop():
    rm = RankMap()

    rm.insert(0)
    rm.insert(1)
    rm.pop()
    rm.insert(0)


def test_heap_replace():
    rm = RankMap()

    rm.insert(0)
    rm.replace(0, 1)
    assert len(rm) == 1

    items = [3, 4, 6, 7, 8]
    for item in items:
        rm.insert(item)

    rm.replace(1, 2)
    rm.replace(4, 5)
    rm.replace(6, 0)
    rm.replace(7, 9)
    rm.replace(8, 1)
    rm.replace(9, 4)

    actual_result = []
    while rm:
        actual_result.append(rm.pop())

    expected_result = [0, 1, 2, 3, 4, 5]
    assert actual_result == expected_result


def test_heap_replace_by_copy():
    rm = RankMap()

    rm.insert(1)
    rm.insert_or_replace(1)
    rm.insert(2)
    rm.insert_or_replace(2)

    actual_result = []
    while rm:
        actual_result.append(rm.pop())

    expected_result = [1, 2]
    assert actual_result == expected_result


def test_heap_remove():
    rm = RankMap()

    rm.insert(1)
    rm.remove(1)

    assert len(rm) == 0

    rm.insert(1)
    rm.insert(2)
    rm.insert(3)
    rm.insert(4)

    rm.remove(2)
    assert rm.pop() == 1
    assert rm.pop() == 3

    rm.remove(4)
    assert len(rm) == 0


def test_heap_duplicate_error():
    rm = RankMap()

    rm.insert(1)
    with pytest.raises(KeyError):
        rm.insert(1)

    rm.insert(2)
    with pytest.raises(KeyError):
        rm.replace(2, 1)


def test_heap_clear():
    rm = RankMap()

    items = [0, 1, 2]
    for item in items:
        rm.insert(item)

    rm.clear()
    assert len(rm) == 0


def test_heap_not_found_error():
    rm = RankMap()

    with pytest.raises(KeyError):
        rm.replace(1, 2)
