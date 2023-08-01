import pytest

from generic_connection_pool.heap import ExtHeap


def test_heap_top():
    heap = ExtHeap()

    heap.insert(3)
    heap.insert(1)
    heap.insert(2)

    assert heap.top() == 1
    heap.pop()

    assert heap.top() == 2
    heap.pop()

    assert heap.top() == 3
    heap.pop()

    assert heap.top() is None


def test_heap_push_pop():
    heap = ExtHeap()

    items = [0, 2, 1, 4, 3, 5, 6, 7, 8, 12, 11, 10, 9]
    for item in items:
        heap.insert(item)

    actual_result = []
    while heap:
        actual_result.append(heap.pop())

    expected_result = sorted(items)
    assert actual_result == expected_result
    assert heap.pop() is None
    assert len(heap) == 0


def test_heap_multiple_push_pop():
    heap = ExtHeap()

    heap.insert(0)
    heap.insert(1)
    heap.pop()
    heap.insert(0)


def test_heap_replace():
    heap = ExtHeap()

    heap.insert(0)
    heap.replace(0, 1)
    assert len(heap) == 1

    items = [3, 4, 6, 7, 8]
    for item in items:
        heap.insert(item)

    heap.replace(1, 2)
    heap.replace(4, 5)
    heap.replace(6, 0)
    heap.replace(7, 9)
    heap.replace(8, 1)
    heap.replace(9, 4)

    actual_result = []
    while heap:
        actual_result.append(heap.pop())

    expected_result = [0, 1, 2, 3, 4, 5]
    assert actual_result == expected_result


def test_heap_replace_by_copy():
    heap = ExtHeap()

    heap.insert(1)
    heap.insert_or_replace(1)
    heap.insert(2)
    heap.insert_or_replace(2)

    actual_result = []
    while heap:
        actual_result.append(heap.pop())

    expected_result = [1, 2]
    assert actual_result == expected_result


def test_heap_remove():
    heap = ExtHeap()

    heap.insert(1)
    heap.remove(1)

    assert len(heap) == 0

    heap.insert(1)
    heap.insert(2)
    heap.insert(3)
    heap.insert(4)

    heap.remove(2)
    assert heap.pop() == 1
    assert heap.pop() == 3

    heap.remove(4)
    assert len(heap) == 0


def test_heap_duplicate_error():
    heap = ExtHeap()

    heap.insert(1)
    with pytest.raises(KeyError):
        heap.insert(1)

    heap.insert(2)
    with pytest.raises(KeyError):
        heap.replace(2, 1)


def test_heap_clear():
    heap = ExtHeap()

    items = [0, 1, 2]
    for item in items:
        heap.insert(item)

    heap.clear()
    assert len(heap) == 0


def test_heap_not_found_error():
    heap = ExtHeap()

    with pytest.raises(KeyError):
        heap.replace(1, 2)
