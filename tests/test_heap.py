import pytest

from generic_connection_pool.heap import ExtHeap


def test_heap_top():
    heap = ExtHeap()

    heap.push(3)
    heap.push(1)
    heap.push(2)

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
        heap.push(item)

    actual_result = []
    while heap:
        actual_result.append(heap.pop())

    expected_result = sorted(items)
    assert actual_result == expected_result
    assert heap.pop() is None
    assert len(heap) == 0


def test_heap_multiple_push_pop():
    heap = ExtHeap()

    heap.push(0)
    heap.push(1)
    heap.pop()
    heap.push(0)


def test_heap_replace():
    heap = ExtHeap()

    heap.push(0)
    heap.replace(0, 1)
    assert len(heap) == 1

    items = [3, 4, 6, 7, 8]
    for item in items:
        heap.push(item)

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


def test_heap_remove():
    heap = ExtHeap()

    heap.push(1)
    heap.remove(1)

    assert len(heap) == 0

    heap.push(1)
    heap.push(2)
    heap.push(3)
    heap.push(4)

    heap.remove(2)
    assert heap.pop() == 1
    assert heap.pop() == 3

    heap.remove(4)
    assert len(heap) == 0


def test_heap_duplicate_error():
    heap = ExtHeap()

    heap.push(1)
    with pytest.raises(ValueError):
        heap.push(1)

    heap.push(2)
    with pytest.raises(ValueError):
        heap.replace(2, 1)


def test_heap_clear():
    heap = ExtHeap()

    items = [0, 1, 2]
    for item in items:
        heap.push(item)

    heap.clear()
    assert len(heap) == 0


def test_heap_not_found_error():
    heap = ExtHeap()

    with pytest.raises(ValueError):
        heap.remove(1)

    with pytest.raises(ValueError):
        heap.replace(1, 2)
