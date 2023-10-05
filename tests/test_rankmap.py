import pytest

from generic_connection_pool.rankmap import RankMap


def test_heap_top():
    rmap = RankMap()

    rmap['3'] = 3
    rmap['1'] = 1
    rmap['2'] = 2
    rmap['4'] = 4

    assert rmap.top() == 1
    assert rmap.next() == 1

    assert rmap.top() == 2
    assert rmap.next() == 2

    assert rmap.top() == 3
    assert rmap.next() == 3

    assert rmap.top() == 4
    assert rmap.next() == 4

    assert rmap.top() is None


def test_contains_key():
    rmap = RankMap()

    rmap['a'] = 1
    assert 'a' in rmap
    assert 'b' not in rmap


def test_replace():
    rmap = RankMap()

    rmap['a'] = 1
    assert rmap['a'] == 1

    rmap['a'] = 2
    assert rmap['a'] == 2


def test_heap_push_pop():
    rmap = RankMap()

    items = [0, 2, 1, 4, 3, 5, 6, 7, 8, 12, 11, 10, 9]
    for item in items:
        rmap.insert(item, item)

    actual_result = []
    while rmap:
        actual_result.append(rmap.next())

    expected_result = sorted(items)
    assert actual_result == expected_result
    assert rmap.next() is None
    assert len(rmap) == 0


def test_heap_remove():
    rmap = RankMap()

    rmap.insert(1, 1)
    del rmap[1]

    assert len(rmap) == 0

    rmap.insert(1, 1)
    rmap.insert(2, 2)
    rmap.insert(3, 3)
    rmap.insert(4, 4)

    del rmap[2]
    assert rmap.next() == 1
    assert rmap.next() == 3

    del rmap[4]
    assert len(rmap) == 0


def test_heap_duplicate_error():
    rmap = RankMap()

    rmap.insert(1, 1)
    with pytest.raises(KeyError):
        rmap.insert(1, 1)

    rmap.insert(2, 1)
    with pytest.raises(KeyError):
        rmap.insert(2, 2)


def test_heap_clear():
    rmap = RankMap()

    items = [0, 1, 2]
    for item in items:
        rmap.insert(item, item)

    rmap.clear()
    assert len(rmap) == 0
