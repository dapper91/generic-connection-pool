from typing import Any, Dict, Hashable, Iterator, List, MutableMapping, Optional, Protocol, Tuple, TypeVar


class ComparableP(Protocol):
    def __lt__(self, other: Any) -> bool: ...
    def __eq__(self, other: Any) -> bool: ...


Key = TypeVar('Key', bound=Hashable)
Value = TypeVar('Value', bound=ComparableP)


class RankMap(MutableMapping[Key, Value]):
    """
    A hash-map / binary-heap combined data structure that additionally supports the following operations:
        - top: get the element with the lowest value in O(1)
        - next: removes the element with the lowest value in O(log(n))
    """

    def __init__(self) -> None:
        self._heap: List[Tuple[Value, Key]] = []
        self._index: Dict[Key, int] = {}

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __getitem__(self, key: Key) -> Value:
        idx = self._index[key]
        value, key = self._heap[idx]

        return value

    def __setitem__(self, key: Key, value: Value) -> None:
        self.insert_or_replace(key, value)

    def __delitem__(self, key: Key) -> None:
        self.remove(key)

    def __iter__(self) -> Iterator[Key]:
        return (key for value, key in self._heap)

    def clear(self) -> None:
        """
        Remove all items from the map.
        """

        self._heap.clear()
        self._index.clear()

    def insert(self, key: Key, value: Value) -> None:
        """
        Inserts an item into the map.
        If an item with the provided key already presented raises `KeyError`.

        :param key: item key
        :param value: item value
        """

        if key in self._index:
            raise KeyError("key already exists")

        item_idx = len(self._heap)
        self._heap.append((value, key))
        self._index[key] = item_idx

        self._siftdown(item_idx)

    def insert_or_replace(self, key: Key, value: Value) -> Optional[Value]:
        """
        Inserts an item into the map or replaces it if it already exists.

        :param key: item key
        :param value: item value
        :return: previous item or `None`
        """

        if key in self._index:
            prev = self.remove(key)
        else:
            prev = None

        self.insert(key, value)

        return prev

    def next(self) -> Optional[Value]:
        """
        Pops the smallest item from the map.

        :return: the smallest item
        """

        if len(self._heap) == 0:
            return None

        value, key = self._heap[0]
        return self.remove(key)

    def top(self) -> Optional[Value]:
        """
        Returns the smallest item from the map.

        :return: the smallest item
        """

        if len(self._heap) != 0:
            first_value, first_key = self._heap[0]
            return first_value
        else:
            return None

    def remove(self, key: Key) -> Optional[Value]:
        """
        Removes an item from the map

        :param key: item key
        :returns: removed item value
        """

        if (idx := self._index.get(key)) is None:
            return None

        self._swap_heap_elements(idx, len(self._heap) - 1)

        value, key = self._heap.pop()
        self._index.pop(key)

        if idx < len(self._heap):
            self._siftdown(idx)
            self._siftup(idx)

        return value

    def _swap_heap_elements(self, idx1: int, idx2: int) -> None:
        key1 = self._heap[idx1][1]
        key2 = self._heap[idx2][1]

        self._heap[idx1], self._heap[idx2] = self._heap[idx2], self._heap[idx1]
        self._index[key1] = idx2
        self._index[key2] = idx1

    def _siftup(self, idx: int) -> None:
        value, key = self._heap[idx]

        left_child_idx = 2 * idx + 1
        while left_child_idx < len(self._heap):
            right_child_idx = left_child_idx + 1
            if right_child_idx >= len(self._heap) or self._heap[left_child_idx][0] < self._heap[right_child_idx][0]:
                min_child_idx = left_child_idx
            else:
                min_child_idx = right_child_idx

            child_value, child_key = self._heap[min_child_idx]
            if value < child_value or value == child_value:
                break

            self._heap[idx] = child_value, child_key
            self._index[child_key] = idx

            idx = min_child_idx
            left_child_idx = 2 * idx + 1

        self._heap[idx] = value, key
        self._index[key] = idx

    def _siftdown(self, idx: int) -> None:
        value, key = self._heap[idx]

        while idx > 0:
            parent_idx = (idx - 1) // 2
            parent_value, parent_key = self._heap[parent_idx]
            if value < parent_value:
                self._heap[idx] = parent_value, parent_key
                self._index[parent_key] = idx
                idx = parent_idx
            else:
                break

        self._heap[idx] = value, key
        self._index[key] = idx
