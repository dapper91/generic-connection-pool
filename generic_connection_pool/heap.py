from typing import Any, Dict, Generic, Hashable, List, Optional, Protocol, TypeVar


class ComparableP(Protocol):
    def __lt__(self, other: Any) -> bool: ...
    def __eq__(self, other: Any) -> bool: ...


class ComparableAndHashable(ComparableP, Protocol, Hashable):
    pass


Item = TypeVar('Item', bound=ComparableAndHashable)


class ExtHeap(Generic[Item]):
    """
    Extended heap data structure implementation.
    Similar to `heapq` but supports remove and replace operations.
    To support remove and replace operations items must be unique.
    """

    def __init__(self) -> None:
        self._heap: List[Item] = []
        self._index: Dict[Item, int] = {}

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __contains__(self, item: Item) -> bool:
        return item in self._index

    def clear(self) -> None:
        """
        Remove all items from the heap.
        """

        self._heap.clear()
        self._index.clear()

    def insert(self, item: Item) -> None:
        """
        Inserts an item onto the heap, maintaining the heap invariant.
        If the item already presented raises `KeyError`.

        :param item: item to be pushed
        """

        if item in self._index:
            raise KeyError("item already exists")

        item_idx = len(self._heap)
        self._heap.append(item)
        self._index[item] = item_idx

        self._siftdown(item_idx)

    def insert_or_replace(self, item: Item) -> None:
        """
        Inserts an item onto the heap or replaces it if it already exists.
        """

        if item in self._index:
            self.remove(item)

        self.insert(item)

    def pop(self) -> Optional[Item]:
        """
        Pops the smallest item off the heap, maintaining the heap invariant.

        :return: the smallest item
        """

        if len(self._heap) == 0:
            return None

        last_item = self._heap.pop()
        self._index.pop(last_item)

        if self._heap:
            first_item = self._heap[0]
            self._index.pop(first_item)
            self._heap[0] = last_item
            self._index[last_item] = 0
            self._siftup(0)
            return first_item

        else:
            return last_item

    def top(self) -> Optional[Item]:
        if len(self._heap) != 0:
            return self._heap[0]
        else:
            return None

    def remove(self, item: Item) -> None:
        """
        Removes an item from the heap.

        :param item: item to be removed
        """

        if item not in self._index:
            return

        idx = self._index.pop(item)
        if idx == len(self._heap) - 1:
            self._heap.pop()
        else:
            last_item = self._heap[-1]
            self._heap[idx] = last_item
            self._heap.pop()
            self._index[last_item] = idx
            self._siftup(idx)

    def replace(self, old_item: Item, new_item: Item) -> None:
        """
        Replaces an item with the new one, maintaining the heap invariant.
        If the old_item not presented raises `KeyError`.
        If the new_item already presented raises `KeyError`.

        :param old_item: item to be replaces
        :param new_item: item the old one is replaced by
        """

        if old_item not in self._index:
            raise KeyError("item not found")

        if new_item in self._index:
            raise KeyError("item already exists")

        old_idx = self._index.pop(old_item)
        self._heap[old_idx] = new_item
        self._index[new_item] = old_idx

        if new_item < old_item:
            self._siftdown(old_idx)
        else:
            self._siftup(old_idx)

    def _siftup(self, idx: int) -> None:
        item = self._heap[idx]

        left_child_idx = 2 * idx + 1
        while left_child_idx < len(self._heap):
            right_child_idx = left_child_idx + 1
            if right_child_idx >= len(self._heap) or self._heap[left_child_idx] < self._heap[right_child_idx]:
                min_child_idx = left_child_idx
            else:
                min_child_idx = right_child_idx

            child = self._heap[min_child_idx]
            if item < child or item == child:
                break

            self._heap[idx] = child
            self._index[child] = idx

            idx = min_child_idx
            left_child_idx = 2 * idx + 1

        self._heap[idx] = item
        self._index[item] = idx

    def _siftdown(self, idx: int) -> None:
        item = self._heap[idx]

        while idx > 0:
            parent_idx = (idx - 1) // 2
            parent = self._heap[parent_idx]
            if item < parent:
                self._heap[idx] = parent
                self._index[parent] = idx
                idx = parent_idx
            else:
                break

        self._heap[idx] = item
        self._index[item] = idx
