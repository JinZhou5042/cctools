"""Memory-efficient stores for positive, mostly contiguous DataVine IDs."""


class DenseIdStore:
    """List-backed positive-integer mapping with a minimal dict interface."""

    __slots__ = ("_items", "_count")

    def __init__(self):
        self._items = [None]
        self._count = 0

    @staticmethod
    def _key(value):
        key = int(value)
        if key < 1:
            raise KeyError(value)
        return key

    def __len__(self):
        return self._count

    def __contains__(self, key):
        try:
            key = self._key(key)
        except (KeyError, TypeError, ValueError):
            return False
        return key < len(self._items) and self._items[key] is not None

    def __getitem__(self, key):
        key = self._key(key)
        if key >= len(self._items) or self._items[key] is None:
            raise KeyError(key)
        return self._items[key]

    def __setitem__(self, key, value):
        key = self._key(key)
        if key >= len(self._items):
            self._items.extend([None] * (key + 1 - len(self._items)))
        if self._items[key] is None:
            self._count += 1
        self._items[key] = value

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, TypeError, ValueError):
            return default

    def update(self, values):
        source = values.items() if hasattr(values, "items") else values
        for key, value in source:
            self[key] = value

    def values(self):
        return (
            value for value in self._items[1:] if value is not None
        )

    def items(self):
        return (
            (key, value)
            for key, value in enumerate(self._items[1:], 1)
            if value is not None
        )

    @property
    def allocated_slots(self):
        return len(self._items) - 1
