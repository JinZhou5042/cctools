#!/usr/bin/env python3

import sys

from ndcctools.taskvine.datavine.controller.stores import DenseIdStore
from ndcctools.taskvine.datavine.models import (
    EDataRecord,
    IDataRecord,
    SerializationMetadata,
    TaskRecord,
)
from ndcctools.taskvine.datavine.workflow import OutputRef, WorkflowTask


def main():
    store = DenseIdStore()
    assert len(store) == 0
    assert store.get(1) is None
    store[1] = "one"
    store[3] = "three"
    assert len(store) == 2
    assert store.allocated_slots == 3
    assert store[1] == "one"
    assert store.get(2, "missing") == "missing"
    assert 1 in store and 2 not in store and 0 not in store
    store.update({2: "two", 3: "THREE"})
    assert len(store) == 3
    assert tuple(store.values()) == ("one", "two", "THREE")
    assert tuple(store.items()) == (
        (1, "one"),
        (2, "two"),
        (3, "THREE"),
    )
    try:
        store[0] = "zero"
    except KeyError:
        pass
    else:
        raise AssertionError("accepted non-positive ID")

    count = 100_000
    dense = DenseIdStore()
    sparse = {}
    marker = object()
    for key in range(1, count + 1):
        dense[key] = marker
        sparse[key] = marker
    dense_bytes = sys.getsizeof(dense._items)
    dict_bytes = sys.getsizeof(sparse)
    assert dense_bytes < dict_bytes / 3, (dense_bytes, dict_bytes)

    for record_type in (
        SerializationMetadata,
        EDataRecord,
        IDataRecord,
        TaskRecord,
        OutputRef,
        WorkflowTask,
    ):
        assert "__dict__" not in record_type.__dict__, record_type

    print("DataVine dense ID store contract PASS")


if __name__ == "__main__":
    main()
