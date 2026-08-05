#!/usr/bin/env python3

from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.worker.cache import WorkerProcessCache
from ndcctools.taskvine.datavine.worker.outputs import (
    normalize_output_values,
)
from ndcctools.taskvine.datavine.worker.cache import WorkerMemoryStore


def main():
    single = TaskRecord(1, 2, (), (), (3,), ())
    multiple = TaskRecord(2, 2, (), (), (4, 5), ())
    assert normalize_output_values(single, [1, 2]) == ([1, 2],)
    assert normalize_output_values(multiple, [1, 2]) == (1, 2)
    for result, error_type in ((1, TypeError), ((1,), ValueError)):
        try:
            normalize_output_values(multiple, result)
        except error_type:
            pass
        else:
            raise AssertionError("accepted invalid multi-output result")

    first = WorkerProcessCache()
    second = WorkerProcessCache()
    first.clients["controller"] = object()
    first.task_records[1] = single
    assert not second.clients
    assert not second.task_records
    first.clear()
    assert not first.clients
    assert not first.task_records

    shared = WorkerMemoryStore()
    digest = "0" * 64
    assert shared.put("scope", "i:1", digest, b"abc", 6, 1, True)[0]
    assert shared.find("scope", "i:1", digest, 3) == b"abc"
    assert shared.local("scope", "i:1") == b"abc"
    assert not shared.put(
        "scope", "i:2", digest, b"defg", 6, 1, True
    )[0]
    assert shared.put(
        "scope", "i:1", "1" * 64, b"xy", 6, 1, True
    )[0]
    assert shared.find("scope", "i:1", digest, 3) is None
    assert shared.remove("scope", "i:1", "1" * 64, 2)
    assert shared.bytes == 0

    print("DataVine worker module contracts PASS")


if __name__ == "__main__":
    main()
