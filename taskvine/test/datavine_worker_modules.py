#!/usr/bin/env python3

from ndcctools.taskvine.datavine.models import TaskRecord
from ndcctools.taskvine.datavine.worker.cache import WorkerProcessCache
from ndcctools.taskvine.datavine.worker.outputs import (
    normalize_output_values,
)


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

    cache = first.data
    cache.configure(6)
    assert cache.put("hot", b"abc")
    assert cache.get("hot") == b"abc"
    assert cache.put("cold", b"def")
    assert not cache.put("large-cold", b"123456")
    assert cache.get("hot") == b"abc"
    assert cache.snapshot()["bytes"] <= 6
    cache.configure(2)
    assert cache.snapshot()["bytes"] <= 2

    print("DataVine worker module contracts PASS")


if __name__ == "__main__":
    main()
