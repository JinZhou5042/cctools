#!/usr/bin/env python3

from types import SimpleNamespace

from ndcctools.taskvine.datavine.scheduler.readiness import (
    build_cache_plan,
    plan_ready_batches,
    select_ready_tasks,
)


def main():
    records = {
        1: SimpleNamespace(
            function_data_id=10,
            positional=(("c", 11), ("i", 20), ("e", 12)),
            keyword=(("arg", ("e", 12)),),
        ),
        2: SimpleNamespace(
            function_data_id=10,
            positional=(("i", 20),),
            keyword=(),
        ),
    }
    cache_plan = build_cache_plan(
        records,
        records.__getitem__,
        {1: (21,)},
        {1: (30,), 2: (31, 32)},
        {"e:10": 5, "e:11": 7, "e:12": 11, "i:20": 13, "i:21": 17}.__getitem__,
        retention_items=9,
        retention_bytes=100,
        admission_items=8,
        admission_bytes=60,
    )
    assert cache_plan.task_inputs == {
        1: {"e:10", "e:11", "e:12", "i:20", "i:21"},
        2: {"e:10", "i:20"},
    }
    assert cache_plan.remaining_uses["e:10"] == 2
    assert cache_plan.max_task_items == 6
    assert cache_plan.max_known_input_bytes == 53
    assert cache_plan.retention_items == 2
    assert cache_plan.retention_bytes == 7

    for capacity_name, capacity in (("admission_items", 5), ("admission_bytes", 52)):
        arguments = {capacity_name: capacity}
        try:
            build_cache_plan(
                records,
                records.__getitem__,
                {1: (21,)},
                {1: (30,), 2: (31, 32)},
                {
                    "e:10": 5,
                    "e:11": 7,
                    "e:12": 11,
                    "i:20": 13,
                    "i:21": 17,
                }.__getitem__,
                **arguments,
            )
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted insufficient {capacity_name}")

    dependencies = {
        1: set(),
        2: {1},
        3: {1},
        4: set(),
        5: set(),
    }
    cache_inputs = {
        1: {"shared"},
        2: {"shared", "two"},
        3: {"shared", "three"},
        4: {"pruning"},
        5: {"five"},
    }
    ready = select_ready_tasks(
        {5, 4, 3, 2, 1},
        dependencies,
        {1},
        cache_inputs,
        {"pruning"},
        lambda task_id: task_id != 5,
    )
    assert ready == (1, 2, 3)

    sizes = {
        "shared": 8,
        "two": 5,
        "three": 5,
        "five": 1,
    }
    assert plan_ready_batches(
        ready,
        set(),
        cache_inputs,
        sizes,
        maximum_batch_size=8,
        connected_slots=1,
        input_byte_limit=18,
    ) == ((1, 2, 3),)
    assert plan_ready_batches(
        ready,
        {2},
        cache_inputs,
        sizes,
        maximum_batch_size=8,
        connected_slots=1,
        input_byte_limit=18,
    ) == ((1,), (2,), (3,))
    assert plan_ready_batches(
        ready,
        set(),
        cache_inputs,
        sizes,
        maximum_batch_size=8,
        connected_slots=2,
        input_byte_limit=12,
    ) == ((1,), (2,), (3,))
    assert plan_ready_batches(
        (), set(), cache_inputs, sizes, 1, 1
    ) == ()

    for position in range(3):
        values = [1, 1, 1]
        values[position] = 0
        try:
            plan_ready_batches((), set(), {}, {}, *values)
        except ValueError:
            pass
        else:
            raise AssertionError(f"accepted invalid planner values {values}")

    print("DataVine Scheduler readiness contract PASS")


if __name__ == "__main__":
    main()
