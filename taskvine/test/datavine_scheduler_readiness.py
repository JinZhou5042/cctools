#!/usr/bin/env python3

from ndcctools.taskvine.datavine.scheduler.readiness import (
    plan_ready_batches,
    select_ready_tasks,
)


def main():
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
