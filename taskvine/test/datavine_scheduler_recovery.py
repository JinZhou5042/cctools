#!/usr/bin/env python3

from ndcctools.taskvine.datavine.scheduler.recovery import (
    select_recovery_audit_data_ids,
)


def main():
    dependencies = {
        1: set(),
        2: {1},
        3: {1},
        4: {2, 3},
        5: set(),
    }
    dependents = {
        1: {2, 3},
        2: {4},
        3: {4},
        4: set(),
        5: set(),
    }
    producers = {11: 1, 12: 2, 13: 3, 14: 4, 15: 5, 16: 4}

    assert select_recovery_audit_data_ids(
        {11, 12, 14, 16},
        producers,
        dependencies,
        dependents,
        (1, 2, 3, 4, 5),
    ) == (14,)
    assert select_recovery_audit_data_ids(
        {12, 13, 15},
        producers,
        dependencies,
        dependents,
        (1, 2, 3, 4, 5),
    ) == (12, 13, 15)
    assert select_recovery_audit_data_ids(
        set(), producers, dependencies, dependents, (1, 2, 3, 4, 5)
    ) == ()

    print("DataVine Scheduler recovery planner contract PASS")


if __name__ == "__main__":
    main()
