#!/usr/bin/env python3

from ndcctools.taskvine.datavine.scheduler.persistence import (
    PersistencePolicy,
)


def main():
    policy = PersistencePolicy.from_options(2, 3, 0.25, 1.0, 2)
    assert policy.injected_failures == 2
    assert policy.maximum_retries == 3
    assert [policy.retry_delay(value) for value in range(5)] == [
        0.25,
        0.5,
        1.0,
        1.0,
        1.0,
    ]
    for position in range(5):
        values = [0, 0, 0.0, 0.0, 0.0]
        values[position] = -1
        try:
            PersistencePolicy.from_options(*values)
        except ValueError as exc:
            assert "cannot be negative" in str(exc)
        else:
            raise AssertionError(f"accepted invalid policy {values}")
    try:
        policy.retry_delay(-1)
    except ValueError as exc:
        assert "retry count" in str(exc)
    else:
        raise AssertionError("accepted negative retry count")

    print("DataVine Scheduler persistence policy contract PASS")


if __name__ == "__main__":
    main()
