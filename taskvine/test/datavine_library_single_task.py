#!/usr/bin/env python3

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


def identity(value):
    return value


def total(*values):
    return sum(values)


def main():
    workflow = Workflow()
    leaves = [workflow.add_task(identity, value) for value in range(64)]
    target = workflow.add_task(total, *(leaf.output() for leaf in leaves))
    snapshot = run_case(
        "library-single-task",
        workflow,
        target.task_id,
        sum(range(64)),
        worker_count=1,
        worker_cores=4,
        prefetch=False,
        use_worker_library=True,
        detailed_report=False,
    )
    report = snapshot["scheduler_report"]
    assert report["logical_tasks"] == 65
    assert report["physical_compute_submissions"] == 65
    assert report["logical_tasks_per_physical_submission"] == 1
    assert len(report["physical_task_metrics"]) == 65
    assert report["worker_seconds"] > 0
    events = []
    for task in report["physical_task_metrics"]:
        events.append((task["time_workers_execute_last_start"], 1))
        events.append((task["time_workers_execute_last_end"], -1))
    active = 0
    high_water = 0
    for _, delta in sorted(events, key=lambda event: (event[0], -event[1])):
        active += delta
        high_water = max(high_water, active)
    assert high_water >= 2
    status_requests = report["scheduler_controller_requests"].get(
        "GET /v1/idata/{id}/status", {}
    ).get("count", 0)
    assert status_requests <= 2
    print("DataVine native single-task library E2E PASS")


if __name__ == "__main__":
    main()
