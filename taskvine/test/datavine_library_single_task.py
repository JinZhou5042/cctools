#!/usr/bin/env python3

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


def identity(value):
    return value


def main():
    workflow = Workflow()
    target = None
    for value in range(64):
        target = workflow.add_task(identity, value)
    snapshot = run_case(
        "library-single-task",
        workflow,
        target.task_id,
        63,
        worker_count=1,
        worker_cores=4,
        prefetch=False,
        use_worker_library=True,
        detailed_report=False,
    )
    report = snapshot["scheduler_report"]
    assert report["logical_tasks"] == 64
    assert report["physical_compute_submissions"] == 64
    assert report["logical_tasks_per_physical_submission"] == 1
    assert len(report["physical_task_metrics"]) == 64
    assert report["worker_seconds"] > 0
    status_requests = report["scheduler_controller_requests"].get(
        "GET /v1/idata/{id}/status", {}
    ).get("count", 0)
    assert status_requests <= 2
    print("DataVine native single-task library E2E PASS")


if __name__ == "__main__":
    main()
