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
        "library-batching",
        workflow,
        target.task_id,
        63,
        worker_count=1,
        worker_cores=4,
        prefetch=False,
        use_worker_library=True,
        library_batch_size=32,
        detailed_report=False,
    )
    report = snapshot["scheduler_report"]
    assert report["logical_tasks"] == 64
    assert report["physical_compute_submissions"] < 64
    assert report["library_batch_size"] == 32
    assert report["logical_tasks_per_physical_submission"] > 1
    assert report["batch_worker_seconds"] >= 0
    assert report["physical_batch_metrics"]
    print("DataVine worker-library batching E2E PASS")


if __name__ == "__main__":
    main()
