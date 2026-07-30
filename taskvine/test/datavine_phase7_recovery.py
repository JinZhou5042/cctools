#!/usr/bin/env python3

import argparse
import json

from ndcctools.taskvine.datavine import Workflow
from ndcctools.taskvine.datavine.scheduler.thread import (
    TaskSchedulerThread,
)
from datavine_phase4_demand_pull import run_case


def add(left, right):
    return left + right


def build():
    workflow = Workflow()
    first = workflow.add_task(add, 7, 5)
    second = workflow.add_task(add, first.output(), 10)
    third = workflow.add_task(add, second.output(), first.output())
    target = workflow.add_task(add, third.output(), 1)
    return workflow, first, target, 35


class TransientWorkerStatusManager:
    def __init__(self):
        self.workers = [
            {"workerid": "worker-stable"},
            {"address": "worker-connecting"},
        ]

    def status(self, category):
        assert category == "workers"
        return list(self.workers)


class ReconciliationRecorder:
    def __init__(self):
        self.observations = []

    def reconcile_workers(self, worker_ids):
        self.observations.append(set(worker_ids))


def worker_status_contract():
    controller = ReconciliationRecorder()
    manager = TransientWorkerStatusManager()
    scheduler = TaskSchedulerThread(controller)
    scheduler._manager = manager
    observed = scheduler._sync_worker_epochs()
    assert observed == {"worker-stable"}
    assert controller.observations == []
    assert scheduler._worker_reconciliation_deferrals == 1

    manager.workers = [{"workerid": "worker-stable"}]
    observed = scheduler._sync_worker_epochs()
    assert observed == {"worker-stable"}
    assert controller.observations == [{"worker-stable"}]
    return {
        "incomplete_snapshot_deferred": True,
        "complete_snapshot_reconciled": True,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    status_contract = worker_status_contract()
    workflow, first, target, expected = build()
    recovered = run_case(
        "phase7-global-loss",
        workflow,
        target.task_id,
        expected,
        worker_count=1,
        inject_worker_loss_after=first.task_id,
        replacement_worker_delay=1,
        factory_manager=args.factory_manager,
    )
    report = recovered["scheduler_report"]
    assert report["worker_loss_injected"]
    assert report["recovery_reexecutions"] == 1
    assert report["physical_attempts"] == len(workflow.tasks) + 1
    assert report["local_idata_hits"] >= 3
    assert recovered["available_idata"] == len(workflow.tasks)
    if args.factory_manager:
        assert recovered["taskvine_worker_disconnections"] >= 1
    else:
        assert recovered["taskvine_workers_used"] == 2

    workflow, _, target, expected = build()
    rollback = run_case(
        "phase7-no-loss",
        workflow,
        target.task_id,
        expected,
        worker_count=1,
        factory_manager=args.factory_manager,
    )
    assert rollback["scheduler_report"]["recovery_reexecutions"] == 0
    print(
        json.dumps(
            {
                "recovered": recovered,
                "rollback": rollback,
                "worker_status_contract": status_contract,
            },
            sort_keys=True,
        )
    )
    print(
        "DataVine Phase 7 volatile publication and "
        "unified recovery E2E PASS"
    )


if __name__ == "__main__":
    main()
