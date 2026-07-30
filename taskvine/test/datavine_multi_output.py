#!/usr/bin/env python3

import argparse
import json

from ndcctools.taskvine.datavine import Workflow

from datavine_phase4_demand_pull import run_case


def produce_pair(value):
    # Equal bytes must remain distinct logical output slots.
    return {"value": value}, {"value": value}


def verify_nested_aliases(container, increment):
    assert container[0] is container[1]
    assert container[2]["alias"] is container[0]
    assert container[3] is container
    return container[0]["value"] + increment


def build_workflow():
    workflow = Workflow()
    producer = workflow.add_task(
        produce_pair,
        41,
        output_count=2,
    )
    demanded = producer.output(0)
    nested = [demanded, demanded, {"alias": demanded}]
    nested.append(nested)
    consumer = workflow.add_task(verify_nested_aliases, nested, 1)
    return workflow, producer, consumer


def validate_snapshot(snapshot, producer, consumer, recovered):
    report = snapshot["scheduler_report"]
    output_slots = report["logical_output_slots"][
        str(producer.task_id)
    ]
    assert len(output_slots) == 2
    assert output_slots[0] != output_slots[1]
    expected_attempts = 2 if recovered else 1
    output_status = report["logical_output_status"]
    first_status = output_status[str(output_slots[0])]
    second_status = output_status[str(output_slots[1])]
    assert first_status["producer_output_index"] == 0
    assert second_status["producer_output_index"] == 1
    assert first_status["content_hash"] == second_status["content_hash"]
    assert first_status["attempt"] == expected_attempts
    assert second_status["attempt"] == expected_attempts
    assert report["logical_output_slots"][str(consumer.task_id)] == [3]
    assert snapshot["idata"] == 3
    assert snapshot["available_idata"] == 3
    assert report["legacy_recovery_tasks"] == 0
    assert report["attempts_by_task"][str(producer.task_id)] == (
        expected_attempts
    )
    assert report["attempts_by_task"][str(consumer.task_id)] == 1
    assert report["physical_attempts"] == (3 if recovered else 2)
    assert report["recovery_reexecutions"] == (1 if recovered else 0)
    if recovered:
        assert report["recovery_waves"] == [
            {
                "tasks": [producer.task_id],
                "rollback_depth": 1,
                "recovery_depths": report["recovery_waves"][0][
                    "recovery_depths"
                ],
            }
        ]
    return output_slots


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory-manager")
    args = parser.parse_args()

    normal_workflow, normal_producer, normal_consumer = build_workflow()
    normal = run_case(
        "multi-output-normal",
        normal_workflow,
        normal_consumer.task_id,
        42,
        factory_manager=args.factory_manager,
        prefetch=False,
    )
    normal_slots = validate_snapshot(
        normal, normal_producer, normal_consumer, False
    )

    recovery_workflow, recovery_producer, recovery_consumer = (
        build_workflow()
    )
    recovered = run_case(
        "multi-output-recovery",
        recovery_workflow,
        recovery_consumer.task_id,
        42,
        factory_manager=args.factory_manager,
        prefetch=False,
        inject_global_loss_after=recovery_producer.task_id,
    )
    recovery_slots = validate_snapshot(
        recovered, recovery_producer, recovery_consumer, True
    )
    assert normal_slots == recovery_slots
    print(
        json.dumps(
            {
                "alias_identity_preserved": True,
                "normal_output_slots": normal_slots,
                "partial_demand_output_index": 0,
                "recovery_output_slots": recovery_slots,
                "stable_across_retry": True,
                "status": "PASS",
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
