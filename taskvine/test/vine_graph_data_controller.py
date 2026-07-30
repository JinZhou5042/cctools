#!/usr/bin/env python3

import dataclasses
import json
from pathlib import Path
import tempfile

import cloudpickle

from ndcctools.taskvine.vine_graph import (
    LegacyMountExpectation,
    VineGraph,
    Workflow,
)


def passthrough(value):
    return value


def consume(*values):
    return values


def build_workflow(temp_dir):
    input_path = Path(temp_dir) / "input.dat"
    input_path.write_bytes(b"phase3-input")

    workflow = Workflow()
    input_file = workflow.file(input_path)
    producer = workflow.add_task(passthrough, b"shared")
    produced_file = producer.file("produced.dat")
    middle = workflow.add_task(
        consume,
        producer.output(),
        {
            "nested": producer.output()["value"],
            "input": input_file,
            "produced": produced_file,
        },
    )
    workflow.add_task(consume, producer.output(), middle.output())
    return workflow


def test_controller_registry_and_queries():
    with tempfile.TemporaryDirectory(prefix="datavine-phase3-") as temp_dir:
        workflow = build_workflow(temp_dir)
        workflow.finalize(
            indexed_data_identity=True,
            shadow_data_graph=True,
            data_controller=True,
        )
        controller = workflow.data_controller
        assert controller is not None
        assert workflow.indexed_data_identity is None
        assert workflow.shadow_data_graph is None
        assert controller.validate()
        assert controller.summary() == {
            "tasks": 3,
            "edata": 5,
            "idata": 4,
            "input_bindings": 5,
            "output_bindings": 4,
        }
        assert controller.comparison_report()["mismatches"] == []
        assert set(controller.edata_availability.values()) == {"controller"}
        assert set(controller.idata_state.values()) == {"unproduced"}

        producer_plan = controller.materialization_plan(
            controller.task_id_for(1)
        )
        middle_plan = controller.materialization_plan(
            controller.task_id_for(2)
        )
        assert producer_plan.parent_task_ids == ()
        assert middle_plan.parent_task_ids == (1,)
        assert len(middle_plan.input_file_data_ids) == 2
        assert len(producer_plan.output_file_data_ids) == 1

        for workflow_key in workflow.task_dict:
            expectation = controller.legacy_mount_expectation(
                workflow, workflow_key
            )
            assert expectation.task_id == controller.task_id_for(workflow_key)

        try:
            controller.tasks[1] = producer_plan
        except TypeError:
            pass
        else:
            raise AssertionError("Controller task registry is mutable")
        try:
            producer_plan.task_id = 99
        except dataclasses.FrozenInstanceError:
            pass
        else:
            raise AssertionError("Controller task plan is mutable")

        encoded = json.dumps(
            controller.comparison_report(),
            sort_keys=True,
            separators=(",", ":"),
        )
        rebuilt = cloudpickle.loads(cloudpickle.dumps(controller))
        assert json.dumps(
            rebuilt.comparison_report(),
            sort_keys=True,
            separators=(",", ":"),
        ) == encoded
        try:
            rebuilt.edata[1] = rebuilt.edata[1]
        except TypeError:
            pass
        else:
            raise AssertionError("pickled Controller registry became mutable")

        assert controller.audit_report()["audited_tasks"] == 0
        for task_id in controller.tasks:
            controller.record_materialization_audit(task_id, 1)
        assert controller.audit_report()["mismatches"] == []
        assert controller.audit_report()["audited_tasks"] == 3


def test_controller_fails_closed():
    with tempfile.TemporaryDirectory(prefix="datavine-phase3-") as temp_dir:
        workflow = build_workflow(temp_dir)
        workflow.finalize(
            indexed_data_identity=True,
            shadow_data_graph=True,
            data_controller=True,
        )
        workflow.parents_of[2].clear()
        try:
            workflow.data_controller.legacy_mount_expectation(workflow, 2)
        except ValueError as exc:
            assert "legacy parent bindings differ" in str(exc)
        else:
            raise AssertionError("Controller accepted mismatched legacy lineage")

        for kwargs in (
            {"data_controller": True},
            {"indexed_data_identity": True, "data_controller": True},
            {"shadow_data_graph": True, "data_controller": True},
        ):
            other = build_workflow(temp_dir)
            try:
                other.finalize(**kwargs)
            except ValueError as exc:
                assert "data-controller requires" in str(exc)
            else:
                raise AssertionError("Controller accepted missing prerequisite")


def test_c_materializer_expectation_fails_closed():
    workflow = Workflow()
    producer = workflow.add_task(passthrough, 1)
    workflow.add_task(consume, producer.output())
    with VineGraph(port=0) as manager:
        manager.set_params(
            {
                "indexed-data-identity": 1,
                "shadow-data-graph": 1,
                "data-controller": 1,
            }
        )
        py_graph = manager.build_workflow(workflow)
        bridge = manager.build_capi_bridge(py_graph, [])
        try:
            task_id = py_graph.data_controller.task_id_for(2)
            wrong = LegacyMountExpectation(
                task_id=task_id,
                parent_inputs=2,
                extra_inputs=0,
                extra_outputs=0,
            )
            try:
                bridge.set_data_binding_expectations(2, wrong)
            except RuntimeError as exc:
                assert "expectation mismatch" in str(exc)
            else:
                raise AssertionError("C materializer accepted wrong bindings")
        finally:
            bridge.delete()


def main():
    test_controller_registry_and_queries()
    test_controller_fails_closed()
    test_c_materializer_expectation_fails_closed()
    print("DataVine Phase 3 Data Controller component tests PASS")


if __name__ == "__main__":
    main()
