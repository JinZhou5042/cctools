#!/usr/bin/env python3

import os
from pathlib import Path
import tempfile

from ndcctools.taskvine.vine_graph import (
    DataReference,
    WorkerDataAgent,
    Workflow,
)


def passthrough(value):
    return value


def consume(*values):
    return values


def build_workflow(temp_dir):
    input_path = Path(temp_dir) / "input.dat"
    input_path.write_bytes(b"phase4-input")
    workflow = Workflow()
    input_file = workflow.file(input_path)
    producer = workflow.add_task(passthrough, b"shared")
    produced_file = producer.file("produced.dat")
    consumer = workflow.add_task(
        consume,
        producer.output(),
        input_file,
        produced_file,
        b"shared",
    )
    workflow.finalize(
        indexed_data_identity=True,
        shadow_data_graph=True,
        data_controller=True,
        worker_data_agent=True,
    )
    return workflow, producer, consumer


def materialize_legacy_sources(workflow, temp_dir):
    old_cwd = os.getcwd()
    os.chdir(temp_dir)
    try:
        for workflow_key in workflow.task_dict:
            workflow.outfile_remote_name[workflow_key] = (
                f"outfile_node_{workflow_key}"
            )
        Path(workflow.outfile_remote_name[1]).write_bytes(b"result")
        for file_id in workflow.input_files:
            Path(workflow.file_input_path(file_id)).write_bytes(b"input")
        for file_id in workflow.output_files:
            Path(workflow.file_input_path(file_id)).write_bytes(b"produced")
        yield
    finally:
        os.chdir(old_cwd)


def test_inventory_and_source_resolution():
    with tempfile.TemporaryDirectory(prefix="datavine-phase4-") as temp_dir:
        workflow, _, _ = build_workflow(temp_dir)
        controller = workflow.data_controller
        source_assignment = controller.worker_assignment(1)
        consumer_assignment = controller.worker_assignment(2)
        assert source_assignment.startswith("T1|")
        assert consumer_assignment.startswith("T2|")
        assert len(consumer_assignment) < 128
        task_id, references = controller.parse_worker_assignment(
            consumer_assignment
        )
        assert task_id == 2
        assert references == controller.required_data_references(2)
        assert any(ref.data_kind == "idata" for ref in references)

        for _ in materialize_legacy_sources(workflow, temp_dir):
            agent = WorkerDataAgent()
            empty = agent.prepare(controller, workflow, consumer_assignment)
            assert empty.local_before == ()
            assert empty.missing == references
            assert len(empty.resolved) == len(references)
            assert {
                source.source_kind for source in empty.resolved
            } >= {
                "controller-context",
                "legacy-input-file",
                "legacy-parent-output",
                "legacy-produced-file",
            }

            complete = agent.prepare(
                controller, workflow, consumer_assignment
            )
            assert complete.local_before == references
            assert complete.missing == ()
            assert complete.resolved == ()

            partial = WorkerDataAgent()
            partial.seed_inventory(references[:2])
            partial_report = partial.prepare(
                controller, workflow, consumer_assignment
            )
            assert partial_report.local_before == references[:2]
            assert partial_report.missing == references[2:]


def test_stale_unknown_and_prerequisites_fail_closed():
    with tempfile.TemporaryDirectory(prefix="datavine-phase4-") as temp_dir:
        workflow, _, _ = build_workflow(temp_dir)
        controller = workflow.data_controller
        assignment = controller.worker_assignment(2)
        _, references = controller.parse_worker_assignment(assignment)
        for _ in materialize_legacy_sources(workflow, temp_dir):
            file_reference = next(
                reference
                for reference in references
                if controller.resolve_stable_source(
                    workflow, 2, reference
                ).source_kind == "legacy-input-file"
            )
            source = controller.resolve_stable_source(
                workflow, 2, file_reference
            )
            agent = WorkerDataAgent()
            agent.seed_inventory((file_reference,))
            Path(source.locator).unlink()
            try:
                agent.prepare(controller, workflow, assignment)
            except FileNotFoundError as exc:
                assert "no available stable source" in str(exc)
            else:
                raise AssertionError("stale source was accepted")

        for invalid in (
            "bad",
            "T999|e1",
            assignment + ",i999",
        ):
            try:
                WorkerDataAgent().prepare(
                    controller, workflow, invalid
                )
            except (KeyError, ValueError):
                pass
            else:
                raise AssertionError(f"invalid assignment accepted: {invalid}")

        extra = DataReference("edata", 999)
        try:
            controller.resolve_stable_source(workflow, 2, extra)
        except ValueError:
            pass
        else:
            raise AssertionError("unrequired DataID was resolved")

        other = Workflow()
        other.add_task(passthrough, 1)
        try:
            other.finalize(worker_data_agent=True)
        except ValueError as exc:
            assert "requires data-controller" in str(exc)
        else:
            raise AssertionError("worker agent accepted missing Controller")


def main():
    test_inventory_and_source_resolution()
    test_stale_unknown_and_prerequisites_fail_closed()
    print("DataVine Phase 4 Worker Data Agent component tests PASS")


if __name__ == "__main__":
    main()
