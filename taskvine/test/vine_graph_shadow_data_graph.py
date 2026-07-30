#!/usr/bin/env python3

import json
from pathlib import Path
import tempfile

from ndcctools.taskvine.vine_graph import ShadowDataGraph, Workflow


def passthrough(value):
    return value


def consume(*values):
    return values


def test_shadow_graph():
    with tempfile.TemporaryDirectory(prefix="datavine-phase2-") as temp_dir:
        input_path = Path(temp_dir) / "input.dat"
        input_path.write_bytes(b"phase2-input")

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

        workflow.finalize(
            indexed_data_identity=True,
            shadow_data_graph=True,
        )
        shadow = workflow.shadow_data_graph
        report = shadow.comparison_report()
        assert report["mismatches"] == []
        assert report["counts"]["tasks"] == 3
        assert report["counts"]["idata"] == 4
        assert report["counts"]["workflow_dependency_edges"] == 3
        assert report["counts"]["shadow_dependency_edges"] == 3
        assert report["counts"]["producer_edges"] == 4
        assert all(
            node.availability == "controller"
            for node in shadow.edata.values()
        )
        assert all(node.state == "unproduced" for node in shadow.idata.values())

        producer_task_id = workflow.indexed_data_identity.task_ids[1]
        producer_return_id = workflow.indexed_data_identity.task_bindings[
            producer_task_id
        ].outputs[0].data_id
        consumer_task_ids = {
            consumer.task_id
            for consumer in shadow.idata[producer_return_id].consumers
        }
        assert consumer_task_ids == {2, 3}

        encoded = json.dumps(report, sort_keys=True, separators=(",", ":"))
        assert encoded == json.dumps(
            shadow.comparison_report(), sort_keys=True, separators=(",", ":")
        )

        workflow.finalize(
            indexed_data_identity=True,
            shadow_data_graph=True,
        )
        assert workflow.shadow_data_graph.comparison_report() == report

        workflow.finalize(indexed_data_identity=True)
        assert workflow.indexed_data_identity is not None
        assert workflow.shadow_data_graph is None

        try:
            workflow.finalize(shadow_data_graph=True)
        except ValueError as exc:
            assert "requires indexed-data-identity" in str(exc)
        else:
            raise AssertionError("shadow graph was allowed without Phase 1")


def test_mismatch_fails_closed():
    workflow = Workflow()
    producer = workflow.add_task(passthrough, 1)
    workflow.add_task(consume, producer.output())
    workflow.finalize(indexed_data_identity=True)

    workflow.parents_of[2].clear()
    try:
        ShadowDataGraph.from_workflow(workflow)
    except ValueError as exc:
        assert "dependency edges differ" in str(exc)
    else:
        raise AssertionError("shadow graph accepted mismatched dependency edges")


def main():
    test_shadow_graph()
    test_mismatch_fails_closed()
    print("DataVine Phase 2 shadow graph component tests PASS")


if __name__ == "__main__":
    main()
