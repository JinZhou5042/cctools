#!/usr/bin/env python3

import dataclasses
from pathlib import Path
import tempfile

from ndcctools.taskvine.vine_graph import (
    SerializationMetadata,
    SerializedEDataRegistry,
    Workflow,
)


def passthrough(value):
    return value


def consume(*args, **kwargs):
    return args, kwargs


def constant_digest(payload):
    return "forced-collision"


def test_registry():
    registry = SerializedEDataRegistry()
    left = bytearray(b"same-value")
    right = bytearray(b"same-value")
    assert left == right and left is not right

    left_id = registry.register(left)
    right_id = registry.register(right)
    assert left_id == right_id
    assert registry.lookup(bytearray(b"same-value")) == left_id

    record = registry.get(left_id)
    assert record.metadata.serializer == "cloudpickle"
    assert record.metadata.protocol > 0
    assert record.metadata.type_qualname == "bytearray"

    collision_registry = SerializedEDataRegistry(digest_func=constant_digest)
    first_id = collision_registry.register(b"first")
    second_id = collision_registry.register(b"second")
    repeated_id = collision_registry.register(b"first")
    assert first_id != second_id
    assert repeated_id == first_id

    metadata = SerializationMetadata(
        serializer="test",
        serializer_version="1",
        protocol=1,
        python_implementation="test",
        python_version=(1, 0),
        type_module="test",
        type_qualname="value",
    )
    metadata_id = collision_registry.register_serialized(b"same", metadata)
    changed_metadata_id = collision_registry.register_serialized(
        b"same", dataclasses.replace(metadata, protocol=2)
    )
    assert metadata_id != changed_metadata_id


def test_workflow_bindings():
    with tempfile.TemporaryDirectory(prefix="datavine-phase1-") as temp_dir:
        root_path = Path(temp_dir)
        first_file = root_path / "first.dat"
        second_file = root_path / "second.dat"
        first_file.write_bytes(b"identical-file-content")
        second_file.write_bytes(b"identical-file-content")

        workflow = Workflow()
        file_a = workflow.file(first_file)
        file_b = workflow.file(second_file)
        payload_a = bytearray(b"shared-argument")
        payload_b = bytearray(b"shared-argument")

        producer = workflow.add_task(passthrough, payload_a)
        produced_file = producer.file("result.dat")
        consumer = workflow.add_task(
            consume,
            payload_b,
            producer.output(),
            file_a,
            {
                "nested": producer.output()["value"],
                "input_file": file_b,
                "output_file": produced_file,
            },
            named=payload_a,
        )

        workflow.finalize(indexed_data_identity=True)
        identity = workflow.indexed_data_identity
        assert identity is not None
        assert identity.validate()
        assert identity.task_ids == {1: 1, 2: 2}

        producer_binding = identity.task_bindings[1]
        consumer_binding = identity.task_bindings[2]
        assert producer_binding.task_id == 1
        assert consumer_binding.task_id == 2

        producer_argument_id = producer_binding.inputs[0].data_id
        consumer_argument_id = consumer_binding.inputs[0].data_id
        consumer_keyword_id = consumer_binding.inputs[-1].data_id
        assert producer_argument_id == consumer_argument_id == consumer_keyword_id

        direct_dependency = consumer_binding.inputs[1]
        assert direct_dependency.source_kind == "idata"
        assert direct_dependency.data_id == producer_binding.outputs[0].data_id

        first_file_binding = consumer_binding.inputs[2]
        structured_binding = consumer_binding.inputs[3]
        assert first_file_binding.source_kind == "edata"
        assert structured_binding.source_kind == "structured"
        assert {reference.data_kind for reference in structured_binding.references} == {
            "edata",
            "idata",
        }

        file_records = [
            record
            for record in identity.edata.records.values()
            if record.metadata.serializer == "raw-file"
        ]
        assert len(file_records) == 1
        assert first_file_binding.data_id == file_records[0].data_id

        return_output_id = producer_binding.outputs[0].data_id
        file_output_id = producer_binding.outputs[1].data_id
        assert return_output_id != file_output_id
        assert identity.idata[file_output_id].slot_kind == "file"
        assert identity.idata[file_output_id].slot == "result.dat"

        snapshot = (
            dict(identity.task_ids),
            dict(identity.idata),
            dict(identity.task_bindings),
        )
        workflow.finalize(indexed_data_identity=True)
        rebuilt = workflow.indexed_data_identity
        assert rebuilt.validate()
        assert snapshot == (
            rebuilt.task_ids,
            rebuilt.idata,
            rebuilt.task_bindings,
        )

        workflow.finalize(indexed_data_identity=False)
        assert workflow.indexed_data_identity is None


def main():
    test_registry()
    test_workflow_bindings()
    print("DataVine Phase 1 identity component tests PASS")


if __name__ == "__main__":
    main()
