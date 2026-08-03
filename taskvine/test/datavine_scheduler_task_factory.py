#!/usr/bin/env python3

from ndcctools.taskvine.datavine.scheduler.run_context import (
    WorkflowRunContext,
)
from ndcctools.taskvine.datavine.scheduler.task_factory import TaskFactory


class FakeFile:
    def __init__(self, declaration):
        self.declaration = declaration
        self.data_id = None
        self.content_hash = None

    def set_datavine_data_id(self, data_id):
        self.data_id = data_id
        return True

    def set_datavine_content_hash(self, content_hash):
        self.content_hash = content_hash
        return True


class FakeManager:
    def __init__(self):
        self.declarations = []

    def _declare(self, kind, value, **options):
        file_object = FakeFile((kind, value, options))
        self.declarations.append(file_object)
        return file_object

    def declare_url(self, value, **options):
        return self._declare("url", value, **options)

    def declare_url_cached(self, value, cached_name, **options):
        return self._declare(
            "url", value, cached_name=cached_name, **options
        )

    def declare_file(self, value, **options):
        return self._declare("file", value, **options)


class FakeController:
    endpoint = "http://127.0.0.1:1234"
    token = "secret token"

    def __init__(self):
        self.metadata_fetches = 0

    def get_edata_metadata(self, data_id):
        self.metadata_fetches += 1
        return {
            "data_id": data_id,
            "storage": "controller-memory",
            "serialized_sha256": f"hash-{data_id}",
        }


def main():
    manager = FakeManager()
    controller = FakeController()
    context = WorkflowRunContext()
    context.edata_info[1] = {
        "data_id": 1,
        "storage": "bulk-origin",
        "origin_path": "/origin/one.pkl",
        "serialized_sha256": "bulk-hash",
    }
    edata_files = {}
    idata_files = {}
    factory = TaskFactory(
        manager,
        controller,
        context,
        lambda task_id: None,
        edata_files,
        idata_files,
    )

    bulk = factory.edata_file(1)
    assert bulk.declaration[0:2] == ("file", "/origin/one.pkl")
    assert bulk.data_id == "e:1"
    assert bulk.content_hash == "bulk-hash"
    assert factory.edata_file(1) is bulk

    controller_edata = factory.edata_file(2)
    assert controller_edata.declaration[0] == "url"
    assert "/v1/edata/2?" in controller_edata.declaration[1]
    assert "secret+token" in controller_edata.declaration[1]
    assert controller_edata.data_id == "e:2"
    assert controller_edata.content_hash == "hash-2"
    assert controller_edata.declaration[2]["cached_name"] == (
        "datavine-e-2-hash-2"
    )
    assert controller.metadata_fetches == 1

    output = factory.idata_output_file(4, 2)
    assert output.data_id == "i:4"
    assert "attempt=2" in output.declaration[1]
    assert output.declaration[2]["cached_name"] == (
        "datavine-i-4-attempt-2"
    )
    assert idata_files[4] is output

    durable = factory.durable_idata_file(
        5,
        {
            "durable_path": "/durable/five.pkl",
            "content_hash": "durable-hash",
        },
    )
    assert durable.declaration[0:2] == (
        "file",
        "/durable/five.pkl",
    )
    assert durable.data_id == "i:5"
    assert durable.content_hash == "durable-hash"

    print("DataVine Scheduler task factory contract PASS")


if __name__ == "__main__":
    main()
