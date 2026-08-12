#!/usr/bin/env python3
"""Focused contract for worker-local output retention and durability policy."""

import cloudpickle
import importlib.machinery
from pathlib import Path
import tempfile


def main():
    repository = Path(__file__).resolve().parents[2]
    executor_path = repository / "taskvine/src/tools/datavine_python_executor"
    module = importlib.machinery.SourceFileLoader(
        "datavine_python_executor_policy_test", str(executor_path)
    ).load_module()

    def split(value):
        return value, value + 1, value + 2

    invocation = cloudpickle.dumps({
        "args": [("pickle", cloudpickle.dumps(40))],
        "kwargs": {},
        "output_count": 3,
        "output_files": [
            "datavine-python-output-0",
            "datavine-python-output-1",
            "datavine-python-output-2",
        ],
    })
    calls = []
    original_fsync = module.os.fsync

    with tempfile.TemporaryDirectory(prefix="datavine-output-policy-") as root:
        previous = module.os.getcwd()
        module.os.chdir(root)
        try:
            module.os.fsync = lambda fd: (calls.append(fd), original_fsync(fd))[1]
            module.callable_main(
                Path(root), invocation, cloudpickle.dumps(split),
                bytes((1, 0, 1)), bytes((0, 0, 1)),
            )
        finally:
            module.os.fsync = original_fsync
            module.os.chdir(previous)

        root_path = Path(root)
        assert cloudpickle.load(
            (root_path / "datavine-python-output-0").open("rb")
        ) == 40
        assert not (root_path / "datavine-python-output-1").exists()
        assert cloudpickle.load(
            (root_path / "datavine-python-output-2").open("rb")
        ) == 42
        manifest = (root_path / module.MANIFEST_NAME).read_text().splitlines()
        assert manifest[0:2] == ["DVM1", "3"]
        assert manifest[3] == f"0 {'0' * 64}"
        assert len(calls) == 1, calls

    print(
        "DataVine Python output policy PASS retained=2 skipped=1 "
        "remote-fsync=0 durable-fsync=1 manifest-fsync=0"
    )


if __name__ == "__main__":
    main()
