#!/usr/bin/env python3

import copy
import json
import os
from pathlib import Path
import subprocess

from ndcctools.taskvine.datavine.workflow import Workflow


TEST_DIR = Path(__file__).resolve().parent
REPOSITORY = TEST_DIR.parent.parent
def native_validate(document):
    executable = REPOSITORY / "taskvine/src/tools/datavine_workflow"
    result = subprocess.run(
        (str(executable), "validate", "-"),
        input=json.dumps(document, ensure_ascii=False).encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        env={**os.environ, "LC_ALL": "C"},
    )
    response = json.loads(result.stdout)
    assert result.returncode == (0 if response["valid"] else 1), (
        result.returncode,
        response,
        result.stderr.decode(),
    )
    return response


def main():
    fixtures = json.loads(
        (TEST_DIR / "datavine_workflow_ir_fixtures.json").read_text()
    )
    for fixture in fixtures["valid"]:
        native = native_validate(fixture["document"])
        assert native["valid"] is True, (fixture["name"], native)
        assert native["digest"] == fixture["digest"], (
            fixture["name"],
            native["digest"],
            fixture["digest"],
        )

    for fixture in fixtures["invalid"]:
        native = native_validate(fixture["document"])
        assert native["valid"] is False, (fixture["name"], native)
        for key in ("error", "path"):
            assert native[key] == fixture[key], (
                fixture["name"],
                key,
                native[key],
                fixture[key],
            )

    callable_workflow = Workflow(
        "callable-validator-v1",
        workflow_id="callable-validator",
        maximum_tasks=1,
        maximum_edges=0,
    )
    output = callable_workflow.python_callable(lambda: 1)
    callable_workflow.request(output)
    callable_document = callable_workflow.document()
    assert native_validate(callable_document)["valid"] is True

    invalid_digest = copy.deepcopy(callable_document)
    invalid_digest["tasks"][0]["executor"]["function_digest"] = "A" * 64
    digest_response = native_validate(invalid_digest)
    assert digest_response["valid"] is False
    assert digest_response["error"] == "value"

    invalid_reference = copy.deepcopy(callable_document)
    invalid_reference["tasks"][0]["executor"]["function_ref"] = 999
    reference_response = native_validate(invalid_reference)
    assert reference_response["valid"] is False
    assert reference_response["error"] == "reference"

    print(
        "DataVine Workflow IR v1 native golden-fixture PASS "
        f"({len(fixtures['valid'])} valid, "
        f"{len(fixtures['invalid'])} invalid, callable-register=1)"
    )


if __name__ == "__main__":
    main()
