"""Small logical workflow API for the independent DataVine runtime."""

import asyncio
import base64
import copy
import dataclasses
import hashlib
import threading
import time
import uuid


_WORKFLOW_SCHEMA = "datavine.workflow/v1"
_WORKFLOW_DELTA_SCHEMA = "datavine.workflow-delta/v1"
_COMMAND_EXECUTOR_VERSION = "1"
_PYTHON_SOURCE_VERSION = "source-v1"
_PYTHON_CALLABLE_VERSION = "callable-v1"
_TASKVINE_EXECUTOR_VERSION = "builtin-v1"


@dataclasses.dataclass(frozen=True, slots=True)
class DataRef:
    """A workflow-local DataID used by the language-neutral adaptor."""

    data_id: int
    codec: tuple = ("bytes", "1")

    @property
    def argv_token(self):
        return f"{{{{data:{self.data_id}}}}}"


class Workflow:
    """Thin Workflow IR v1 builder; execution remains entirely native."""

    def __init__(
        self,
        idempotency_key,
        *,
        workflow_id=None,
        streaming=False,
        maximum_tasks=100_000,
        maximum_edges=1_000_000,
        idata_backup="controller-background",
        metadata=None,
    ):
        if not str(idempotency_key):
            raise ValueError("idempotency_key must not be empty")
        self.workflow_id = None if workflow_id is None else str(workflow_id)
        self.idempotency_key = str(idempotency_key)
        self.streaming = bool(streaming)
        self.maximum_tasks = int(maximum_tasks)
        self.maximum_edges = int(maximum_edges)
        if idata_backup not in ("worker-local", "controller-background"):
            raise ValueError(
                "idata_backup must be 'worker-local' or 'controller-background'"
            )
        self.idata_backup = idata_backup
        self.metadata = dict(metadata or {})
        self._tasks = []
        self._data = []
        self._requested = set()
        self._next_task_id = 1
        self._next_data_id = 1
        self._committed_tasks = 0
        self._committed_data = 0
        self._committed_requested = set()
        self._python_functions = {}
        self._python_invocations = {}
        self._python_literal_bytes = {}
        self._python_function_objects = {}
        self._installed_objects = {}
        self._profile = {
            "python_function_serialize_nanoseconds": 0,
            "python_value_serialize_nanoseconds": 0,
            "python_invocation_serialize_nanoseconds": 0,
            "object_hash_nanoseconds": 0,
            "object_put_rpc_nanoseconds": 0,
            "object_put_requests": 0,
            "object_put_deduplicated": 0,
            "object_records": 0,
            "object_local_deduplicated": 0,
            "object_put_bytes": 0,
            "object_put_wall_nanoseconds": 0,
            "object_put_parallelism": 0,
            "inline_invocation_records": 0,
            "inline_invocation_bytes": 0,
        }

    def _data_ref(self, codec, origin, content_sha256=None):
        data_id = self._next_data_id
        self._next_data_id += 1
        record = {
            "data_id": data_id,
            "codec": {
                "name": str(codec[0]),
                "version": str(codec[1]),
            },
            "origin": origin,
        }
        if content_sha256 is not None:
            record["content_sha256"] = str(content_sha256)
        self._data.append(record)
        return DataRef(data_id, (str(codec[0]), str(codec[1])))

    def inline(self, value, *, codec=("bytes", "1"), content_sha256=None):
        payload = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        return self._data_ref(
            codec,
            {
                "kind": "inline",
                "base64": base64.b64encode(payload).decode("ascii"),
            },
            content_sha256,
        )

    def uri(self, value, *, codec=("file", "1"), content_sha256=None):
        return self._data_ref(
            codec,
            {"kind": "uri", "uri": str(value)},
            content_sha256,
        )

    def python_value(self, value):
        """Serialize one Python value in the adaptor, never in native C."""

        import cloudpickle

        started = time.perf_counter_ns()
        payload = cloudpickle.dumps(value)
        self._profile["python_value_serialize_nanoseconds"] += (
            time.perf_counter_ns() - started
        )
        return self.inline(payload, codec=("python/cloudpickle", "3"))

    def _task(
        self,
        executor,
        inputs,
        output_codecs,
        resources,
        maximum_attempts,
        *,
        priority=None,
        labels=None,
    ):
        input_refs = tuple(inputs)
        if any(not isinstance(item, DataRef) for item in input_refs):
            raise TypeError("inputs must contain DataRef values")
        codecs = tuple(output_codecs)
        if not codecs:
            raise ValueError("task requires at least one output")
        task_id = self._next_task_id
        self._next_task_id += 1
        outputs = tuple(
            self._data_ref(
                codec,
                {"kind": "output", "task_id": task_id, "output_index": index},
            )
            for index, codec in enumerate(codecs)
        )
        task = {
            "task_id": task_id,
            "executor": executor,
            "inputs": [
                {"position": index, "data_id": item.data_id}
                for index, item in enumerate(input_refs)
            ],
            "output_data_ids": [item.data_id for item in outputs],
            "retry": {"maximum_attempts": int(maximum_attempts)},
        }
        if resources:
            task["resources"] = dict(resources)
        if priority is not None:
            task["priority"] = int(priority)
        if labels:
            task["labels"] = {str(key): str(value) for key, value in labels.items()}
        self._tasks.append(task)
        return outputs[0] if len(outputs) == 1 else outputs

    def command(
        self,
        argv,
        *,
        inputs=(),
        output_codecs=(("bytes", "1"),),
        output_files=None,
        resources=None,
        maximum_attempts=1,
        priority=None,
        labels=None,
    ):
        encoded_argv = [
            item.argv_token if isinstance(item, DataRef) else str(item)
            for item in argv
        ]
        executor = {
            "kind": "command",
            "version": _COMMAND_EXECUTOR_VERSION,
            "argv": encoded_argv,
        }
        if output_files is not None:
            executor["output_files"] = [str(item) for item in output_files]
        return self._task(
            executor, inputs, output_codecs, resources, maximum_attempts,
            priority=priority, labels=labels,
        )

    def builtin(
        self,
        operation="noop",
        payload=b"",
        *,
        inputs=(),
        output_codec=("bytes", "1"),
        resources=None,
        maximum_attempts=1,
    ):
        """Register an inline noop/echo call in the shared executor."""

        operations = {"noop": 1, "echo": 2}
        if operation not in operations:
            raise ValueError("builtin operation must be noop or echo")
        if isinstance(payload, str):
            payload = payload.encode()
        payload = bytes(payload)
        if operation == "noop" and payload:
            raise ValueError("noop builtin does not accept a payload")
        if b"\0" in payload:
            raise ValueError("builtin output payload cannot contain NUL")
        encoded = self.inline(
            b"DVB1" + bytes((operations[operation],)) + payload,
            codec=("datavine/builtin", "1"),
        )
        return self._task(
            {
                "kind": "taskvine",
                "version": _TASKVINE_EXECUTOR_VERSION,
                "payload_ref": encoded.data_id,
            },
            inputs,
            (output_codec,),
            resources,
            maximum_attempts,
        )

    def python_source(
        self,
        source,
        *,
        inputs=(),
        output_codecs=(("bytes", "1"),),
        resources=None,
        maximum_attempts=1,
    ):
        """Register opaque Python source; the C runtime never interprets it."""

        payload = self.inline(str(source), codec=("python/source", "1"))
        codecs = tuple(output_codecs)
        output_files = [
            f"datavine-python-output-{index}" for index in range(len(codecs))
        ]
        executor = {
            "kind": "python",
            "version": _PYTHON_SOURCE_VERSION,
            "payload_ref": payload.data_id,
            "output_files": output_files,
        }
        return self._task(
            executor,
            inputs,
            codecs,
            resources,
            maximum_attempts,
        )

    def python_callable(
        self,
        function,
        *args,
        output_count=1,
        resources=None,
        maximum_attempts=1,
        **kwargs,
    ):
        """Package a callable as an opaque Python-executor payload.

        DataRef values may appear inside ordinary containers. Their
        bytes must use the ``python/cloudpickle`` codec. Literal Python values
        are embedded in the payload by the adaptor.
        """

        if not callable(function):
            raise TypeError("function must be callable")
        output_count = int(output_count)
        if output_count < 1:
            raise ValueError("output_count must be positive")
        import cloudpickle

        inputs = []
        seen_inputs = set()

        def encode(value):
            if isinstance(value, DataRef):
                if value.codec[0] != "python/cloudpickle":
                    raise TypeError(
                        "Python callable DataRefs require python/cloudpickle codec"
                    )
                if value.data_id not in seen_inputs:
                    seen_inputs.add(value.data_id)
                    inputs.append(value)
                return ("ref", value.data_id)
            if isinstance(value, tuple):
                return ("tuple", tuple(encode(item) for item in value))
            if isinstance(value, list):
                return ("list", tuple(encode(item) for item in value))
            if isinstance(value, dict):
                return (
                    "dict",
                    tuple(
                        (encode(key), encode(item))
                        for key, item in value.items()
                    ),
                )
            literal_key = (
                (type(value), value)
                if isinstance(value, (type(None), bool, int, str, bytes))
                else None
            )
            payload = (
                self._python_literal_bytes.get(literal_key)
                if literal_key is not None else None
            )
            if payload is not None:
                return ("pickle", payload)
            started = time.perf_counter_ns()
            payload = cloudpickle.dumps(value)
            self._profile["python_value_serialize_nanoseconds"] += (
                time.perf_counter_ns() - started
            )
            if literal_key is not None:
                self._python_literal_bytes[literal_key] = payload
            return ("pickle", payload)

        cached_function = self._python_function_objects.get(id(function))
        if cached_function is not None and cached_function[0] is function:
            _, function_bytes, function_digest, function_ref = cached_function
        else:
            # A callable is a stable workflow resource: its first use fixes the
            # cloudpickle snapshot for every task that reuses the same object.
            # Callers that need a later closure/object state must pass a new
            # callable object, which makes the different task semantics explicit.
            started = time.perf_counter_ns()
            function_bytes = cloudpickle.dumps(function)
            self._profile["python_function_serialize_nanoseconds"] += (
                time.perf_counter_ns() - started
            )
            function_digest = hashlib.sha256(function_bytes).hexdigest()
            function_ref = self._python_functions.get(function_digest)
            if function_ref is None:
                function_ref = self.inline(
                    function_bytes,
                    codec=("python/callable", "1"),
                    content_sha256=function_digest,
                )
                self._python_functions[function_digest] = function_ref
            self._python_function_objects[id(function)] = (
                function,
                function_bytes,
                function_digest,
                function_ref,
            )

        encoded_args = tuple(encode(value) for value in args)
        encoded_kwargs = tuple(
            (key, encode(value)) for key, value in kwargs.items()
        )
        output_files = tuple(
            f"datavine-python-output-{index}" for index in range(output_count)
        )
        invocation_key = (
            encoded_args, encoded_kwargs, output_count, output_files
        )
        invocation_ref = self._python_invocations.get(invocation_key)
        if invocation_ref is None:
            invocation_started = time.perf_counter_ns()
            payload = cloudpickle.dumps({
                "args": encoded_args,
                "kwargs": dict(encoded_kwargs),
                "output_count": output_count,
                "output_files": list(output_files),
            })
            self._profile["python_invocation_serialize_nanoseconds"] += (
                time.perf_counter_ns() - invocation_started
            )
            invocation_digest = hashlib.sha256(payload).hexdigest()
            invocation_ref = self.inline(
                payload,
                codec=("python/invocation", "1"),
                content_sha256=invocation_digest,
            )
            self._python_invocations[invocation_key] = invocation_ref
        return self._task(
            {
                "kind": "python",
                "version": _PYTHON_CALLABLE_VERSION,
                "payload_ref": invocation_ref.data_id,
                "function_ref": function_ref.data_id,
                "function_digest": function_digest,
                "output_files": [
                    f"datavine-python-output-{index}"
                    for index in range(output_count)
                ],
            },
            inputs,
            (("python/cloudpickle", "3"),) * output_count,
            resources,
            maximum_attempts,
        )

    def request(self, *outputs):
        for output in outputs:
            if not isinstance(output, DataRef):
                raise TypeError("requested outputs must be DataRef values")
            self._requested.add(output.data_id)
        return self

    def _document(self, *, idempotency_key=None, copy_records):
        document = {
            "schema": _WORKFLOW_SCHEMA,
            "idempotency_key": str(idempotency_key or self.idempotency_key),
            "mode": "streaming" if self.streaming else "sealed",
            "tasks": copy.deepcopy(self._tasks) if copy_records else self._tasks,
            "data": (
                copy.deepcopy(self._data)
                if copy_records else self._data
            ),
            "requested_outputs": sorted(self._requested),
            "policy": {
                "maximum_tasks": self.maximum_tasks,
                "maximum_edges": self.maximum_edges,
                "idata_backup": self.idata_backup,
            },
        }
        if self.workflow_id is not None:
            document["workflow_id"] = self.workflow_id
        if self.metadata:
            document["metadata"] = (
                copy.deepcopy(self.metadata) if copy_records else self.metadata
            )
        return document

    def document(self, *, idempotency_key=None):
        return self._document(idempotency_key=idempotency_key, copy_records=True)

    def _delta_document(
        self, task_start, data_start, requested_before, *, idempotency_key,
        copy_records,
    ):
        """Return only records created since the caller's last commit."""

        if self.workflow_id is None:
            raise ValueError("delta requires an explicit workflow_id")
        return {
            "schema": _WORKFLOW_DELTA_SCHEMA,
            "workflow_id": self.workflow_id,
            "idempotency_key": str(idempotency_key),
            "tasks": (
                copy.deepcopy(self._tasks[task_start:])
                if copy_records else self._tasks[task_start:]
            ),
            "data": (
                copy.deepcopy(self._data[data_start:])
                if copy_records else self._data[data_start:]
            ),
            "requested_outputs": sorted(self._requested - requested_before),
        }

    def delta_document(
        self, task_start, data_start, requested_before, *, idempotency_key
    ):
        return self._delta_document(
            task_start, data_start, requested_before,
            idempotency_key=idempotency_key, copy_records=True,
        )

    def _externalize(self, client, data_start):
        """Install inline data once, then leave only content identities in IR."""

        builtin_payloads = {
            task["executor"]["payload_ref"]
            for task in self._tasks
            if task["executor"]["kind"] == "taskvine"
        }
        pending = {}
        for record in self._data[data_start:]:
            origin = record["origin"]
            if origin["kind"] != "inline" or record["data_id"] in builtin_payloads:
                continue
            payload = base64.b64decode(origin["base64"], validate=True)
            codec = record.get("codec", {})
            if (
                codec.get("name") == "python/invocation"
                and codec.get("version") == "1"
                and 0 < len(payload) <= 64 * 1024
            ):
                self._profile["inline_invocation_records"] += 1
                self._profile["inline_invocation_bytes"] += len(payload)
                continue
            hash_started = time.perf_counter_ns()
            digest = hashlib.sha256(payload).hexdigest()
            self._profile["object_hash_nanoseconds"] += (
                time.perf_counter_ns() - hash_started
            )
            self._profile["object_records"] += 1
            object_key = (client.endpoint, digest)
            installed = self._installed_objects.get(object_key)
            if installed is not None:
                self._profile["object_local_deduplicated"] += 1
                self._externalize_record(record, installed)
                continue
            queued = pending.get(object_key)
            if queued is None:
                pending[object_key] = [payload, digest, [record]]
            else:
                queued[2].append(record)
                self._profile["object_local_deduplicated"] += 1

        entries = list(pending.values())
        if entries:
            parallelism = min(4, len(entries))
            started = time.perf_counter_ns()
            installed_objects = client.put_objects(
                ((payload, digest) for payload, digest, _ in entries),
                workers=parallelism,
            )
            self._profile["object_put_wall_nanoseconds"] += (
                time.perf_counter_ns() - started
            )
            self._profile["object_put_parallelism"] = max(
                self._profile["object_put_parallelism"], parallelism
            )
            for entry, installed in zip(entries, installed_objects):
                _, digest, records = entry
                object_key = (client.endpoint, digest)
                self._installed_objects[object_key] = installed
                self._profile["object_put_rpc_nanoseconds"] += installed.get(
                    "rpc_nanoseconds", 0
                )
                self._profile["object_put_requests"] += 1
                self._profile["object_put_deduplicated"] += int(
                    installed["deduplicated"]
                )
                self._profile["object_put_bytes"] += installed["size"]
                for record in records:
                    self._externalize_record(record, installed)

    @staticmethod
    def _externalize_record(record, installed):
        """Replace one inline record after its immutable object is durable."""

        existing = record.get("content_sha256")
        if existing is not None and existing != installed["sha256"]:
            raise RuntimeError(
                f"DataID {record['data_id']} content identity changed"
            )
        record["content_sha256"] = installed["sha256"]
        record["origin"] = {
            "kind": "object",
            "sha256": installed["sha256"],
        }

    def profile(self):
        """Return cumulative adaptor/data-ingest metrics for this workflow."""

        profile = dict(self._profile)
        stages = {
            "python_serialization": sum(
                profile[key] for key in (
                    "python_function_serialize_nanoseconds",
                    "python_value_serialize_nanoseconds",
                    "python_invocation_serialize_nanoseconds",
                )
            ),
            "object_hash": profile["object_hash_nanoseconds"],
            "object_put": profile["object_put_wall_nanoseconds"],
        }
        dominant, nanoseconds = max(stages.items(), key=lambda item: item[1])
        profile["dominant_ingest_stage"] = dominant
        profile["dominant_ingest_nanoseconds"] = nanoseconds
        return profile

    def _mark_committed(self):
        self._committed_tasks = len(self._tasks)
        self._committed_data = len(self._data)
        self._committed_requested = set(self._requested)

    def submit(self, client):
        self._externalize(client, 0)
        info = client.submit_workflow(self._document(copy_records=False))
        self._mark_committed()
        return info

    def append(self, client, expected_generation, *, idempotency_key):
        if self.workflow_id is None:
            raise ValueError("append requires an explicit workflow_id")
        self._externalize(client, self._committed_data)
        document = self._delta_document(
            self._committed_tasks,
            self._committed_data,
            self._committed_requested,
            idempotency_key=idempotency_key,
            copy_records=False,
        )
        info = client.append_workflow(
            self.workflow_id,
            expected_generation,
            document,
        )
        self._mark_committed()
        return info

    def seal(self, client, expected_generation):
        if self.workflow_id is None:
            raise ValueError("seal requires an explicit workflow_id")
        return client.seal_workflow(self.workflow_id, expected_generation)


class WorkflowFuture:
    """A durable remote value; waiting never owns runtime scheduling state."""

    def __init__(self, session, reference):
        self.session = session
        self.reference = reference

    @property
    def data_id(self):
        return self.reference.data_id

    @property
    def codec(self):
        return self.reference.codec

    def done(self):
        try:
            self.session.client.workflow_result_info(
                self.session.workflow_id, self.data_id
            )
            return True
        except Exception as error:
            if getattr(error, "status", None) == 5:
                return False
            raise

    def result(self, timeout=None, poll_interval=0.05):
        deadline = None if timeout is None else time.monotonic() + float(timeout)
        while True:
            try:
                payload = self.session.client.fetch_workflow_result(
                    self.session.workflow_id, self.data_id
                )
                break
            except Exception as error:
                if getattr(error, "status", None) != 5:
                    raise
            info = self.session.client.describe_workflow(self.session.workflow_id)
            if info["state"] in {"failed", "cancelled"}:
                raise RuntimeError(
                    f"workflow {self.session.workflow_id} is {info['state']}"
                )
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"DataID {self.data_id} did not complete before timeout"
                )
            time.sleep(float(poll_interval))
        if self.codec[0] == "python/cloudpickle":
            import cloudpickle

            return cloudpickle.loads(payload)
        if self.codec[0] == "text/utf-8":
            return payload.decode("utf-8")
        return payload

    async def _async_result(self):
        return await asyncio.to_thread(self.result)

    def __await__(self):
        return self._async_result().__await__()


class WorkflowSession:
    """Notebook-facing dynamic workflow session backed by the native runtime.

    Python executes ordinary control flow. Each submitted operation is one
    bounded immutable delta; a kernel interruption stops only the local wait,
    and ``attach`` reconnects to the durable remote workflow.
    """

    def __init__(self, client, builder, generation):
        self.client = client
        self.builder = builder
        self.workflow_id = builder.workflow_id
        self.generation = int(generation)
        self.builder._mark_committed()
        self._lock = threading.RLock()

    @classmethod
    def create(
        cls,
        client,
        workflow_id,
        *,
        maximum_tasks=100_000,
        maximum_edges=1_000_000,
        metadata=None,
        idempotency_key=None,
    ):
        builder = Workflow(
            idempotency_key or f"{workflow_id}-initial-{uuid.uuid4().hex}",
            workflow_id=workflow_id,
            streaming=True,
            maximum_tasks=maximum_tasks,
            maximum_edges=maximum_edges,
            metadata=metadata,
        )
        info = client.submit_workflow(builder.document())
        return cls(client, builder, info["generation"])

    @classmethod
    def attach(cls, client, workflow_id):
        info = client.describe_workflow(workflow_id)
        if not info["streaming"]:
            raise ValueError("attach requires a streaming workflow")
        if info["state"] not in {"open", "running_open", "open_quiescent"}:
            raise ValueError(
                f"attach requires an open workflow, not {info['state']}"
            )
        frontier = client.workflow_frontier(workflow_id)
        builder = Workflow(
            f"{workflow_id}-attached-local",
            workflow_id=workflow_id,
            streaming=True,
            maximum_tasks=frontier["maximum_tasks"],
            maximum_edges=frontier["maximum_edges"],
        )
        builder._next_task_id = int(frontier["maximum_task_id"]) + 1
        builder._next_data_id = int(frontier["maximum_data_id"]) + 1
        return cls(client, builder, info["generation"])

    def ref(self, data_id, codec=("bytes", "1")):
        return DataRef(int(data_id), tuple(codec))

    def profile(self):
        return {"workflow_id": self.workflow_id, **self.builder.profile()}

    def future(self, data_id, codec=("bytes", "1")):
        return WorkflowFuture(self, self.ref(data_id, codec))

    @staticmethod
    def _references(value):
        if isinstance(value, WorkflowFuture):
            return value.reference
        if isinstance(value, tuple):
            return tuple(WorkflowSession._references(item) for item in value)
        if isinstance(value, list):
            return [WorkflowSession._references(item) for item in value]
        if isinstance(value, dict):
            return {
                WorkflowSession._references(key): WorkflowSession._references(item)
                for key, item in value.items()
            }
        return value

    def _commit(self, idempotency_key=None):
        key = idempotency_key or (
            f"{self.workflow_id}-g{self.generation + 1}-{uuid.uuid4().hex}"
        )
        if len(self.builder._tasks) == self.builder._committed_tasks:
            raise ValueError("a dynamic transaction must add at least one task")
        info = self.builder.append(
            self.client, self.generation, idempotency_key=key
        )
        self.generation = int(info["generation"])
        return info

    @staticmethod
    def _futures(session, outputs):
        if isinstance(outputs, tuple):
            return tuple(WorkflowFuture(session, output) for output in outputs)
        return WorkflowFuture(session, outputs)

    def command(self, argv, *, inputs=(), idempotency_key=None, **options):
        with self._lock:
            argv = [self._references(value) for value in argv]
            inputs = tuple(self._references(value) for value in inputs)
            outputs = self.builder.command(argv, inputs=inputs, **options)
            requested = outputs if isinstance(outputs, tuple) else (outputs,)
            self.builder.request(*requested)
            self._commit(idempotency_key)
            return self._futures(self, outputs)

    def submit(
        self, function, *args, idempotency_key=None, output_count=1, **options
    ):
        with self._lock:
            args = tuple(self._references(value) for value in args)
            options = {
                key: self._references(value) for key, value in options.items()
            }
            outputs = self.builder.python_callable(
                function, *args, output_count=output_count, **options
            )
            requested = outputs if isinstance(outputs, tuple) else (outputs,)
            self.builder.request(*requested)
            self._commit(idempotency_key)
            return self._futures(self, outputs)

    def seal(self):
        with self._lock:
            info = self.client.seal_workflow(self.workflow_id, self.generation)
            self.generation = int(info["generation"])
            return info

    def cancel(self):
        return self.client.cancel_workflow(self.workflow_id)


@dataclasses.dataclass(frozen=True)
class ManagedCall:
    function: object
    args: tuple = ()
    kwargs: dict = dataclasses.field(default_factory=dict)
    output_count: int = 1


def managed_call(function, *args, output_count=1, **kwargs):
    """Describe one task yielded by a replayable managed generator."""

    return ManagedCall(function, tuple(args), dict(kwargs), int(output_count))


def _managed_checkpoint_write(path, state):
    import cloudpickle
    import os

    temporary = f"{path}.tmp-{os.getpid()}"
    with open(temporary, "wb") as stream:
        cloudpickle.dump(state, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _managed_generator_process(payload_path, checkpoint_path):
    """Child entry point. Kept public only for the subprocess bootstrap."""

    import cloudpickle
    from .workflow_client import WorkflowClient

    with open(payload_path, "rb") as stream:
        payload = cloudpickle.load(stream)
    client = WorkflowClient(payload["endpoint"], payload["token"])
    try:
        session = WorkflowSession.attach(client, payload["workflow_id"])
    except Exception as error:
        if getattr(error, "status", None) != 5:
            raise
        session = WorkflowSession.create(
            client,
            payload["workflow_id"],
            maximum_tasks=payload["maximum_tasks"],
            maximum_edges=payload["maximum_edges"],
            idempotency_key=payload["initial_key"],
        )
    try:
        with open(checkpoint_path, "rb") as stream:
            state = cloudpickle.load(stream)
    except FileNotFoundError:
        state = {"results": [], "complete": False, "final": None}
    if state["complete"]:
        return

    generator = payload["factory"]()
    try:
        call = next(generator)
        for prior in state["results"]:
            call = generator.send(prior)
    except StopIteration as done:
        state.update(complete=True, final=done.value)
        _managed_checkpoint_write(checkpoint_path, state)
        session.seal()
        return

    while True:
        if not isinstance(call, ManagedCall):
            raise TypeError("managed generator must yield managed_call values")
        info = client.describe_workflow(payload["workflow_id"])
        if info["tasks"] > len(state["results"]):
            if info["tasks"] != len(state["results"]) + 1:
                raise RuntimeError("managed workflow has an unexpected task frontier")
            last_data = int(info["data"])
            references = tuple(
                session.future(
                    last_data - call.output_count + index + 1,
                    ("python/cloudpickle", "3"),
                )
                for index in range(call.output_count)
            )
            future = references[0] if call.output_count == 1 else references
        else:
            future = session.submit(
                call.function,
                *call.args,
                output_count=call.output_count,
                idempotency_key=(
                    f"{payload['workflow_id']}-managed-{len(state['results']) + 1}"
                ),
                **call.kwargs,
            )
        result = (
            tuple(item.result() for item in future)
            if isinstance(future, tuple) else future.result()
        )
        state["results"].append(result)
        _managed_checkpoint_write(checkpoint_path, state)
        try:
            call = generator.send(result)
        except StopIteration as done:
            state.update(complete=True, final=done.value)
            _managed_checkpoint_write(checkpoint_path, state)
            session.seal()
            return


class ManagedWorkflowGenerator:
    """Supervise a replayable generator in a replaceable Python process."""

    def __init__(
        self,
        client,
        workflow_id,
        checkpoint_path,
        *,
        maximum_tasks=100_000,
        maximum_edges=1_000_000,
        maximum_restarts=3,
    ):
        self.client = client
        self.workflow_id = str(workflow_id)
        self.checkpoint_path = str(checkpoint_path)
        self.maximum_tasks = int(maximum_tasks)
        self.maximum_edges = int(maximum_edges)
        self.maximum_restarts = int(maximum_restarts)

    def run(self, generator_factory):
        import cloudpickle
        import os
        import subprocess
        import sys
        import tempfile

        payload = {
            "endpoint": self.client.endpoint,
            "token": self.client.token.decode("utf-8"),
            "workflow_id": self.workflow_id,
            "maximum_tasks": self.maximum_tasks,
            "maximum_edges": self.maximum_edges,
            "initial_key": f"{self.workflow_id}-managed-initial",
            "factory": generator_factory,
        }
        descriptor, payload_path = tempfile.mkstemp(prefix="datavine-generator-")
        try:
            with os.fdopen(descriptor, "wb") as stream:
                cloudpickle.dump(payload, stream)
            command = (
                sys.executable,
                "-c",
                "from ndcctools.taskvine.datavine.workflow import "
                "_managed_generator_process as run; import sys; run(sys.argv[1],sys.argv[2])",
                payload_path,
                self.checkpoint_path,
            )
            for attempt in range(self.maximum_restarts + 1):
                completed = subprocess.run(command)
                if completed.returncode == 0:
                    with open(self.checkpoint_path, "rb") as stream:
                        return cloudpickle.load(stream)["final"]
                if attempt == self.maximum_restarts:
                    raise RuntimeError(
                        f"managed generator failed after {attempt} restarts"
                    )
        finally:
            try:
                os.unlink(payload_path)
            except FileNotFoundError:
                pass
