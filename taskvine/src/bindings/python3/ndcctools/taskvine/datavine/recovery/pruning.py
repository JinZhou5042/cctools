"""Recovery-frontier pruning with bounded incremental state."""

import collections
import dataclasses


TASK_STATES = frozenset(("pending", "running", "completed", "cancelled"))
ACTIVE_TASK_STATES = frozenset(("pending", "running"))
PERSISTENCE_STATES = frozenset(("none", "queued", "writing"))


@dataclasses.dataclass(frozen=True)
class DataState:
    available: bool = False
    durable: bool = False
    pinned: bool = False
    required_output: bool = False
    persistence: str = "none"

    def validate(self):
        if self.persistence not in PERSISTENCE_STATES:
            raise ValueError(f"invalid persistence state {self.persistence!r}")
        if self.durable and not self.available:
            raise ValueError("durable data must be available")
        return self


@dataclasses.dataclass(frozen=True)
class PruningRecord:
    data_id: int
    decision: str
    reasons: tuple
    recovery_targets: tuple
    graph_revision: int
    state_revision: int

    def to_dict(self):
        return {
            "data_id": self.data_id,
            "decision": self.decision,
            "reasons": list(self.reasons),
            "recovery_targets": list(self.recovery_targets),
            "graph_revision": self.graph_revision,
            "state_revision": self.state_revision,
        }


@dataclasses.dataclass(frozen=True)
class PruningPlan:
    prunable: tuple
    cancel_persistence: tuple
    protected: tuple
    recovery_depths: tuple
    records: tuple
    nodes_examined: int

    def semantic(self):
        return {
            "prunable": self.prunable,
            "cancel_persistence": self.cancel_persistence,
            "protected": self.protected,
            "recovery_depths": self.recovery_depths,
            "records": tuple(
                (r.data_id, r.decision, r.reasons, r.recovery_targets)
                for r in self.records
            ),
        }

    def to_dict(self):
        return {
            "prunable": list(self.prunable),
            "cancel_persistence": list(self.cancel_persistence),
            "protected": list(self.protected),
            "recovery_depths": dict(self.recovery_depths),
            "records": [record.to_dict() for record in self.records],
            "nodes_examined": self.nodes_examined,
        }


@dataclasses.dataclass(frozen=True)
class PruningMutation:
    graph_revision: int
    state_revision: int
    touched_records: int
    changed: bool

    def to_dict(self):
        return dataclasses.asdict(self)


class LineageGraph:
    """Append-only, topologically ordered producer graph."""

    def __init__(self):
        self.inputs_by_task = {}
        self.outputs_by_task = {}
        self.producer_by_data = {}
        self.consumers_by_data = {}
        self.revision = 0

    @property
    def task_ids(self):
        return tuple(sorted(self.inputs_by_task))

    @property
    def data_ids(self):
        return tuple(sorted(self.producer_by_data))

    def add_task(self, task_id, inputs, outputs):
        task_id = int(task_id)
        inputs = tuple(dict.fromkeys(map(int, inputs)))
        outputs = tuple(dict.fromkeys(map(int, outputs)))
        if task_id in self.inputs_by_task:
            raise ValueError(f"duplicate TaskID {task_id}")
        if not outputs:
            raise ValueError("a task must have at least one output")
        unknown = {
            data_id for data_id in inputs
            if data_id not in self.producer_by_data
        }
        if unknown:
            raise KeyError(f"TaskID {task_id} has unknown input IDataIDs {sorted(unknown)}")
        duplicate = {
            data_id for data_id in outputs
            if data_id in self.producer_by_data
        }
        if duplicate:
            raise ValueError(f"duplicate output IDataIDs {sorted(duplicate)}")
        self.inputs_by_task[task_id] = inputs
        self.outputs_by_task[task_id] = outputs
        for data_id in inputs:
            self.consumers_by_data[data_id].add(task_id)
        for data_id in outputs:
            self.producer_by_data[data_id] = task_id
            self.consumers_by_data[data_id] = set()
        self.revision += 1

    def producer_inputs(self, data_id):
        return self.inputs_by_task[self.producer_by_data[int(data_id)]]

    def validate(self):
        seen = set()
        for task_id in self.task_ids:
            if not set(self.inputs_by_task[task_id]) <= seen:
                raise ValueError("lineage graph is not topologically ordered")
            outputs = set(self.outputs_by_task[task_id])
            if outputs & seen:
                raise ValueError("IDataID has multiple producers")
            seen.update(outputs)
        if seen != set(self.producer_by_data):
            raise ValueError("data index mismatch")
        return True


def _record(graph, revision, data_id, state, direct, anchor_refs):
    reasons = set(direct.get(data_id, ()))
    if state.pinned:
        reasons.add("pinned")
    if anchor_refs.get(data_id, 0):
        reasons.add("recovery-anchor")
    if state.persistence == "writing":
        reasons.add("persistence-writing")
    if not state.available:
        decision = "absent"
        reasons.add("no-accepted-replica")
    elif reasons:
        decision = "keep"
        if state.persistence == "queued":
            reasons.add("persistence-queued")
    elif state.persistence == "queued":
        decision = "cancel-persistence"
        reasons.add("obsolete-persistence")
    else:
        decision = "prune"
        reasons.update(("lineage-reproducible", "no-live-consumer"))
    return PruningRecord(
        data_id,
        decision,
        tuple(sorted(reasons)),
        (),
        graph.revision,
        revision,
    )


def _plan(records, examined, depths=()):
    records = tuple(records)
    return PruningPlan(
        tuple(r.data_id for r in records if r.decision == "prune"),
        tuple(r.data_id for r in records if r.decision == "cancel-persistence"),
        tuple(r.data_id for r in records if r.decision == "keep"),
        tuple(sorted(depths)),
        records,
        examined,
    )


class IncrementalPruner:
    """Maintain the durable recovery boundary by reference propagation.

    Each live consumer contributes demand to its inputs. Demand propagates
    backward only while a datum is not durable. A zero-to-one or one-to-zero
    transition is the only event that traverses another edge, so ordinary DAG
    execution is linear in graph size and uses no per-target ancestor sets.
    """

    def __init__(self, graph):
        graph.validate()
        self.graph = graph
        self.task_states = {task_id: "pending" for task_id in graph.task_ids}
        self.data_states = {data_id: DataState() for data_id in graph.data_ids}
        self.state_revision = 0
        self._obligations = {}
        self._direct = {data_id: set() for data_id in graph.data_ids}
        self._demand = collections.Counter()
        self._anchor_refs = collections.Counter()
        self._records = {}
        self._last_examined = 0
        touched = set()
        for task_id in graph.task_ids:
            touched.update(self._add_task_obligations(task_id))
        self._refresh(graph.data_ids)
        self._last_examined = len(touched)

    def _propagate(self, data_id, delta):
        touched = set()
        stack = [(int(data_id), int(delta))]
        while stack:
            current, change = stack.pop()
            old = self._demand[current]
            new = old + change
            if new < 0:
                raise RuntimeError(f"negative recovery demand for IDataID {current}")
            if new:
                self._demand[current] = new
            else:
                self._demand.pop(current, None)
            touched.add(current)
            if old == 0 and new > 0:
                if self.data_states[current].durable:
                    self._anchor_refs[current] = 1
                else:
                    stack.extend((parent, 1) for parent in self.graph.producer_inputs(current))
            elif old > 0 and new == 0:
                if self.data_states[current].durable:
                    self._anchor_refs.pop(current, None)
                else:
                    stack.extend((parent, -1) for parent in self.graph.producer_inputs(current))
        return touched

    def _add_obligation(self, key, data_id, reason):
        if key in self._obligations:
            return set()
        self._obligations[key] = data_id
        self._direct[data_id].add(reason)
        return self._propagate(data_id, 1)

    def _remove_obligation(self, key, reason):
        data_id = self._obligations.pop(key)
        self._direct[data_id].discard(reason)
        return self._propagate(data_id, -1)

    def _add_task_obligations(self, task_id):
        touched = set()
        if self.task_states[task_id] in ACTIVE_TASK_STATES:
            for data_id in self.graph.inputs_by_task[task_id]:
                touched.update(self._add_obligation(
                    ("task", task_id, data_id), data_id,
                    f"active-consumer:T{task_id}",
                ))
        return touched

    def _remove_task_obligations(self, task_id):
        touched = set()
        for data_id in self.graph.inputs_by_task[task_id]:
            key = ("task", task_id, data_id)
            if key in self._obligations:
                touched.update(self._remove_obligation(key, f"active-consumer:T{task_id}"))
        return touched

    def _refresh(self, data_ids):
        for data_id in data_ids:
            self._records[data_id] = _record(
                self.graph, self.state_revision, data_id,
                self.data_states[data_id], self._direct, self._anchor_refs,
            )

    def _mutation(self, touched, changed=True):
        self._last_examined = len(touched)
        if changed:
            self.state_revision += 1
            self._refresh(touched)
        return PruningMutation(
            self.graph.revision, self.state_revision, len(touched), changed
        )

    @staticmethod
    def _check_transition(old, new):
        allowed = {
            "pending": {"running", "completed", "cancelled"},
            "running": {"pending", "completed", "cancelled"},
            "completed": {"pending"},
            "cancelled": set(),
        }
        if new not in TASK_STATES:
            raise ValueError(f"invalid task state {new!r}")
        if old != new and new not in allowed[old]:
            raise ValueError(f"invalid task transition {old}->{new}")

    def set_task_state(self, task_id, state):
        task_id = int(task_id)
        old = self.task_states[task_id]
        self._check_transition(old, state)
        if old == state:
            return self._mutation(set(), False)
        touched = self._remove_task_obligations(task_id)
        self.task_states[task_id] = state
        touched.update(self._add_task_obligations(task_id))
        return self._mutation(touched)

    def set_task_states(self, task_ids, state):
        task_ids = tuple(dict.fromkeys(map(int, task_ids)))
        changed = []
        for task_id in task_ids:
            old = self.task_states[task_id]
            self._check_transition(old, state)
            if old != state:
                changed.append(task_id)
        if not changed:
            mutation = self._mutation(set(), False)
            return tuple(mutation for _ in task_ids)
        touched = set()
        for task_id in changed:
            touched.update(self._remove_task_obligations(task_id))
            self.task_states[task_id] = state
            touched.update(self._add_task_obligations(task_id))
        mutation = self._mutation(touched)
        unchanged = PruningMutation(self.graph.revision, self.state_revision, 0, False)
        changed = set(changed)
        return tuple(mutation if task_id in changed else unchanged for task_id in task_ids)

    def _set_data(self, data_id, new):
        old = self.data_states[data_id]
        touched = {data_id}
        demanded = self._demand[data_id] > 0
        if demanded and old.durable != new.durable:
            if new.durable:
                for parent in self.graph.producer_inputs(data_id):
                    touched.update(self._propagate(parent, -1))
                self._anchor_refs[data_id] = 1
            else:
                self._anchor_refs.pop(data_id, None)
                for parent in self.graph.producer_inputs(data_id):
                    touched.update(self._propagate(parent, 1))
        self.data_states[data_id] = new
        if old.required_output != new.required_output:
            key = ("output", data_id, data_id)
            if new.required_output:
                touched.update(self._add_obligation(key, data_id, "required-output"))
            else:
                touched.update(self._remove_obligation(key, "required-output"))
        return touched

    def set_data_state(self, data_id, **changes):
        data_id = int(data_id)
        new = dataclasses.replace(self.data_states[data_id], **changes).validate()
        if new == self.data_states[data_id]:
            return self._mutation(set(), False)
        return self._mutation(self._set_data(data_id, new))

    def set_data_states(self, updates):
        normalized = {}
        for data_id, changes in updates:
            data_id = int(data_id)
            new = dataclasses.replace(self.data_states[data_id], **dict(changes)).validate()
            if data_id in normalized and normalized[data_id] != new:
                raise ValueError(f"conflicting data state updates for {data_id}")
            normalized[data_id] = new
        changed = {k: v for k, v in normalized.items() if self.data_states[k] != v}
        if not changed:
            return self._mutation(set(), False)
        touched = set()
        for data_id, new in changed.items():
            touched.update(self._set_data(data_id, new))
        return self._mutation(touched)

    def add_task(self, task_id, inputs, outputs):
        return self.add_tasks(((task_id, inputs, outputs),))

    def add_tasks(self, tasks):
        tasks = tuple(tasks)
        if not tasks:
            return self._mutation(set(), False)
        touched = set()
        for task_id, inputs, outputs in tasks:
            outputs = tuple(outputs)
            self.graph.add_task(task_id, inputs, outputs)
            task_id = int(task_id)
            self.task_states[task_id] = "pending"
            for data_id in map(int, outputs):
                self.data_states[data_id] = DataState()
                self._direct[data_id] = set()
                touched.add(data_id)
            touched.update(self._add_task_obligations(task_id))
        return self._mutation(touched)

    def plan(self):
        records = tuple(
            dataclasses.replace(
                self._records[data_id],
                graph_revision=self.graph.revision,
                state_revision=self.state_revision,
            )
            for data_id in self.graph.data_ids
        )
        return _plan(records, self._last_examined)


def reference_pruning_plan(graph, task_states, data_states, state_revision=0):
    """Independent iterative oracle for tests, never used by production."""

    graph.validate()
    direct = {data_id: set() for data_id in graph.data_ids}
    demanded = set()
    anchors = collections.Counter()
    stack = []
    for task_id in graph.task_ids:
        if task_states[task_id] in ACTIVE_TASK_STATES:
            for data_id in graph.inputs_by_task[task_id]:
                direct[data_id].add(f"active-consumer:T{task_id}")
                stack.append(data_id)
    for data_id in graph.data_ids:
        if data_states[data_id].required_output:
            direct[data_id].add("required-output")
            stack.append(data_id)
    examined = 0
    while stack:
        data_id = stack.pop()
        if data_id in demanded:
            continue
        demanded.add(data_id)
        examined += 1
        if data_states[data_id].durable:
            anchors[data_id] = 1
        else:
            stack.extend(graph.producer_inputs(data_id))
    records = (
        _record(graph, state_revision, data_id, data_states[data_id], direct, anchors)
        for data_id in graph.data_ids
    )
    return _plan(records, examined)
