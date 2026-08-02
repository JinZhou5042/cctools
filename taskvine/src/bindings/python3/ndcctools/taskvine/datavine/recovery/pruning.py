"""Recovery-aware shadow pruning with reference and incremental evaluators."""

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
            raise ValueError(
                f"invalid persistence state {self.persistence!r}"
            )
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
                (
                    record.data_id,
                    record.decision,
                    record.reasons,
                    record.recovery_targets,
                )
                for record in self.records
            ),
        }

    def to_dict(self):
        return {
            "prunable": list(self.prunable),
            "cancel_persistence": list(self.cancel_persistence),
            "protected": list(self.protected),
            "recovery_depths": {
                str(data_id): depth
                for data_id, depth in self.recovery_depths
            },
            "records": [record.to_dict() for record in self.records],
            "nodes_examined": self.nodes_examined,
        }


@dataclasses.dataclass(frozen=True)
class PruningMutation:
    """Bounded acknowledgement for an incremental pruning-state mutation."""

    graph_revision: int
    state_revision: int
    touched_records: int
    changed: bool

    def to_dict(self):
        return dataclasses.asdict(self)


class LineageGraph:
    """Append-only logical producer/consumer graph for pruning proofs."""

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
        inputs = tuple(dict.fromkeys(int(value) for value in inputs))
        outputs = tuple(dict.fromkeys(int(value) for value in outputs))
        if task_id in self.inputs_by_task:
            raise ValueError(f"duplicate TaskID {task_id}")
        if not outputs:
            raise ValueError("a task must have at least one output")
        unknown = {
            data_id
            for data_id in inputs
            if data_id not in self.producer_by_data
        }
        if unknown:
            raise KeyError(
                f"TaskID {task_id} has unknown input IDataIDs "
                f"{sorted(unknown)}"
            )
        duplicate = {
            data_id
            for data_id in outputs
            if data_id in self.producer_by_data
        }
        if duplicate:
            raise ValueError(
                f"duplicate output IDataIDs {sorted(duplicate)}"
            )
        self.inputs_by_task[task_id] = inputs
        self.outputs_by_task[task_id] = outputs
        for data_id in inputs:
            self.consumers_by_data[data_id].add(task_id)
        for data_id in outputs:
            self.producer_by_data[data_id] = task_id
            self.consumers_by_data[data_id] = set()
        self.revision += 1

    def producer_inputs(self, data_id):
        producer = self.producer_by_data[int(data_id)]
        return self.inputs_by_task[producer]

    def validate(self):
        seen_tasks = set()
        seen_data = set()
        for task_id in self.task_ids:
            inputs = self.inputs_by_task[task_id]
            if not set(inputs) <= seen_data:
                raise ValueError("lineage graph is not topologically ordered")
            outputs = self.outputs_by_task[task_id]
            if seen_data & set(outputs):
                raise ValueError("IDataID has multiple producers")
            seen_tasks.add(task_id)
            seen_data.update(outputs)
        if seen_tasks != set(self.outputs_by_task):
            raise ValueError("task index mismatch")
        if seen_data != set(self.producer_by_data):
            raise ValueError("data index mismatch")
        return True


def _active_obligations(graph, task_states, data_states):
    obligations = {}
    direct_sources = {data_id: set() for data_id in graph.data_ids}
    for task_id in graph.task_ids:
        if task_states[task_id] not in ACTIVE_TASK_STATES:
            continue
        for data_id in graph.inputs_by_task[task_id]:
            key = ("task", task_id, data_id)
            obligations[key] = data_id
            direct_sources[data_id].add(f"active-consumer:T{task_id}")
    for data_id in graph.data_ids:
        state = data_states[data_id]
        if state.required_output:
            key = ("output", data_id, data_id)
            obligations[key] = data_id
            direct_sources[data_id].add("required-output")
    return obligations, direct_sources


def _recovery_walk(graph, data_states, target_data_id):
    anchors = set()
    visited = set()
    depths = {}

    def visit(data_id):
        if data_id in depths:
            return depths[data_id]
        visited.add(data_id)
        state = data_states[data_id]
        if state.available and state.durable:
            anchors.add(data_id)
            depth = 0
        else:
            parents = graph.producer_inputs(data_id)
            depth = 1 + max(
                (visit(parent_id) for parent_id in parents),
                default=0,
            )
        depths[data_id] = depth
        return depth

    depth = visit(target_data_id)
    return anchors, visited, depth


def _make_record(
    graph,
    state_revision,
    data_id,
    state,
    direct_sources,
    recovery_sources,
):
    reasons = set(direct_sources.get(data_id, ()))
    targets = tuple(sorted(recovery_sources.get(data_id, ())))
    if state.pinned:
        reasons.add("pinned")
    if targets:
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
        reasons.update(
            ("lineage-reproducible", "no-live-consumer")
        )
    return PruningRecord(
        data_id=data_id,
        decision=decision,
        reasons=tuple(sorted(reasons)),
        recovery_targets=targets,
        graph_revision=graph.revision,
        state_revision=state_revision,
    )


def reference_pruning_plan(graph, task_states, data_states, state_revision=0):
    """Full-scan oracle used to validate the incremental implementation."""

    graph.validate()
    obligations, direct_sources = _active_obligations(
        graph, task_states, data_states
    )
    recovery_sources = {data_id: set() for data_id in graph.data_ids}
    recovery_depths = {}
    nodes_examined = 0
    for target_data_id in obligations.values():
        anchors, visited, depth = _recovery_walk(
            graph, data_states, target_data_id
        )
        recovery_depths[target_data_id] = depth
        nodes_examined += len(visited)
        for anchor_id in anchors:
            recovery_sources[anchor_id].add(target_data_id)
    records = tuple(
        _make_record(
            graph,
            state_revision,
            data_id,
            data_states[data_id],
            direct_sources,
            recovery_sources,
        )
        for data_id in graph.data_ids
    )
    return _plan_from_records(
        records, nodes_examined, recovery_depths.items()
    )


def _plan_from_records(records, nodes_examined, recovery_depths=()):
    return PruningPlan(
        prunable=tuple(
            record.data_id
            for record in records
            if record.decision == "prune"
        ),
        cancel_persistence=tuple(
            record.data_id
            for record in records
            if record.decision == "cancel-persistence"
        ),
        protected=tuple(
            record.data_id
            for record in records
            if record.decision == "keep"
        ),
        recovery_depths=tuple(sorted(recovery_depths)),
        records=tuple(records),
        nodes_examined=nodes_examined,
    )


class IncrementalPruner:
    """Event-indexed shadow evaluator with oracle-comparable decisions."""

    def __init__(self, graph):
        graph.validate()
        self.graph = graph
        self.task_states = {
            task_id: "pending" for task_id in graph.task_ids
        }
        self.data_states = {
            data_id: DataState() for data_id in graph.data_ids
        }
        self.state_revision = 0
        self._obligations = {}
        self._target_refcounts = collections.Counter()
        self._target_anchors = {}
        self._target_visited = {}
        self._target_depths = {}
        self._ancestor_targets = {
            data_id: set() for data_id in graph.data_ids
        }
        self._direct_sources = {
            data_id: set() for data_id in graph.data_ids
        }
        self._recovery_sources = {
            data_id: collections.Counter() for data_id in graph.data_ids
        }
        self._records = {}
        self._last_nodes_examined = 0
        self._begin_event()
        for task_id in graph.task_ids:
            self._add_task_obligations(task_id)
        self._end_event()
        self._refresh(graph.data_ids)

    def _begin_event(self):
        self._event_memo = {}
        self._event_examined = set()

    def _end_event(self):
        self._last_nodes_examined = len(self._event_examined)

    def _calculate(self, target_data_id):
        def visit(data_id):
            if data_id in self._event_memo:
                return self._event_memo[data_id]
            self._event_examined.add(data_id)
            state = self.data_states[data_id]
            if state.available and state.durable:
                result = ({data_id}, {data_id}, 0)
            else:
                anchors = set()
                visited = {data_id}
                parent_depths = []
                for parent_id in self.graph.producer_inputs(data_id):
                    (
                        parent_anchors,
                        parent_visited,
                        parent_depth,
                    ) = visit(parent_id)
                    anchors.update(parent_anchors)
                    visited.update(parent_visited)
                    parent_depths.append(parent_depth)
                result = (
                    anchors,
                    visited,
                    1 + max(parent_depths, default=0),
                )
            self._event_memo[data_id] = result
            return result

        anchors, visited, depth = visit(target_data_id)
        return set(anchors), set(visited), depth

    def _add_target(self, data_id):
        anchors, visited, depth = self._calculate(data_id)
        self._target_anchors[data_id] = anchors
        self._target_visited[data_id] = visited
        self._target_depths[data_id] = depth
        for visited_id in visited:
            self._ancestor_targets[visited_id].add(data_id)
        for anchor_id in anchors:
            self._recovery_sources[anchor_id][data_id] += 1
        return {data_id, *anchors}

    def _remove_target(self, data_id):
        anchors = self._target_anchors.pop(data_id)
        visited = self._target_visited.pop(data_id)
        self._target_depths.pop(data_id)
        for visited_id in visited:
            self._ancestor_targets[visited_id].discard(data_id)
        for anchor_id in anchors:
            del self._recovery_sources[anchor_id][data_id]
        return {data_id, *anchors}

    def _recalculate_target(self, data_id):
        touched = self._remove_target(data_id)
        touched.update(self._add_target(data_id))
        return touched

    def _add_obligation(self, key, data_id, direct_reason):
        if key in self._obligations:
            return set()
        self._obligations[key] = data_id
        self._direct_sources[data_id].add(direct_reason)
        self._target_refcounts[data_id] += 1
        if self._target_refcounts[data_id] == 1:
            return self._add_target(data_id)
        return {data_id}

    def _remove_obligation(self, key, direct_reason):
        data_id = self._obligations.pop(key)
        self._direct_sources[data_id].discard(direct_reason)
        self._target_refcounts[data_id] -= 1
        if self._target_refcounts[data_id] == 0:
            del self._target_refcounts[data_id]
            return self._remove_target(data_id)
        return {data_id}

    def _add_task_obligations(self, task_id):
        touched = set()
        if self.task_states[task_id] not in ACTIVE_TASK_STATES:
            return touched
        for data_id in self.graph.inputs_by_task[task_id]:
            key = ("task", task_id, data_id)
            touched.update(
                self._add_obligation(
                    key, data_id, f"active-consumer:T{task_id}"
                )
            )
        return touched

    def _remove_task_obligations(self, task_id):
        touched = set()
        for data_id in self.graph.inputs_by_task[task_id]:
            key = ("task", task_id, data_id)
            if key in self._obligations:
                touched.update(
                    self._remove_obligation(
                        key, f"active-consumer:T{task_id}"
                    )
                )
        return touched

    def _refresh(self, data_ids):
        for data_id in data_ids:
            self._records[data_id] = _make_record(
                self.graph,
                self.state_revision,
                data_id,
                self.data_states[data_id],
                self._direct_sources,
                self._recovery_sources,
            )

    def set_task_state(self, task_id, state):
        task_id = int(task_id)
        if state not in TASK_STATES:
            raise ValueError(f"invalid task state {state!r}")
        old = self.task_states[task_id]
        if old == state:
            return PruningMutation(
                self.graph.revision, self.state_revision, 0, False
            )
        allowed = {
            "pending": {"running", "completed", "cancelled"},
            "running": {"pending", "completed", "cancelled"},
            "completed": {"pending"},
            "cancelled": set(),
        }
        if state not in allowed[old]:
            raise ValueError(f"invalid task transition {old}->{state}")
        self._begin_event()
        touched = set()
        if old in ACTIVE_TASK_STATES:
            touched.update(self._remove_task_obligations(task_id))
        self.task_states[task_id] = state
        if state in ACTIVE_TASK_STATES:
            touched.update(self._add_task_obligations(task_id))
        self._end_event()
        self.state_revision += 1
        self._refresh(touched)
        return PruningMutation(
            self.graph.revision,
            self.state_revision,
            len(touched),
            True,
        )

    def add_tasks(self, tasks):
        tasks = tuple(tasks)
        if not tasks:
            return PruningMutation(
                self.graph.revision, self.state_revision, 0, False
            )
        self._begin_event()
        touched = set()
        for task_id, inputs, outputs in tasks:
            outputs = tuple(outputs)
            self.graph.add_task(task_id, inputs, outputs)
            self.task_states[int(task_id)] = "pending"
            for data_id in outputs:
                data_id = int(data_id)
                self.data_states[data_id] = DataState()
                self._ancestor_targets[data_id] = set()
                self._direct_sources[data_id] = set()
                self._recovery_sources[data_id] = collections.Counter()
            touched.update(int(value) for value in outputs)
            touched.update(self._add_task_obligations(int(task_id)))
        self._end_event()
        self.state_revision += 1
        self._refresh(touched)
        return PruningMutation(
            self.graph.revision,
            self.state_revision,
            len(touched),
            True,
        )

    def set_task_states(self, task_ids, state):
        task_ids = tuple(dict.fromkeys(int(value) for value in task_ids))
        if state not in TASK_STATES:
            raise ValueError(f"invalid task state {state!r}")
        allowed = {
            "pending": {"running", "completed", "cancelled"},
            "running": {"pending", "completed", "cancelled"},
            "completed": {"pending"},
            "cancelled": set(),
        }
        changed = []
        for task_id in task_ids:
            old = self.task_states[task_id]
            if old == state:
                continue
            if state not in allowed[old]:
                raise ValueError(f"invalid task transition {old}->{state}")
            changed.append(task_id)
        if not changed:
            mutation = PruningMutation(
                self.graph.revision, self.state_revision, 0, False
            )
            return tuple(mutation for _ in task_ids)
        self._begin_event()
        touched = set()
        for task_id in changed:
            old = self.task_states[task_id]
            if old in ACTIVE_TASK_STATES:
                touched.update(self._remove_task_obligations(task_id))
            self.task_states[task_id] = state
            if state in ACTIVE_TASK_STATES:
                touched.update(self._add_task_obligations(task_id))
        self._end_event()
        self.state_revision += 1
        self._refresh(touched)
        mutation = PruningMutation(
            self.graph.revision,
            self.state_revision,
            len(touched),
            True,
        )
        unchanged = PruningMutation(
            self.graph.revision,
            self.state_revision,
            0,
            False,
        )
        changed = set(changed)
        return tuple(
            mutation if task_id in changed else unchanged
            for task_id in task_ids
        )

    def set_data_state(self, data_id, **changes):
        data_id = int(data_id)
        old = self.data_states[data_id]
        new = dataclasses.replace(old, **changes).validate()
        if old == new:
            return PruningMutation(
                self.graph.revision, self.state_revision, 0, False
            )
        self._begin_event()
        affected = set(self._ancestor_targets[data_id])
        self.data_states[data_id] = new
        touched = {data_id}
        for target_id in tuple(affected):
            touched.update(self._recalculate_target(target_id))
        if old.required_output != new.required_output:
            key = ("output", data_id, data_id)
            if new.required_output:
                touched.update(
                    self._add_obligation(
                        key, data_id, "required-output"
                    )
                )
            else:
                touched.update(
                    self._remove_obligation(key, "required-output")
                )
        self._end_event()
        self.state_revision += 1
        self._refresh(touched)
        return PruningMutation(
            self.graph.revision,
            self.state_revision,
            len(touched),
            True,
        )

    def set_data_states(self, updates):
        normalized = {}
        for data_id, changes in updates:
            data_id = int(data_id)
            changes = dict(changes)
            old = self.data_states[data_id]
            new = dataclasses.replace(old, **changes).validate()
            previous = normalized.get(data_id)
            if previous is not None and previous != new:
                raise ValueError(
                    f"conflicting data state updates for {data_id}"
                )
            normalized[data_id] = new
        changed = {
            data_id: (self.data_states[data_id], new)
            for data_id, new in normalized.items()
            if self.data_states[data_id] != new
        }
        if not changed:
            return PruningMutation(
                self.graph.revision, self.state_revision, 0, False
            )

        self._begin_event()
        affected_targets = set()
        touched = set(changed)
        for data_id, (_, new) in changed.items():
            affected_targets.update(self._ancestor_targets[data_id])
            self.data_states[data_id] = new
        for target_id in tuple(affected_targets):
            touched.update(self._recalculate_target(target_id))
        for data_id, (old, new) in changed.items():
            if old.required_output == new.required_output:
                continue
            key = ("output", data_id, data_id)
            if new.required_output:
                touched.update(
                    self._add_obligation(
                        key, data_id, "required-output"
                    )
                )
            else:
                touched.update(
                    self._remove_obligation(key, "required-output")
                )
        self._end_event()
        self.state_revision += 1
        self._refresh(touched)
        return PruningMutation(
            self.graph.revision,
            self.state_revision,
            len(touched),
            True,
        )

    def add_task(self, task_id, inputs, outputs):
        self._begin_event()
        outputs = tuple(outputs)
        self.graph.add_task(task_id, inputs, outputs)
        self.task_states[int(task_id)] = "pending"
        for data_id in outputs:
            data_id = int(data_id)
            self.data_states[data_id] = DataState()
            self._ancestor_targets[data_id] = set()
            self._direct_sources[data_id] = set()
            self._recovery_sources[data_id] = collections.Counter()
        self.state_revision += 1
        touched = set(int(value) for value in outputs)
        touched.update(self._add_task_obligations(int(task_id)))
        self._end_event()
        self._refresh(touched)
        return PruningMutation(
            self.graph.revision,
            self.state_revision,
            len(touched),
            True,
        )

    def plan(self):
        records = tuple(
            dataclasses.replace(
                self._records[data_id],
                graph_revision=self.graph.revision,
                state_revision=self.state_revision,
            )
            for data_id in self.graph.data_ids
        )
        return _plan_from_records(
            records,
            self._last_nodes_examined,
            self._target_depths.items(),
        )

    def reference_plan(self):
        return reference_pruning_plan(
            self.graph,
            self.task_states,
            self.data_states,
            self.state_revision,
        )

    def assert_matches_reference(self):
        incremental = self.plan()
        reference = self.reference_plan()
        if incremental.semantic() != reference.semantic():
            raise AssertionError(
                "incremental pruning differs from reference: "
                f"incremental={incremental.semantic()} "
                f"reference={reference.semantic()}"
            )
        return incremental
