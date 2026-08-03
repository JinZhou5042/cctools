"""Pure recovery-wave planning."""


def select_recovery_audit_data_ids(
    affected_data_ids,
    producer_by_data_id,
    dependencies,
    dependents,
    task_order,
):
    """Select deepest affected producers and suppress covered ancestors."""

    affected_by_task = {}
    for data_id in affected_data_ids:
        affected_by_task.setdefault(producer_by_data_id[data_id], []).append(
            data_id
        )
    affected_tasks = set(affected_by_task)
    ancestor_closure = set(affected_tasks)
    stack = list(affected_tasks)
    while stack:
        ancestor = stack.pop()
        for parent_id in dependencies[ancestor]:
            if parent_id in ancestor_closure:
                continue
            ancestor_closure.add(parent_id)
            stack.append(parent_id)

    has_affected_descendant = {}
    for task_id in reversed(tuple(task_order)):
        if task_id not in ancestor_closure:
            continue
        has_affected_descendant[task_id] = any(
            child_id in affected_tasks
            or has_affected_descendant.get(child_id, False)
            for child_id in dependents[task_id]
            if child_id in ancestor_closure
        )
    covered_ancestors = {
        task_id
        for task_id in affected_tasks
        if has_affected_descendant[task_id]
    }
    return tuple(
        sorted(
            min(affected_by_task[task_id])
            for task_id in affected_tasks - covered_ancestors
        )
    )
