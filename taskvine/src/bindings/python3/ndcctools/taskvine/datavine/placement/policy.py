"""Deterministic bounded prefetch and placement policy."""

import dataclasses


@dataclasses.dataclass(frozen=True)
class PrefetchCandidate:
    data_id: int
    size: int
    fanout: int

    @property
    def score(self):
        return self.fanout / max(self.size, 1)


def select_prefetch(candidates, byte_budget, item_budget):
    selected = []
    used = 0
    ordered = sorted(
        candidates,
        key=lambda value: (
            -value.fanout,
            -value.score,
            value.size,
            value.data_id,
        ),
    )
    for candidate in ordered:
        if len(selected) >= item_budget:
            break
        if candidate.fanout < 2:
            continue
        if used + candidate.size > byte_budget:
            continue
        selected.append(candidate)
        used += candidate.size
    return tuple(selected)
