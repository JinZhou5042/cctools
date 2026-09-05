"""Shared fixed-point data value model for storage admission."""


def data_value_score(
    size,
    *,
    remaining_uses=0,
    fanout=0,
    recompute_depth=0,
    replicas=1,
    durable=False,
):
    size = max(1, int(size))
    scarcity = max(0, 2 - int(replicas))
    benefit = (
        1
        + 8 * int(remaining_uses)
        + 2 * int(fanout)
        + 4 * int(recompute_depth)
        + 8 * scarcity
        + (0 if durable else 16)
    )
    return benefit * 1_000_000 // size
