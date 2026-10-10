"""Public adaptor entry points for converting external graph formats into Graphs."""

from .dask_adaptor import from_dask
from .graphed import graphed_plan_to_graph

__all__ = ["from_dask", "graphed_plan_to_graph"]
