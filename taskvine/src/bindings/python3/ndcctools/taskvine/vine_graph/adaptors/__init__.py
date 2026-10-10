"""Adaptors that convert external graph formats into Graphs."""

from .adaptor import from_dask, graphed_plan_to_graph

__all__ = ["from_dask", "graphed_plan_to_graph"]
