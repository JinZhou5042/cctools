"""Adaptors that convert external graph formats into Graphs."""

from .dask_adaptor import from_dask

__all__ = ["from_dask"]
