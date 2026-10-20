"""Public adaptor entry points for converting external graph formats into Graphs."""

from .dask_adaptor import from_dask

__all__ = ["from_dask"]
