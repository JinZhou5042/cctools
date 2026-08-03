"""Canonical Python serialization used at the DataVine data-plane boundary."""

import pickle
import platform
import sys

import cloudpickle

from .models import SerializationMetadata


def serialize(value):
    metadata = SerializationMetadata(
        serializer="cloudpickle",
        serializer_version=cloudpickle.__version__,
        protocol=pickle.HIGHEST_PROTOCOL,
        python_implementation=platform.python_implementation(),
        python_version=(sys.version_info.major, sys.version_info.minor),
        type_module=type(value).__module__,
        type_qualname=type(value).__qualname__,
    )
    return metadata, cloudpickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
