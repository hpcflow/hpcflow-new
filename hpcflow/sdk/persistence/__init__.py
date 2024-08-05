"""
Workflow persistence subsystem.
"""

import copy
from pathlib import Path
import random
import string
import time
from typing import Type, Union

from reretry import retry

from hpcflow.sdk.persistence.base import PersistentStore
from hpcflow.sdk.persistence.json import JSONPersistentStore
from hpcflow.sdk.persistence.zarr import ZarrPersistentStore, ZarrZipPersistentStore

_ALL_STORE_CLS = {
    "zarr": ZarrPersistentStore,
    "zip": ZarrZipPersistentStore,
    "json": JSONPersistentStore,
    # "json-single": JSONPersistentStore,  # TODO
}
#: The name of the default persistence store.
DEFAULT_STORE_FORMAT = "zarr"
#: The persistence formats supported.
ALL_STORE_FORMATS = tuple(_ALL_STORE_CLS.keys())
#: The persistence formats supported for creation.
ALL_CREATE_STORE_FORMATS = tuple(
    k for k, v in _ALL_STORE_CLS.items() if v._features.create
)


def store_cls_from_str(store_format: str) -> Type[PersistentStore]:
    """
    Get the class that implements the persistence store from its name.
    """
    try:
        return _ALL_STORE_CLS[store_format]
    except KeyError:
        raise ValueError(f"Store format {store_format!r} not known.")
