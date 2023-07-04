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

ALL_STORE_CLS = {
    "zarr": ZarrPersistentStore,
    "zip": ZarrZipPersistentStore,
    "json": JSONPersistentStore,
    # "json-single": JSONPersistentStore,  # TODO
}
DEFAULT_STORE_FORMAT = "zarr"
ALL_STORE_FORMATS = tuple(ALL_STORE_CLS.keys())
ALL_CREATE_STORE_FORMATS = tuple(
    k for k, v in ALL_STORE_CLS.items() if v._features.create
)


def store_cls_from_str(store_format: str) -> Type[PersistentStore]:
    try:
        return ALL_STORE_CLS[store_format]
    except KeyError:
        raise ValueError(f"Store format {store_format!r} not known.")
