from pathlib import Path
import random
import string
from typing import Type

from hpcflow.sdk.core.errors import WorkflowNotFoundError

from .base import PersistentStore, dropbox_permission_err_retry
from .json import JSONPersistentStore
from .zarr import ZarrPersistentStore

ALL_STORE_FORMATS = ("zarr", "json")
DEFAULT_STORE_FORMAT = "zarr"


def store_cls_from_path(workflow_path: Path) -> Type[PersistentStore]:
    if ZarrPersistentStore.path_has_store(workflow_path):
        return ZarrPersistentStore
    elif JSONPersistentStore.path_has_store(workflow_path):
        return JSONPersistentStore
    else:
        raise WorkflowNotFoundError(
            f"No workflow of a known store type found at path: {workflow_path!r}."
        )


def store_cls_from_str(store_format: str) -> Type[PersistentStore]:
    if store_format == "zarr":
        return ZarrPersistentStore
    elif store_format == "json":
        return JSONPersistentStore
    else:
        raise ValueError(f"Store format {store_format!r} not known.")


@dropbox_permission_err_retry
def temporary_workflow_rename(path):
    """Rename an existing same-path workflow directory so we can restore it if workflow
    creation fails"""
    temp_ext = "".join(random.choices(string.ascii_letters, k=10))
    replaced_dir = path.with_suffix(f"{path.suffix}.{temp_ext}")
    path.rename(replaced_dir)
    return replaced_dir
