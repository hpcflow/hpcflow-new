from collections.abc import Mapping as _Mapping
from hpcflow.sdk.persistence.base import PersistentStore
from hpcflow.sdk.persistence.json import JSONPersistentStore
from hpcflow.sdk.persistence.zarr import ZarrPersistentStore, ZarrZipPersistentStore

# Because of python/mypy#4717, we need to disable an error here:
# mypy: disable-error-code="type-abstract"
_ALL_STORE_CLS: _Mapping[str, type[PersistentStore]] = {
    "zarr": ZarrPersistentStore,
    "zip": ZarrZipPersistentStore,
    "json": JSONPersistentStore,
    # "json-single": JSONPersistentStore,  # TODO
}
# Without that, there's literally no way to write the above with a sane type.

DEFAULT_STORE_FORMAT = "zarr"
ALL_STORE_FORMATS = tuple(_ALL_STORE_CLS.keys())
ALL_CREATE_STORE_FORMATS = tuple(
    k for k, v in _ALL_STORE_CLS.items() if v._features.create
)


def store_cls_from_str(store_format: str) -> type[PersistentStore]:
    try:
        return _ALL_STORE_CLS[store_format]
    except KeyError:
        raise ValueError(f"Store format {store_format!r} not known.")
