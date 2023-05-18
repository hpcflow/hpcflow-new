from typing import Any, Dict, Union

import zarr
import numpy as np

from hpcflow.sdk.core.utils import get_in_container, get_relative_path, set_in_container


PRIMITIVES = (
    int,
    float,
    str,
    type(None),
)


def _zarr_encode(obj, zarr_group, path=None, encoded=None):
    path = path or []
    encoded = encoded or []

    if len(path) > 50:
        raise RuntimeError("I'm in too deep!")

    if isinstance(obj, ZarrEncodable):
        obj = obj.to_dict()
        out, encoded = _zarr_encode(
            obj, zarr_group=zarr_group, path=path, encoded=encoded
        )

    elif isinstance(obj, (list, tuple, set)):
        out = []
        for idx, item in enumerate(obj):
            item, encoded = _zarr_encode(item, zarr_group, path + [idx], encoded)
            out.append(item)
        if isinstance(obj, tuple):
            out = tuple(out)
        elif isinstance(obj, set):
            out = set(out)

    elif isinstance(obj, dict):
        out = {}
        for dct_key, dct_val in obj.items():
            dct_val, encoded = _zarr_encode(
                dct_val, zarr_group, path + [dct_key], encoded
            )
            out.update({dct_key: dct_val})

    elif isinstance(obj, PRIMITIVES):
        out = obj

    elif isinstance(obj, np.ndarray):
        names = [int(i) for i in zarr_group.keys()]
        if not names:
            new_name = "0"
        else:
            new_name = str(max(names) + 1)

        zarr_group.create_dataset(name=new_name, data=obj)
        encoded.append(
            {
                "path": path,
                "dataset": new_name,
            }
        )
        out = None

    return out, encoded


def zarr_encode(data, zarr_group, is_pending_add, is_set):
    data, encoded = _zarr_encode(data, zarr_group)
    zarr_group.attrs["encoded"] = encoded
    zarr_group.attrs["data"] = data
    zarr_group.attrs["is_set"] = is_set
    if is_pending_add:
        zarr_group.attrs["is_pending_add"] = is_pending_add


# TODO: need to separate so this func doesn't need matflow


def _zarr_encode_NEW(
    obj: Any,
    base_arr: zarr.Array,
    root_group: zarr.Group,
    arr_path: str,
    path=None,
    arr_lookup=None,
):
    """
    Save arbitrarily-nested Python-primitive, `ZarrEncodable` and numpy array objects into
    Zarr.

    Parameters
    ----------
    obj
    base_arr
        Zarr object array into which (the top-level) `obj` is saved using the MsgPack
        encoder.
    root_group
        Parent Zarr group into which new Zarr arrays will be added (at `arr_path`).
    arr_path
        Path relative to `root_group` into which new Zarr arrays will be added.

    Returns
    -------
    (data, arr_lookup)


    """

    path = path or []
    arr_lookup = arr_lookup or []

    if len(path) > 50:
        raise RuntimeError("I'm in too deep!")

    if isinstance(obj, ZarrEncodable):
        data, arr_lookup = _zarr_encode_NEW(
            obj=obj.to_dict(),
            base_arr=base_arr,
            root_group=root_group,
            arr_path=arr_path,
            path=path,
        )

    elif isinstance(obj, (list, tuple, set)):
        data = []
        for idx, item in enumerate(obj):
            item, arr_lookup = _zarr_encode_NEW(
                obj=item,
                base_arr=base_arr,
                root_group=root_group,
                arr_path=arr_path,
                path=path + [idx],
            )
            data.append(item)
        if isinstance(obj, tuple):
            data = tuple(data)
        elif isinstance(obj, set):
            data = set(data)

    elif isinstance(obj, dict):
        data = {}
        for dct_key, dct_val in obj.items():
            dct_val, arr_lookup = _zarr_encode_NEW(
                obj=dct_val,
                base_arr=base_arr,
                root_group=root_group,
                arr_path=arr_path,
                path=path + [dct_key],
            )
            data[dct_key] = dct_val

    elif isinstance(obj, PRIMITIVES):
        data = obj

    elif isinstance(obj, np.ndarray):
        # Might need to generate new group:
        param_arr_group = root_group.require_group(arr_path)
        names = [int(i) for i in param_arr_group.keys()]
        if not names:
            new_idx = 0
        else:
            new_idx = max(names) + 1
        param_arr_group.create_dataset(name=f"arr_{new_idx}", data=obj)
        arr_lookup.append([path, new_idx])
        data = None

    return data, arr_lookup


def zarr_decode(
    param_data: Union[None, Dict],
    arr_group: zarr.Group,
    path=None,
    dataset_copy=False,
):
    if param_data is None:
        return None

    path = path or []

    data = get_in_container(param_data["data"], path)
    # data = copy.deepcopy(data)  # TODO: why did we copy?

    for arr_path, arr_idx in param_data["arr_lookup"]:
        try:
            rel_path = get_relative_path(arr_path, path)
        except ValueError:
            continue

        dataset = arr_group.get(f"arr_{arr_idx}")

        if dataset_copy:
            dataset = dataset[:]

        if rel_path:
            set_in_container(data, rel_path, dataset)
        else:
            data = dataset

    return data


class ZarrEncodable:
    _typ = None

    def to_dict(self):
        if hasattr(self, "__dict__"):
            return dict(self.__dict__)
        elif hasattr(self, "__slots__"):
            return {k: getattr(self, k) for k in self.__slots__}

    def to_zarr(self, zarr_group):
        data = self.to_dict()
        zarr_encode(data, zarr_group)

    @classmethod
    def from_zarr(cls, zarr_group, dataset_copy=False):
        data = zarr_decode(zarr_group, dataset_copy=dataset_copy)
        return cls(**data)
