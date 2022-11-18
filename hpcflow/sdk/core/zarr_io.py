import copy

import numpy as np

from hpcflow.sdk.core.utils import get_in_container, get_relative_path, set_in_container


PRIMITIVES = (
    int,
    float,
    str,
    type(None),
)


def zarr_encode(data, zarr_group):
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

    data, encoded = _zarr_encode(data, zarr_group)
    zarr_group.attrs["encoded"] = encoded
    zarr_group.attrs["data"] = data


def zarr_decode(zarr_group, path=None, dataset_copy=False):

    path = path or []

    data = get_in_container(zarr_group.attrs["data"], path)
    data = copy.deepcopy(data)

    for item in zarr_group.attrs["encoded"]:

        try:
            rel_path = get_relative_path(item["path"], path)
        except ValueError:
            continue

        dataset = zarr_group.get(item["dataset"])
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
