from __future__ import annotations
from contextlib import contextmanager
import copy
from pathlib import Path

import shutil
import time
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple, Union
import numpy as np
import zarr
from numcodecs import MsgPack

from hpcflow.sdk.core.errors import WorkflowNotFoundError
from hpcflow.sdk.core.utils import (
    bisect_slice,
    ensure_in,
    get_in_container,
    get_md5_hash,
    get_relative_path,
    set_in_container,
)
from hpcflow.sdk.persistence import PersistentStore, dropbox_permission_err_retry
from hpcflow.sdk.typing import PathLike


def _encode_numpy_array(obj, type_lookup, path, root_group, arr_path):
    # Might need to generate new group:
    param_arr_group = root_group.require_group(arr_path)
    names = [int(i) for i in param_arr_group.keys()]
    if not names:
        new_idx = 0
    else:
        new_idx = max(names) + 1
    param_arr_group.create_dataset(name=f"arr_{new_idx}", data=obj)
    type_lookup["arrays"].append([path, new_idx])

    return None


def _decode_numpy_arrays(obj, type_lookup, path, arr_group, dataset_copy):
    for arr_path, arr_idx in type_lookup["arrays"]:
        try:
            rel_path = get_relative_path(arr_path, path)
        except ValueError:
            continue

        dataset = arr_group.get(f"arr_{arr_idx}")
        if dataset_copy:
            dataset = dataset[:]

        if rel_path:
            set_in_container(obj, rel_path, dataset)
        else:
            obj = dataset

    return obj


class ZarrPersistentStore(PersistentStore):
    """An efficient storage backend using Zarr that supports parameter-setting from
    multiple processes."""

    _param_grp_name = "parameter_data"
    _elem_grp_name = "element_data"
    _param_base_arr_name = "base"
    _param_sources_arr_name = "sources"
    _param_user_arr_grp_name = "arrays"
    _param_data_arr_grp_name = lambda _, param_idx: f"param_{param_idx}"
    _task_grp_name = lambda _, insert_ID: f"task_{insert_ID}"
    _task_elem_arr_name = "elements"

    _parameter_encoders = {np.ndarray: _encode_numpy_array}  # keys are types
    _parameter_decoders = {"arrays": _decode_numpy_arrays}  # keys are keys in type_lookup

    def __init__(self, workflow: Workflow) -> None:
        self._metadata = None  # cache used in `cached_load` context manager
        super().__init__(workflow)

    def exists(self) -> bool:
        try:
            self._get_root_group()
        except zarr.errors.PathNotFoundError:
            return False
        return True

    @property
    def has_pending(self) -> bool:
        """Returns True if there are pending changes that are not yet committed."""
        return any(bool(v) for k, v in self._pending.items() if k != "element_attrs")

    def _get_pending_dct(self) -> Dict:
        dct = super()._get_pending_dct()
        dct["element_attrs"] = {}  # keys are task indices
        dct["parameter_data"] = 0  # keep number of pending data rather than indices
        return dct

    @classmethod
    def write_empty_workflow(
        cls,
        template_js: Dict,
        template_components_js: Dict,
        path: Path,
        overwrite: bool,
    ) -> None:

        replaced_file = None
        if path.exists():
            if overwrite:
                replaced_file = cls._rename_existing(path)
            else:
                raise ValueError(f"Path already exists: {path}.")

        metadata = {
            "template": template_js,
            "template_components": template_components_js,
            "num_added_tasks": 0,
        }
        if replaced_file:
            metadata["replaced_file"] = str(replaced_file.name)

        store = zarr.DirectoryStore(path)
        root = zarr.group(store=store, overwrite=overwrite)
        root.attrs.update(metadata)

        root.create_group(name=cls._elem_grp_name)
        parameter_data = root.create_group(name=cls._param_grp_name)
        parameter_data.create_dataset(
            name=cls._param_base_arr_name,
            shape=0,
            dtype=object,
            object_codec=MsgPack(),
            chunks=1,
        )
        parameter_data.create_dataset(
            name=cls._param_sources_arr_name,
            shape=0,
            dtype=object,
            object_codec=MsgPack(),
            chunks=1000,  # TODO: check this is a sensible size with many parameters
        )
        parameter_data.create_group(name=cls._param_user_arr_grp_name)

    def load_metadata(self):
        return self._metadata or self._load_metadata()

    def _load_metadata(self):
        return self._get_root_group(mode="r").attrs.asdict()

    @contextmanager
    def cached_load(self) -> Iterator[Dict]:
        """Context manager to cache the root attributes (i.e. metadata)."""
        if self._metadata:
            yield
        else:
            try:
                self._metadata = self._load_metadata()
                yield
            finally:
                self._metadata = None

    def _get_root_group(self, mode: str = "r") -> zarr.Group:
        return zarr.open(self.workflow.path, mode=mode)

    def _get_parameter_group(self, mode: str = "r") -> zarr.Group:
        return self._get_root_group(mode=mode).get(self._param_grp_name)

    def _get_parameter_base_array(self, mode: str = "r") -> zarr.Array:
        return self._get_parameter_group(mode=mode).get(self._param_base_arr_name)

    def _get_parameter_sources_array(self, mode: str = "r") -> zarr.Array:
        return self._get_parameter_group(mode=mode).get(self._param_sources_arr_name)

    def _get_parameter_user_array_group(self, mode: str = "r") -> zarr.Group:
        return self._get_parameter_group(mode=mode).get(self._param_user_arr_grp_name)

    def _get_parameter_data_array_group(
        self,
        parameter_idx: int,
        mode: str = "r",
    ) -> zarr.Group:
        return self._get_parameter_user_array_group(mode=mode).get(
            self._param_data_arr_grp_name(parameter_idx)
        )

    def _get_element_group(self, mode: str = "r") -> zarr.Group:
        return self._get_root_group(mode=mode).get(self._elem_grp_name)

    def _get_task_group_path(self, insert_ID: int) -> str:
        return self._task_grp_name(insert_ID)

    def _get_task_group(self, insert_ID: int, mode: str = "r") -> zarr.Group:
        return self._get_element_group(mode=mode).get(self._task_grp_name(insert_ID))

    def _get_task_elements_array(self, insert_ID: int, mode: str = "r") -> zarr.Array:
        return self._get_task_group(insert_ID, mode=mode).get(self._task_elem_arr_name)

    def _get_task_element_attrs(self, task_idx: int, task_insert_ID: int) -> Dict:
        if task_idx in self._pending["element_attrs"]:
            attrs = self._pending["element_attrs"][task_idx]
        elif task_idx in self._pending["tasks"]:
            # the task is new and not yet committed
            attrs = self._get_element_array_empty_attrs()
        else:
            attrs = self._get_task_elements_array(task_insert_ID, mode="r").attrs.asdict()
        return attrs

    def add_elements(
        self,
        task_idx: int,
        task_insert_ID: int,
        elements: List[Dict],
    ) -> None:

        attrs_original = self._get_task_element_attrs(task_idx, task_insert_ID)
        elements, attrs = self._compress_elements(elements, attrs_original)
        if attrs != attrs_original:
            if task_idx not in self._pending["element_attrs"]:
                self._pending["element_attrs"][task_idx] = {}
            self._pending["element_attrs"][task_idx].update(attrs)

        return super().add_elements(task_idx, task_insert_ID, elements)

    def _compress_elements(self, elements: List, attrs: Dict) -> Tuple[List, Dict]:
        """The great listification of election action run data.

        See also: `_decompress_elements` for the inverse operation.

        """

        attrs = copy.deepcopy(attrs)
        compressed = []
        for elem in elements:
            seq_idx = [
                [ensure_in(k, attrs["sequences"]), v] for k, v in elem["seq_idx"].items()
            ]
            loop_idx = [
                [ensure_in(k, attrs["loops"]), v] for k, v in elem["loop_idx"].items()
            ]
            schema_params = [
                ensure_in(k, attrs["schema_parameters"])
                for k in elem["schema_parameters"]
            ]
            act_runs = []
            for act_idx, runs in elem["actions"].items():
                act_run_i = [
                    act_idx,
                    [
                        [
                            [ensure_in(dk, attrs["parameter_paths"]), dv]
                            for dk, dv in r["data_idx"].items()
                        ]
                        for r in runs
                    ],
                ]
                act_runs.append(act_run_i)

            compact = [
                elem["global_idx"],
                elem["index"],
                elem["es_idx"],
                seq_idx,
                loop_idx,
                schema_params,
                act_runs,
            ]
            compressed.append(compact)
        return compressed, attrs

    def _decompress_elements(self, elements: List, attrs: Dict) -> List:

        out = []
        for elem in elements:
            actions = {}
            for (act_idx, runs) in elem[6]:
                actions[act_idx] = [
                    {
                        "data_idx": {
                            attrs["parameter_paths"][k]: v for (k, v) in data_idx
                        },
                    }
                    for data_idx in runs
                ]
            elem_i = {
                "global_idx": elem[0],
                "index": elem[1],
                "es_idx": elem[2],
                "seq_idx": {attrs["sequences"][k]: v for (k, v) in elem[3]},
                "loop_idx": {attrs["loops"][k]: v for (k, v) in elem[4]},
                "schema_parameters": [attrs["schema_parameters"][k] for k in elem[5]],
                "actions": actions,
            }
            out.append(elem_i)
        return out

    @staticmethod
    def _get_element_array_empty_attrs() -> Dict:
        return {
            "sequences": [],
            "loops": [],
            "schema_parameters": [],
            "parameter_paths": [],
            "input_source_paths": [],
            "source_types": [],
        }

    def _get_zarr_store(self):
        return self._get_root_group().store

    def _remove_pending_parameter_data(self) -> None:
        """Delete pending parameter data from disk."""
        base = self._get_parameter_base_array(mode="r+")
        sources = self._get_parameter_sources_array(mode="r+")
        for param_idx in range(self._pending["parameter_data"], 0, -1):
            grp = self._get_parameter_data_array_group(param_idx - 1)
            if grp:
                zarr.storage.rmdir(store=self._get_zarr_store(), path=grp.path)
        base.resize(base.size - self._pending["parameter_data"])
        sources.resize(sources.size - self._pending["parameter_data"])

    def reject_pending(self) -> None:
        if self._pending["parameter_data"]:
            self._remove_pending_parameter_data()
        super().reject_pending()

    def commit_pending(self) -> None:

        md = self.load_metadata()

        # merge new tasks:
        for task_idx, task_js in self._pending["template_tasks"].items():
            md["template"]["tasks"].insert(task_idx, task_js)  # TODO should be index?

        # write new workflow tasks to disk:
        for task_idx, _ in self._pending["tasks"].items():

            insert_ID = self._pending["template_tasks"][task_idx]["insert_ID"]
            task_group = self._get_element_group(mode="r+").create_group(
                self._get_task_group_path(insert_ID)
            )
            element_arr = task_group.create_dataset(
                name=self._task_elem_arr_name,
                shape=0,
                dtype=object,
                object_codec=MsgPack(),
                chunks=1000,  # TODO: check this is a sensible size with many elements
            )
            element_arr.attrs.update(self._get_element_array_empty_attrs())
            md["num_added_tasks"] += 1

        # merge new template components:
        self._merge_pending_template_components(md["template_components"])

        # merge new element sets:
        for task_idx, es_js in self._pending["element_sets"].items():
            md["template"]["tasks"][task_idx]["element_sets"].extend(es_js)

        # write new elements to disk:
        for (task_idx, insert_ID), elements in self._pending["elements"].items():
            elem_arr = self._get_task_elements_array(insert_ID, mode="r+")
            elem_arr_add = np.empty((len(elements)), dtype=object)
            elem_arr_add[:] = elements
            elem_arr.append(elem_arr_add)
            if task_idx in self._pending["element_attrs"]:
                elem_arr.attrs.put(self._pending["element_attrs"][task_idx])

        if self._pending["remove_replaced_file_record"]:
            del md["replaced_file"]

        # commit updated metadata:
        self._get_root_group(mode="r+").attrs.put(md)
        self.clear_pending()

    def _get_persistent_template_components(self) -> Dict:
        return self.load_metadata()["template_components"]

    def get_template(self) -> Dict:
        # No need to consider pending; this is called once per Workflow object
        return self.load_metadata()["template"]

    def get_num_added_tasks(self) -> int:
        return self.load_metadata()["num_added_tasks"] + len(self._pending["tasks"])

    def get_all_tasks_metadata(self) -> List[Dict]:
        out = []
        for _, grp in self._get_element_group().groups():
            elem_arr = grp.get(self._task_elem_arr_name)
            out.append({"num_elements": len(elem_arr)})
        return out

    def get_task_elements(
        self,
        task_idx: int,
        task_insert_ID: int,
        selection: slice,
    ) -> List:

        num_pers = self.workflow.tasks[task_idx]._num_elements
        pers_slice, pend_slice = bisect_slice(selection, num_pers)
        pers_range = range(pers_slice.start, pers_slice.stop, pers_slice.step)

        if len(pers_range):
            elem_arr = self._get_task_elements_array(task_insert_ID)
            try:
                elements = list(elem_arr[pers_slice])
            except zarr.errors.NegativeStepError:
                elements = [elem_arr[idx] for idx in pers_range]
        else:
            elements = []

        key = (task_idx, task_insert_ID)
        if key in self._pending["elements"]:
            elements += self._pending["elements"][key][pend_slice]

        return self._decompress_elements(elements, self._get_task_element_attrs(*key))

    def _encode_parameter_data(
        self,
        obj: Any,
        root_group: zarr.Group,
        arr_path: str,
        path: List = None,
        type_lookup: Optional[Dict] = None,
    ) -> Dict[str, Any]:

        return super()._encode_parameter_data(
            obj=obj,
            path=path,
            type_lookup=type_lookup,
            root_group=root_group,
            arr_path=arr_path,
        )

    def _decode_parameter_data(
        self,
        data: Union[None, Dict],
        arr_group: zarr.Group,
        path: Optional[List[str]] = None,
        dataset_copy=False,
    ) -> Any:

        return super()._decode_parameter_data(
            data=data,
            path=path,
            arr_group=arr_group,
            dataset_copy=dataset_copy,
        )

    def _add_parameter_data(self, data: Any, source: Dict) -> int:

        base_arr = self._get_parameter_base_array(mode="r+")
        sources = self._get_parameter_sources_array(mode="r+")
        idx = base_arr.size

        if data is not None:
            data = self._encode_parameter_data(
                obj=data["data"],
                root_group=self._get_parameter_user_array_group(mode="r+"),
                arr_path=self._param_data_arr_grp_name(idx),
            )

        base_arr.append([data])
        sources.append([source])
        self._pending["parameter_data"] += 1
        self.save()

        return idx

    def set_parameter(self, index: int, data: Any) -> None:
        """Set the value of a pre-allocated parameter."""

        if self.is_parameter_set(index):
            raise RuntimeError(f"Parameter at index {index} is already set!")

        base_arr = self._get_parameter_base_array(mode="r+")

        data, type_lookup = self._encode_parameter_data(
            obj=data,
            root_group=self._get_parameter_user_array_group(mode="r+"),
            arr_path=self._param_data_arr_grp_name(index),
        )

        base_arr[index] = {"data": data, "type_lookup": type_lookup}

    def _get_parameter_data(self, index: int) -> Any:
        return self._get_parameter_base_array(mode="r")[index]

    def get_parameter_data(self, index: int) -> Any:
        return self._decode_parameter_data(
            data=self._get_parameter_data(index),
            arr_group=self._get_parameter_data_array_group(index),
        )

    def get_parameter_source(self, index: int) -> Dict:
        return self._get_parameter_sources_array(mode="r")[index]

    def get_all_parameter_data(self) -> Dict[int, Any]:
        max_key = self._get_parameter_base_array(mode="r").size - 1
        out = {}
        for idx in range(max_key + 1):
            out[idx] = self.get_parameter_data(idx)
        return out

    def is_parameter_set(self, index: int) -> bool:
        return self._get_parameter_data(index) is not None

    def check_parameters_exist(
        self, indices: Union[int, List[int]]
    ) -> Union[bool, List[bool]]:
        is_multi = True
        if not isinstance(indices, (list, tuple)):
            is_multi = False
            indices = [indices]
        base = self._get_parameter_base_array(mode="r")
        idx_range = range(base.size)
        exists = [i in idx_range for i in indices]
        if not is_multi:
            exists = exists[0]
        return exists

    @dropbox_permission_err_retry
    def delete_no_confirm(self) -> None:
        """Permanently delete the workflow data with no confirmation."""
        # Dropbox (on Windows, at least) seems to try to re-sync some of the workflow
        # files if it is deleted soon after creation, which is the case on a failed
        # workflow creation (e.g. missing inputs):
        while self.workflow.path.is_dir():
            shutil.rmtree(self.workflow.path)
            time.sleep(0.5)

    @dropbox_permission_err_retry
    def remove_replaced_file(self) -> None:
        md = self.load_metadata()
        if "replaced_file" in md:
            shutil.rmtree(Path(md["replaced_file"]))
            self._pending["remove_replaced_file_record"] = True
            self.save()

    @dropbox_permission_err_retry
    def reinstate_replaced_file(self) -> None:
        print(f"reinstate replaced file!")
        md = self.load_metadata()
        if "replaced_file" in md:
            # TODO does rename work for the dir?
            Path(md["replaced_file"]).rename(self.path)

    def copy(self, path: PathLike = None) -> None:
        shutil.copytree(self.path, path)

    def is_modified_on_disk(self) -> bool:
        if self._metadata:
            return get_md5_hash(self._load_metadata()) != get_md5_hash(self._metadata)
        else:
            # nothing to compare to
            return False
