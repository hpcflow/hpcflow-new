from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from os import PathLike
from pathlib import Path
import shutil
from typing import Any, Dict, Generator, Iterator, List, Optional, Union
from hpcflow.sdk.core.errors import WorkflowNotFoundError
from hpcflow.sdk.core.utils import get_md5_hash

from hpcflow.sdk.persistence import PersistentStore, dropbox_permission_err_retry


class JSONPersistentStore(PersistentStore):
    """A verbose but inefficient storage backend, to help with understanding and
    debugging."""

    def __init__(self, workflow: Workflow) -> None:
        self._loaded = None  # cache used in `cached_load` context manager
        super().__init__(workflow)

    def exists(self) -> bool:
        return self.path.is_file()

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
        dat = {
            "parameter_data": {},
            "parameter_sources": {},
            "template_components": template_components_js,
            "template": template_js,
            "tasks": [],
            "num_added_tasks": 0,
        }
        if replaced_file:
            dat["replaced_file"] = str(replaced_file.name)

        cls._dump_to_path(path, dat)

    @contextmanager
    def cached_load(self) -> Iterator[Dict]:
        """Context manager to cache the whole JSON document, allowing for multiple read
        operations with one disk read."""
        if self._loaded:
            yield
        else:
            try:
                self._loaded = self._load()
                yield
            finally:
                self._loaded = None

    def _load(self) -> Dict:
        with open(self.path, "rt") as fp:
            return json.load(fp)

    def load(self) -> Dict:
        return self._loaded or self._load()

    @staticmethod
    @dropbox_permission_err_retry
    def _dump_to_path(path, data: Dict) -> None:
        with open(path, "wt", newline="") as fp:
            json.dump(data, fp, indent=4)

    def _dump(self, wk_data: Dict) -> None:
        self._dump_to_path(self.path, wk_data)

    def _add_parameter_data(self, data: Any, source: Dict) -> int:

        idx = len(self.load()["parameter_data"]) + len(self._pending["parameter_data"])

        if data is not None:
            data = self._encode_parameter_data(data["data"])

        self._pending["parameter_data"][idx] = data
        self._pending["parameter_sources"][idx] = source
        self.save()

        return idx

    def set_parameter(self, index: int, data: Any) -> None:
        """Set the value of a pre-allocated parameter."""
        if self.is_parameter_set(index):
            raise RuntimeError(f"Parameter at index {index} is already set!")
        self._pending["parameter_data"][index] = self._encode_parameter_data(data)
        self.save()

    def get_parameter_data(self, index: int) -> Any:
        if index in self._pending["parameter_data"]:
            data = self._pending["parameter_data"][index]
        else:
            data = self.load()["parameter_data"][str(index)]
        return self._decode_parameter_data(data=data)

    def get_parameter_source(self, index: int) -> Dict:
        # TODO: check pending
        return self.load()["parameter_sources"][str(index)]

    def get_all_parameter_data(self) -> Dict[int, Any]:
        if self._pending["parameter_data"]:
            max_key = max(self._pending["parameter_data"].keys())
        else:
            max_key = int(max(self.load()["parameter_data"].keys(), key=lambda x: int(x)))

        out = {}
        for idx in range(max_key + 1):
            out[idx] = self.get_parameter_data(idx)

        return out

    def is_parameter_set(self, index: int) -> bool:
        return self.load()["parameter_data"][str(index)] is not None

    def check_parameters_exist(
        self, indices: Union[int, List[int]]
    ) -> Union[bool, List[bool]]:
        is_multi = True
        if not isinstance(indices, (list, tuple)):
            is_multi = False
            indices = [indices]
        exists = [
            i in self._pending["parameter_data"]
            or str(i) in self.load()["parameter_data"]
            for i in indices
        ]
        if not is_multi:
            exists = exists[0]
        return exists

    def commit_pending(self) -> None:

        wk_data = self.load()

        # commit new tasks:
        for new_index, task_js in self._pending["template_tasks"].items():
            wk_data["template"]["tasks"].insert(new_index, task_js)

        # commit new workflow tasks:
        for new_index, wk_task in self._pending["tasks"].items():
            wk_data["tasks"].insert(new_index, wk_task)
            wk_data["num_added_tasks"] += 1

        # commit new template components:
        self._merge_pending_template_components(wk_data["template_components"])

        # commit new element sets:
        for task_idx, es_js in self._pending["element_sets"].items():
            wk_data["template"]["tasks"][task_idx]["element_sets"].extend(es_js)

        # commit new elements:
        for (task_idx, _), elements in self._pending["elements"].items():
            wk_data["tasks"][task_idx]["elements"].extend(elements)

        # commit new parameters:
        for param_idx, param_dat in self._pending["parameter_data"].items():
            wk_data["parameter_data"][param_idx] = param_dat

        for param_idx, param_src in self._pending["parameter_sources"].items():
            wk_data["parameter_sources"][param_idx] = param_src

        if self._pending["remove_replaced_file_record"]:
            del wk_data["replaced_file"]

        self._dump(wk_data)
        self.clear_pending()

    def _get_persistent_template_components(self) -> Dict:
        return self.load()["template_components"]

    def get_template(self) -> Dict:
        # No need to consider pending; this is called once per Workflow object
        return self.load()["template"]

    def get_num_added_tasks(self) -> int:
        return self.load()["num_added_tasks"] + len(self._pending["tasks"])

    def get_all_tasks_metadata(self) -> List[Dict]:
        # No need to consider pending; this is called once per Workflow object
        return [{"num_elements": len(i["elements"])} for i in self.load()["tasks"]]

    def get_task_elements(
        self,
        task_idx: int,
        task_insert_ID: int,
        selection: slice,
    ) -> List[Dict]:

        if task_idx in self._pending["tasks"]:
            wk_task_dat = self._pending["tasks"][task_idx]
        else:
            wk_task_dat = self.load()["tasks"][task_idx]

        wk_task = copy.deepcopy(wk_task_dat)
        key = (task_idx, task_insert_ID)
        if key in self._pending["elements"]:
            wk_task["elements"] += copy.deepcopy(self._pending["elements"][key])

        # TODO: maybe support slicing beyond end of available elements, like a list?
        #       using slice.indices ?
        elements = []
        for idx in range(selection.start, selection.stop, selection.step):
            for act_idx_str in list(wk_task["elements"][idx]["actions"].keys()):
                runs = wk_task["elements"][idx]["actions"].pop(act_idx_str)
                wk_task["elements"][idx]["actions"][int(act_idx_str)] = runs
            elements.append(wk_task["elements"][idx])

        return elements

    def get_task_parameters(self, task_idx: int, selection: slice) -> List[Dict]:
        pass

    @dropbox_permission_err_retry
    def delete_no_confirm(self) -> None:
        """Permanently delete the workflow data with no confirmation."""
        self.path.unlink()

    @dropbox_permission_err_retry
    def remove_replaced_file(self) -> None:
        wk_data = self.load()
        if "replaced_file" in wk_data:
            Path(wk_data["replaced_file"]).unlink()
            self._pending["remove_replaced_file_record"] = True
            self.save()

    @dropbox_permission_err_retry
    def reinstate_replaced_file(self) -> None:
        wk_data = self.load()
        if "replaced_file" in wk_data:
            Path(wk_data["replaced_file"]).rename(self.path)

    def copy(self, path: PathLike = None) -> None:
        shutil.copy(self.path, path)

    def is_modified_on_disk(self) -> bool:
        if self._loaded:
            return get_md5_hash(self._load()) != get_md5_hash(self._loaded)
        else:
            # nothing to compare to
            return False
