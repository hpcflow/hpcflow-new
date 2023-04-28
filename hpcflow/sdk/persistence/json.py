from __future__ import annotations

import copy
from datetime import datetime
import json
from contextlib import contextmanager
from os import PathLike
from pathlib import Path
from pprint import pprint
import shutil
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple, Union

from hpcflow.sdk.core.errors import WorkflowNotFoundError
from hpcflow.sdk.core.utils import bisect_slice, get_md5_hash

from hpcflow.sdk.persistence.base import (
    PersistentStore,
    dropbox_permission_err_retry,
    remove_dir,
    rename_dir,
)


class JSONPersistentStore(PersistentStore):
    """A verbose but inefficient storage backend, to help with understanding and
    debugging."""

    _name = "json"
    _store_path_ext = "json"

    @classmethod
    def _get_store_path(cls, workflow_path):
        name = workflow_path.parts[-1]
        return workflow_path.joinpath(f"{name}.{cls._store_path_ext}")

    @classmethod
    def path_has_store(cls, workflow_path):
        return cls._get_store_path(workflow_path).is_file()

    @property
    def store_path(self):
        return self._get_store_path(self.workflow_path)

    def __init__(self, workflow: Workflow) -> None:
        self._loaded = None  # cache used in `cached_load` context manager
        super().__init__(workflow)

    def exists(self) -> bool:
        # TODO: check these methods
        return self.store_path.is_file()

    @classmethod
    def write_empty_workflow(
        cls,
        template_js: Dict,
        template_components_js: Dict,
        path: Path,
        replaced_dir: Path,
    ) -> None:

        path.mkdir()
        store_path = path.joinpath(f"{path.parts[-1]}.{cls._store_path_ext}")
        dat = {
            "parameter_data": {},
            "parameter_sources": {},
            "template_components": template_components_js,
            "template": template_js,
            "tasks": [],
            "num_added_tasks": 0,
            "loops": [],
            "submissions": [],
        }
        if replaced_dir:
            dat["replaced_dir"] = str(replaced_dir.name)

        cls._dump_to_path(store_path, dat)

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
        with open(self.store_path, "rt") as fp:
            return json.load(fp)

    def load(self) -> Dict:
        # TODO: can we prevent loaded data being modified? this has caused some bugs...
        return self._loaded or self._load()

    @staticmethod
    @dropbox_permission_err_retry
    def _dump_to_path(store_path, data: Dict) -> None:
        with open(store_path, "wt", newline="") as fp:
            json.dump(data, fp, indent=4)

    def _dump(self, wk_data: Dict) -> None:
        self._dump_to_path(self.store_path, wk_data)

    def _add_parameter_data(self, data: Any, source: Dict) -> int:

        idx = len(self.load()["parameter_data"]) + len(self._pending["parameter_data"])

        if data is not None:
            data = self._encode_parameter_data(data["data"])

        self._pending["parameter_data"][idx] = data
        self._pending["parameter_sources"][idx] = dict(sorted(source.items()))
        self.save()

        return idx

    def set_parameter(self, index: int, data: Any) -> None:
        """Set the value of a pre-allocated parameter."""
        if self.is_parameter_set(index):
            raise RuntimeError(f"Parameter at index {index} is already set!")
        self._pending["parameter_data"][index] = self._encode_parameter_data(data)
        self.save()

    def get_parameter_data(self, index: int) -> Tuple[bool, Any]:
        if index in self._pending["parameter_data"]:
            data = self._pending["parameter_data"][index]
        else:
            data = self.load()["parameter_data"][str(index)]
        is_set = False if data is None else True
        data = self._decode_parameter_data(data=data)
        return (is_set, data)

    def get_parameter_source(self, index: int) -> Dict:
        if index in self._pending["parameter_sources"]:
            src = self._pending["parameter_sources"][index]
        else:
            src = self.load()["parameter_sources"][str(index)]

        if index in self._pending["parameter_source_updates"]:
            src.update(self._pending["parameter_source_updates"][index])
            src = dict(sorted(src.items()))

        return src

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

        for (task_idx, _), iters_idx in self._pending["element_iterations_idx"].items():
            for elem_idx, iters_idx_i in iters_idx.items():
                wk_data["tasks"][task_idx]["elements"][elem_idx][
                    "iterations_idx"
                ] += iters_idx_i

        # commit new element iterations:
        for (task_idx, _), element_iters in self._pending["element_iterations"].items():
            wk_data["tasks"][task_idx]["element_iterations"].extend(element_iters)

        # commit new element iteration loop indices:
        for (t_idx, _, iters_idx_i), loop_idx_i in self._pending["loop_idx"].items():
            wk_data["tasks"][t_idx]["element_iterations"][iters_idx_i]["loop_idx"].update(
                loop_idx_i
            )

        # commit new element iteration EARs:
        for (t_idx, _, iters_idx_i), actions_i in self._pending["EARs"].items():
            iter_i = wk_data["tasks"][t_idx]["element_iterations"][iters_idx_i]
            iter_i["actions"].update(actions_i)
            iter_i["EARs_initialised"] = True

        # commit new EAR submission indices:
        for (ins_ID, it_idx, act_idx, rn_idx), sub_idx in self._pending[
            "EAR_submission_idx"
        ].items():
            t_idx = self.get_task_idx_from_insert_ID(ins_ID)
            iter_i = wk_data["tasks"][t_idx]["element_iterations"][it_idx]
            EAR = iter_i["actions"][str(act_idx)][rn_idx]
            EAR["metadata"]["submission_idx"] = sub_idx

        # commit new EAR start times:
        for (ins_ID, it_idx, act_idx, rn_idx), start in self._pending[
            "EAR_start_times"
        ].items():
            t_idx = self.get_task_idx_from_insert_ID(ins_ID)
            iter_i = wk_data["tasks"][t_idx]["element_iterations"][it_idx]
            EAR = iter_i["actions"][str(act_idx)][rn_idx]
            EAR["metadata"]["start_time"] = start.strftime(self.timestamp_format)

        # commit new EAR end times:
        for (ins_ID, it_idx, act_idx, rn_idx), end in self._pending[
            "EAR_end_times"
        ].items():
            t_idx = self.get_task_idx_from_insert_ID(ins_ID)
            iter_i = wk_data["tasks"][t_idx]["element_iterations"][it_idx]
            EAR = iter_i["actions"][str(act_idx)][rn_idx]
            EAR["metadata"]["end_time"] = end.strftime(self.timestamp_format)

        # commit new loops:
        wk_data["template"]["loops"].extend(self._pending["template_loops"])

        # commit new workflow loops:
        wk_data["loops"].extend(self._pending["loops"])

        for loop_idx, num_added_iters in self._pending["loops_added_iters"].items():
            wk_data["loops"][loop_idx]["num_added_iterations"] = num_added_iters

        # commit new submissions:
        wk_data["submissions"].extend(self._pending["submissions"])

        # commit new submission attempts:
        for sub_idx, attempts_i in self._pending["submission_attempts"].items():
            wk_data["submissions"][sub_idx]["submission_attempts"].extend(attempts_i)

        # commit new jobscript scheduler version info:
        for sub_idx, js_vers_info in self._pending["jobscript_version_info"].items():
            for js_idx, vers_info in js_vers_info.items():
                wk_data["submissions"][sub_idx]["jobscripts"][js_idx][
                    "version_info"
                ] = vers_info

        # commit new jobscript job IDs:
        for sub_idx, job_IDs in self._pending["jobscript_job_IDs"].items():
            for js_idx, job_ID in job_IDs.items():
                wk_data["submissions"][sub_idx]["jobscripts"][js_idx][
                    "scheduler_job_ID"
                ] = job_ID

        # commit new jobscript submit times:
        for sub_idx, js_submit_times in self._pending["jobscript_submit_times"].items():
            for js_idx, submit_time in js_submit_times.items():
                wk_data["submissions"][sub_idx]["jobscripts"][js_idx][
                    "submit_time"
                ] = submit_time.strftime(self.timestamp_format)

        # commit new parameters:
        for param_idx, param_dat in self._pending["parameter_data"].items():
            wk_data["parameter_data"][str(param_idx)] = param_dat

        for param_idx, param_src in self._pending["parameter_sources"].items():
            wk_data["parameter_sources"][str(param_idx)] = param_src

        for param_idx, src_update in self._pending["parameter_source_updates"].items():
            src = wk_data["parameter_sources"][str(param_idx)]
            src.update(src_update)
            src = dict(sorted(src.items()))
            wk_data["parameter_sources"][str(param_idx)] = src

        if self._pending["remove_replaced_dir_record"]:
            del wk_data["replaced_dir"]

        self._dump(wk_data)
        self.clear_pending()

    def _get_persistent_template_components(self) -> Dict:
        return self.load()["template_components"]

    def get_template(self) -> Dict:
        # No need to consider pending; this is called once per Workflow object
        return self.load()["template"]

    def get_loops(self) -> List[Dict]:
        # No need to consider pending; this is called once per Workflow object
        return self.load()["loops"]

    def get_submissions(self) -> List[Dict]:
        # No need to consider pending; this is called once per Workflow object
        subs = copy.deepcopy(self.load()["submissions"])

        # cast jobscript submit-times and jobscript `task_elements` keys:
        for sub_idx, sub in enumerate(subs):
            for js_idx, js in enumerate(sub["jobscripts"]):
                if js["submit_time"]:
                    subs[sub_idx]["jobscripts"][js_idx][
                        "submit_time"
                    ] = datetime.strptime(js["submit_time"], self.timestamp_format)

                for key in list(js["task_elements"].keys()):
                    subs[sub_idx]["jobscripts"][js_idx]["task_elements"][int(key)] = subs[
                        sub_idx
                    ]["jobscripts"][js_idx]["task_elements"].pop(key)

        return subs

    def get_num_added_tasks(self) -> int:
        return self.load()["num_added_tasks"] + len(self._pending["tasks"])

    def get_all_tasks_metadata(self) -> List[Dict]:
        # No need to consider pending; this is called once per Workflow object
        return [
            {
                "num_elements": len(task["elements"]),
                "num_element_iterations": len(task["element_iterations"]),
                "num_EARs": sum(
                    len(runs)
                    for iter_i in task["element_iterations"]
                    for runs in iter_i["actions"].values()
                ),
            }
            for task in self.load()["tasks"]
        ]

    def get_task_elements(
        self,
        task_idx: int,
        task_insert_ID: int,
        selection: slice,
        keep_iterations_idx: bool = False,
    ) -> List[Dict]:

        # TODO: add tests to check correct return in various states of pending

        num_pers = self.workflow.tasks[task_idx]._num_elements
        pers_slice, pend_slice = bisect_slice(selection, num_pers)
        pers_range = range(pers_slice.start, pers_slice.stop, pers_slice.step)

        if task_idx in self._pending["tasks"]:
            task_data = self._pending["tasks"][task_idx]
        else:
            task_data = copy.deepcopy(self.load()["tasks"][task_idx])

        if len(pers_range):
            elements = task_data["elements"][pers_slice]
        else:
            elements = []

        key = (task_idx, task_insert_ID)
        if key in self._pending["elements"]:
            elements += copy.deepcopy(self._pending["elements"][key][pend_slice])

        # add iterations:
        sel_range = range(selection.start, selection.stop, selection.step)
        for element_idx, element in zip(sel_range, elements):

            # find which iterations to add:
            iters_idx = element["iterations_idx"]
            if not keep_iterations_idx:
                del element["iterations_idx"]

            # include pending iterations:
            if key in self._pending["element_iterations_idx"]:
                iters_idx += self._pending["element_iterations_idx"][key][element_idx]

            # populate new iterations list:
            element["iterations"] = []
            for iters_idx_i in iters_idx:
                if iters_idx_i + 1 > len(task_data["element_iterations"]):
                    i_pending = iters_idx_i - len(task_data["element_iterations"])
                    iter_i = copy.deepcopy(
                        self._pending["element_iterations"][key][i_pending]
                    )
                else:
                    iter_i = task_data["element_iterations"][iters_idx_i]

                for act_idx_str in list(iter_i["actions"].keys()):
                    runs = iter_i["actions"].pop(act_idx_str)
                    iter_i["actions"][int(act_idx_str)] = runs

                # include pending EARs:
                EARs_key = (task_idx, task_insert_ID, iters_idx_i)
                if EARs_key in self._pending["EARs"]:
                    iter_i["actions"].update(self._pending["EARs"][EARs_key])
                    # if there are pending EARs then EARs must be initialised:
                    iter_i["EARs_initialised"] = True

                # include pending loops:
                loop_idx_key = (task_idx, task_insert_ID, iters_idx_i)
                if loop_idx_key in self._pending["loop_idx"]:
                    iter_i["loop_idx"].update(self._pending["loop_idx"][loop_idx_key])

                iter_i["index"] = iters_idx_i
                element["iterations"].append(iter_i)

            element["index"] = element_idx

        # cast EAR start/end times to datetime types:
        for element in elements:
            element_idx = element["index"]
            for iter_i in element["iterations"]:
                iter_idx = iter_i["index"]
                for act_idx, runs in iter_i["actions"].items():
                    for run_idx in range(len(runs)):
                        run = iter_i["actions"][act_idx][run_idx]
                        start_time = run["metadata"]["start_time"]
                        end_time = run["metadata"]["end_time"]
                        if start_time is not None:
                            run["metadata"]["start_time"] = datetime.strptime(
                                start_time, self.timestamp_format
                            )
                        if end_time is not None:
                            run["metadata"]["end_time"] = datetime.strptime(
                                end_time, self.timestamp_format
                            )

                        # update pending submission indices:
                        key = (task_insert_ID, iter_idx, act_idx, run_idx)
                        if key in self._pending["EAR_submission_idx"]:
                            sub_idx = self._pending["EAR_submission_idx"][key]
                            run["metadata"]["submission_idx"] = sub_idx

        return elements

    def _init_task_loop(
        self,
        task_idx: int,
        task_insert_ID: int,
        element_sel: slice,
        name: str,
    ) -> None:
        """Initialise the zeroth iteration of a named loop for a specified task."""

        elements = self.get_task_elements(
            task_idx=task_idx,
            task_insert_ID=task_insert_ID,
            selection=element_sel,
            keep_iterations_idx=True,
        )

        for element in elements:
            for iter_idx, iter_i in zip(element["iterations_idx"], element["iterations"]):
                if name in iter_i["loop_idx"]:
                    raise ValueError(f"Loop {name!r} already initialised!")
                key = (task_idx, task_insert_ID, iter_idx)
                if key not in self._pending["loop_idx"]:
                    self._pending["loop_idx"][key] = {}
                self._pending["loop_idx"][key].update({name: 0})

    def remove_replaced_dir(self) -> None:
        wk_data = self.load()
        if "replaced_dir" in wk_data:
            remove_dir(Path(wk_data["replaced_dir"]))
            self._pending["remove_replaced_dir_record"] = True
            self.save()

    def reinstate_replaced_dir(self) -> None:
        print(f"reinstate replaced directory!")
        wk_data = self.load()
        if "replaced_dir" in wk_data:
            rename_dir(Path(wk_data["replaced_dir"]), self.workflow_path)

    def copy(self, path: PathLike = None) -> None:
        shutil.copy(self.workflow_path, path)

    def is_modified_on_disk(self) -> Union[bool, Dict]:
        if self._loaded:
            # TODO: need to separate out "metadata" as in zarr store, since this is what
            # we mustn't change during persistent ops; just remove non metadata from the
            # comparison for now:
            on_disk = {
                k: v
                for k, v in self._load().items()
                if k not in ("parameter_data", "tasks")
            }
            in_mem = {
                k: v
                for k, v in self._loaded.items()
                if k not in ("parameter_data", "tasks")
            }
            return get_md5_hash(on_disk) != get_md5_hash(in_mem)
        else:
            # nothing to compare to
            return False

    def get_task_idx_from_insert_ID(self, insert_ID):
        for task in self.workflow.template.tasks:
            if task.insert_ID == insert_ID:
                return task.index
