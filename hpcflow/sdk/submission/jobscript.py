from __future__ import annotations
import copy

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import socket
import subprocess
from textwrap import indent
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
from numpy.typing import NDArray
from hpcflow.sdk import app
from hpcflow.sdk.core.actions import EARStatus
from hpcflow.sdk.core.errors import JobscriptSubmissionFailure, NotSubmitMachineError

from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.jobscript_info import JobscriptElementState
from hpcflow.sdk.submission.schedulers import Scheduler
from hpcflow.sdk.submission.shells import get_shell


def is_jobscript_array(resources, num_elements, store):
    """Return True if a job array should be used for the specified `ElementResources`."""
    if resources.scheduler in ("direct", "direct_posix"):
        if resources.use_job_array:
            raise ValueError(
                f"`use_job_array` not supported by scheduler: {resources.scheduler!r}"
            )
        return False

    run_parallelism = store._features.EAR_parallelism
    if resources.use_job_array is None:
        if num_elements > 1 and run_parallelism:
            return True
        else:
            return False
    else:
        if resources.use_job_array and not run_parallelism:
            raise ValueError(
                f"Store type {store!r} does not support element parallelism, so jobs "
                f"cannot be submitted as scheduler arrays."
            )
        return resources.use_job_array


@TimeIt.decorator
def generate_EAR_resource_map(
    task: app.WorkflowTask,
    loop_idx: Dict,
    cache,
) -> Tuple[List[app.ElementResources], List[int], NDArray, NDArray]:
    """Generate an integer array whose rows represent actions and columns represent task
    elements and whose values index unique resources."""
    # TODO: assume single iteration for now; later we will loop over Loop tasks for each
    # included task and call this func with specific loop indices
    none_val = -1
    resources = []
    resource_hashes = []

    arr_shape = (task.num_actions, task.num_elements)
    resource_map = np.empty(arr_shape, dtype=int)
    EAR_ID_map = np.empty(arr_shape, dtype=int)
    resource_map[:] = none_val
    EAR_ID_map[:] = none_val

    for elem_id in task.element_IDs:
        element = cache.elements[elem_id]
        for iter_ID_i in element.iteration_IDs:
            iter_i = cache.iterations[iter_ID_i]
            if iter_i.loop_idx != loop_idx:
                continue
            if iter_i.EARs_initialised:  # not strictly needed (actions will be empty)
                for act_idx, action in iter_i.actions.items():
                    for run in action.runs:
                        if run.status == EARStatus.pending:
                            # TODO: consider `time_limit`s
                            res_hash = run.resources.get_jobscript_hash()
                            if res_hash not in resource_hashes:
                                resource_hashes.append(res_hash)
                                resources.append(run.resources)
                            resource_map[act_idx][element.index] = resource_hashes.index(
                                res_hash
                            )
                            EAR_ID_map[act_idx, element.index] = run.id_

    # set defaults for and validate unique resources:
    for res in resources:
        res.set_defaults()
        res.validate_against_machine()

    return (
        resources,
        resource_hashes,
        resource_map,
        EAR_ID_map,
    )


@TimeIt.decorator
def group_resource_map_into_jobscripts(
    resource_map: Union[List, NDArray],
    none_val: Any = -1,
):
    resource_map = np.asanyarray(resource_map)
    resource_idx = np.unique(resource_map)
    jobscripts = []
    allocated = np.zeros_like(resource_map)
    js_map = np.ones_like(resource_map, dtype=float) * np.nan
    nones_bool = resource_map == none_val
    stop = False
    for act_idx in range(resource_map.shape[0]):
        for res_i in resource_idx:
            if res_i == none_val:
                continue

            if res_i not in resource_map[act_idx]:
                continue

            resource_map[nones_bool] = res_i
            diff = np.cumsum(np.abs(np.diff(resource_map[act_idx:], axis=0)), axis=0)

            elem_bool = np.logical_and(
                resource_map[act_idx] == res_i, allocated[act_idx] == False
            )
            elem_idx = np.where(elem_bool)[0]
            act_elem_bool = np.logical_and(elem_bool, nones_bool[act_idx] == False)
            act_elem_idx = np.where(act_elem_bool)

            # add elements from downstream actions:
            ds_bool = np.logical_and(
                diff[:, elem_idx] == 0,
                nones_bool[act_idx + 1 :, elem_idx] == False,
            )
            ds_act_idx, ds_elem_idx = np.where(ds_bool)
            ds_act_idx += act_idx + 1
            ds_elem_idx = elem_idx[ds_elem_idx]

            EARs_by_elem = {k.item(): [act_idx] for k in act_elem_idx[0]}
            for ds_a, ds_e in zip(ds_act_idx, ds_elem_idx):
                ds_e_item = ds_e.item()
                if ds_e_item not in EARs_by_elem:
                    EARs_by_elem[ds_e_item] = []
                EARs_by_elem[ds_e_item].append(ds_a.item())

            EARs = np.vstack([np.ones_like(act_elem_idx) * act_idx, act_elem_idx])
            EARs = np.hstack([EARs, np.array([ds_act_idx, ds_elem_idx])])

            if not EARs.size:
                continue

            js = {
                "resources": res_i,
                "elements": dict(sorted(EARs_by_elem.items(), key=lambda x: x[0])),
            }
            allocated[EARs[0], EARs[1]] = True
            js_map[EARs[0], EARs[1]] = len(jobscripts)
            jobscripts.append(js)

            if np.all(allocated[~nones_bool]):
                stop = True
                break

        if stop:
            break

    resource_map[nones_bool] = none_val

    return jobscripts, js_map


@TimeIt.decorator
def resolve_jobscript_dependencies(jobscripts, element_deps):
    # first pass is to find the mappings between jobscript elements:
    jobscript_deps = {}
    for js_idx, elem_deps in element_deps.items():
        # keys of new dict are other jobscript indices on which this jobscript (js_idx)
        # depends:
        jobscript_deps[js_idx] = {}

        for js_elem_idx_i, EAR_deps_i in elem_deps.items():
            # locate which jobscript elements this jobscript element depends on:
            for EAR_dep_j in EAR_deps_i:
                for js_k_idx, js_k in jobscripts.items():
                    if js_k_idx == js_idx:
                        break

                    if EAR_dep_j in js_k["EAR_ID"]:
                        if js_k_idx not in jobscript_deps[js_idx]:
                            jobscript_deps[js_idx][js_k_idx] = {"js_element_mapping": {}}

                        if (
                            js_elem_idx_i
                            not in jobscript_deps[js_idx][js_k_idx]["js_element_mapping"]
                        ):
                            jobscript_deps[js_idx][js_k_idx]["js_element_mapping"][
                                js_elem_idx_i
                            ] = []

                        # retrieve column index, which is the JS-element index:
                        js_elem_idx_k = np.where(
                            np.any(js_k["EAR_ID"] == EAR_dep_j, axis=0)
                        )[0][0].item()

                        # add js dependency element-mapping:
                        if (
                            js_elem_idx_k
                            not in jobscript_deps[js_idx][js_k_idx]["js_element_mapping"][
                                js_elem_idx_i
                            ]
                        ):
                            jobscript_deps[js_idx][js_k_idx]["js_element_mapping"][
                                js_elem_idx_i
                            ].append(js_elem_idx_k)

    # next we can determine if two jobscripts have a one-to-one element mapping, which
    # means they can be submitted with a "job array" dependency relationship:
    for js_i_idx, deps_i in jobscript_deps.items():
        for js_k_idx, deps_j in deps_i.items():
            # is this an array dependency?

            js_i_num_js_elements = jobscripts[js_i_idx]["EAR_ID"].shape[1]
            js_k_num_js_elements = jobscripts[js_k_idx]["EAR_ID"].shape[1]

            is_all_i_elems = list(
                sorted(set(deps_j["js_element_mapping"].keys()))
            ) == list(range(js_i_num_js_elements))

            is_all_k_single = set(
                len(i) for i in deps_j["js_element_mapping"].values()
            ) == {1}

            is_all_k_elems = list(
                sorted(i[0] for i in deps_j["js_element_mapping"].values())
            ) == list(range(js_k_num_js_elements))

            is_arr = is_all_i_elems and is_all_k_single and is_all_k_elems
            jobscript_deps[js_i_idx][js_k_idx]["is_array"] = is_arr

    return jobscript_deps


@TimeIt.decorator
def merge_jobscripts_across_tasks(jobscripts: Dict[int, Dict]) -> Dict[int, Dict]:
    """Try to merge jobscripts between tasks.

    This is possible if two jobscripts share the same resources and have an array
    dependency (i.e. one-to-one element dependency mapping).

    """

    for js_idx, js in jobscripts.items():
        # for now only attempt to merge a jobscript with a single dependency:

        if not js["dependencies"]:
            continue

        closest_idx = max(js["dependencies"])
        closest_js = jobscripts[closest_idx]
        other_deps = {k: v for k, v in js["dependencies"].items() if k != closest_idx}

        # if all `other_deps` are also found within `closest_js`'s dependencies, then we
        # can merge `js` into `closest_js`:
        merge = True
        for dep_idx, dep_i in other_deps.items():
            try:
                if closest_js["dependencies"][dep_idx] != dep_i:
                    merge = False
            except KeyError:
                merge = False

        if merge:
            js_j = closest_js  # the jobscript we are merging `js` into
            js_j_idx = closest_idx
            dep_info = js["dependencies"][js_j_idx]

            # can only merge if resources are the same and is array dependency:
            if js["resource_hash"] == js_j["resource_hash"] and dep_info["is_array"]:
                num_loop_idx = len(
                    js_j["task_loop_idx"]
                )  # TODO: should this be: `js_j["task_loop_idx"][0]`?

                # append task_insert_IDs
                js_j["task_insert_IDs"].append(js["task_insert_IDs"][0])
                js_j["task_loop_idx"].append(js["task_loop_idx"][0])

                add_acts = []
                for t_act in js["task_actions"]:
                    t_act = copy.copy(t_act)
                    t_act[2] += num_loop_idx
                    add_acts.append(t_act)

                js_j["task_actions"].extend(add_acts)
                for k, v in js["task_elements"].items():
                    js_j["task_elements"][k].extend(v)

                # append to elements and elements_idx list
                js_j["EAR_ID"] = np.vstack((js_j["EAR_ID"], js["EAR_ID"]))

                # mark this js as defunct
                js["is_merged"] = True

                # update dependencies of any downstream jobscripts that refer to this js
                for ds_js_idx, ds_js in jobscripts.items():
                    if ds_js_idx <= js_idx:
                        continue
                    for dep_k_js_idx in list(ds_js["dependencies"].keys()):
                        if dep_k_js_idx == js_idx:
                            jobscripts[ds_js_idx]["dependencies"][js_j_idx] = ds_js[
                                "dependencies"
                            ].pop(dep_k_js_idx)

    # remove is_merged jobscripts:
    jobscripts = {k: v for k, v in jobscripts.items() if "is_merged" not in v}

    return jobscripts


@TimeIt.decorator
def resolve_jobscript_blocks(jobscripts: Dict[int, Dict]) -> Dict[int, Dict]:
    """For contiguous, dependent, non-array jobscripts with identical resource
    requirements, combine into multi-block jobscripts.

    Parameters
    ----------
    jobscripts
        Dict whose values must be dicts with keys "is_array", "resource_hash" and
        "dependencies".
    run_parallelism
        True if the store supports run parallelism

    """
    js_new = []
    new_idx = {}  # track new positions by new jobscript index and block index
    prev_hash = None
    blocks = []
    for js_idx, js_i in jobscripts.items():

        new_deps_js_j = {new_idx[i][0] for i in js_i["dependencies"]}

        if js_i["is_array"]:
            # array jobs cannot be merged into the same jobscript

            # append existing block:
            if blocks:
                js_new.append(blocks)
                prev_hash = None
                blocks = []

            new_idx[js_idx] = (len(js_new), 0)
            js_new.append([js_i])
            continue

        if js_idx == 0 or prev_hash is None:
            # (note: zeroth index will always exist)

            # start a new block:
            blocks.append(js_i)
            new_idx[js_idx] = (len(js_new), len(blocks) - 1)

            # set resource hash to compare with the next jobscript
            prev_hash = js_i["resource_hash"]

        elif js_i["resource_hash"] == prev_hash and new_deps_js_j == {len(js_new)}:
            # merge with previous jobscript by adding another block
            # only merge if all dependencies are part of the same (current) jobscript
            blocks.append(js_i)
            new_idx[js_idx] = (len(js_new), len(blocks) - 1)

        else:
            # cannot merge, append the new jobscript data:
            js_new.append(blocks)

            # start a new block:
            blocks = [js_i]
            new_idx[js_idx] = (len(js_new), len(blocks) - 1)

            # set resource hash to compare with the next jobscript
            prev_hash = js_i["resource_hash"]

    # append remaining blocks:
    if blocks:
        js_new.append(blocks)
        prev_hash = None
        blocks = []

    # re-index dependencies:
    for js_i_idx, js_i in enumerate(js_new):

        resources = None
        is_array = None
        for block_j in js_i:
            for k, v in new_idx.items():
                dep_data = block_j["dependencies"].pop(k, None)
                if dep_data:
                    block_j["dependencies"][v] = dep_data

            del block_j["resource_hash"]
            resources = block_j.pop("resources", None)
            is_array = block_j.pop("is_array")

        js_new[js_i_idx] = {
            "resources": resources,
            "is_array": is_array,
            "blocks": js_new[js_i_idx],
        }

    return js_new


class JobscriptBlock(JSONLike):
    """A rectangular block of element-actions to run within a jobscript.

    Attributes
    ----------
    task_actions
        (task insert ID, action_idx, index into task_loop_idx) for each JS_ACTION_IDX
    task_elements
        JS_ELEMENT_IDX: [TASK_ELEMENT_IDX for each TASK_INSERT_ID]

    """

    def __init__(
        self,
        task_insert_IDs: List[int],
        task_actions: List[Tuple],
        task_elements: Dict[int, List[int]],
        EAR_ID: NDArray,
        task_loop_idx: List[Dict],
        dependencies: Dict[int, Dict],
        jobscript: Optional[Any] = None,
    ):
        self.jobscript = jobscript
        self._task_insert_IDs = task_insert_IDs
        self._task_actions = task_actions
        self._task_elements = task_elements
        self._task_loop_idx = task_loop_idx
        self._EAR_ID = EAR_ID
        self._dependencies = dependencies

        self._all_EARs = None  # assigned on first access to `all_EARs` property

    @property
    def task_insert_IDs(self):
        return self._task_insert_IDs

    @property
    def task_actions(self):
        return self._task_actions

    @property
    def task_elements(self):
        return self._task_elements

    @property
    def EAR_ID(self):
        return self._EAR_ID

    @property
    def task_loop_idx(self):
        return self._task_loop_idx

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def num_actions(self):
        return self.EAR_ID.shape[0]

    @property
    def num_elements(self):
        return self.EAR_ID.shape[1]

    @property
    def workflow(self):
        return self.jobscript.workflow

    @property
    @TimeIt.decorator
    def all_EARs(self) -> List:
        if not self._all_EARs:
            self._all_EARs = [i for i in self.jobscript.all_EARs if i.id_ in self.EAR_ID]
        return self._all_EARs

    def to_dict(self):
        dct = super().to_dict()
        del dct["_all_EARs"]
        dct["_dependencies"] = [[list(k), v] for k, v in self.dependencies.items()]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        dct["EAR_ID"] = dct["EAR_ID"].tolist()
        return dct

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        json_like["EAR_ID"] = np.array(json_like["EAR_ID"])
        json_like["dependencies"] = {tuple(i[0]): i[1] for i in json_like["dependencies"]}
        return super().from_json_like(json_like, shared_data)

    def _get_EARs_arr(self):
        return np.array(self.all_EARs).reshape(self.EAR_ID.shape)

    def get_task_loop_idx_array(self):
        loop_idx = np.empty_like(self.EAR_ID)
        loop_idx[:] = np.array([i[2] for i in self.task_actions]).reshape(
            (len(self.task_actions), 1)
        )
        return loop_idx

    @TimeIt.decorator
    def make_artifact_dirs(self):
        # TODO: consider using run.get_directory() instead for EAR_dir? whatever is faster
        EARs_arr = self._get_EARs_arr()
        task_loop_idx_arr = self.get_task_loop_idx_array()

        run_dirs = []
        for js_elem_idx in range(self.num_elements):
            run_dirs_i = []
            for js_act_idx in range(self.num_actions):
                EAR_i = EARs_arr[js_act_idx, js_elem_idx]
                t_iID = EAR_i.task.insert_ID
                l_idx = task_loop_idx_arr[js_act_idx, js_elem_idx].item()
                r_idx = EAR_i.index

                loop_idx_i = self.task_loop_idx[l_idx]
                task_dir = self.workflow.tasks.get(insert_ID=t_iID).get_dir_name(
                    loop_idx_i
                )
                elem_dir = EAR_i.element.dir_name
                run_dir = f"r_{r_idx}"

                EAR_dir = Path(self.workflow.execution_path, task_dir, elem_dir, run_dir)
                EAR_dir.mkdir(exist_ok=True, parents=True)

                # copy (TODO: optionally symlink) any input files:
                for name, path in EAR_i.get("input_files", {}).items():
                    if path:
                        shutil.copy(path, EAR_dir)

                run_dirs_i.append(EAR_dir.relative_to(self.workflow.path))

            run_dirs.append(run_dirs_i)

        return run_dirs

    @TimeIt.decorator
    def write_EAR_ID_file(self, fp):
        """Write a text file with `num_elements` lines and `num_actions` delimited tokens
        per line, representing whether a given EAR must be executed."""
        # can't specify "open" newline if we pass the file name only, so pass handle:
        np.savetxt(
            fname=fp,
            X=(self.EAR_ID).T,
            fmt="%.0f",
            delimiter=self.jobscript._EAR_files_delimiter,
        )


class Jobscript(JSONLike):
    _app_attr = "app"
    _EAR_files_delimiter = ":"
    _workflow_app_alias = "wkflow_app"

    _child_objects = (
        ChildObjectSpec(
            name="resources",
            class_name="ElementResources",
        ),
        ChildObjectSpec(
            name="blocks",
            class_name="JobscriptBlock",
            is_multiple=True,
            parent_ref="jobscript",
        ),
    )

    def __init__(
        self,
        index: int,
        is_array: bool,
        resources: app.ElementResources,
        blocks: List,
        submit_time: Optional[datetime] = None,
        submit_hostname: Optional[str] = None,
        submit_machine: Optional[str] = None,
        submit_cmdline: Optional[str] = None,
        scheduler_job_ID: Optional[str] = None,
        process_ID: Optional[int] = None,
        version_info: Optional[Tuple[str]] = None,
        os_name: Optional[str] = None,
        shell_name: Optional[str] = None,
        scheduler_name: Optional[str] = None,
    ):

        if not isinstance(blocks[0], JobscriptBlock):
            blocks = [JobscriptBlock(**i, jobscript=self) for i in blocks]

        self._index = index
        self._blocks = blocks
        self._is_array = is_array
        self._resources = resources

        # assigned on parent `Submission.submit` (or retrieved form persistent store):
        self._submit_time = submit_time
        self._submit_hostname = submit_hostname
        self._submit_machine = submit_machine
        self._submit_cmdline = submit_cmdline

        self._scheduler_job_ID = scheduler_job_ID
        self._process_ID = process_ID
        self._version_info = version_info

        # assigned as submit-time:
        # TODO: these should now always be set in `resources` so shouldn't need these:
        self._os_name = os_name
        self._shell_name = shell_name
        self._scheduler_name = scheduler_name

        self._submission = None  # assigned by parent Submission

        self._scheduler_obj = None  # assigned on first access to `scheduler` property
        self._shell_obj = None  # assigned on first access to `shell` property
        self._submit_time_obj = None  # assigned on first access to `submit_time` property
        self._all_EARs = None  # assigned on first access to `all_EARs` property

        self._set_parent_refs()

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"index={self.index!r}, "
            f"blocks={self.blocks!r}, "
            f"resources={self.resources!r}, "
            f")"
        )

    def to_dict(self):
        dct = super().to_dict()
        del dct["_scheduler_obj"]
        del dct["_shell_obj"]
        del dct["_submit_time_obj"]
        del dct["_all_EARs"]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        return dct

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        return super().from_json_like(json_like, shared_data)

    @property
    def workflow_app_alias(self):
        return self._workflow_app_alias

    def get_commands_file_name(self, block_act_key: Tuple[int, int, int], shell=None):
        return self.app.RunDirAppFiles.get_commands_file_name(
            block_act_key,
            shell=shell or self.shell,
        )

    @property
    def blocks(self):
        return self._blocks

    @property
    def all_EAR_IDs(self) -> NDArray:
        all_EAR_IDs = np.concatenate([i.EAR_ID.flatten() for i in self.blocks])
        return all_EAR_IDs

    @property
    @TimeIt.decorator
    def all_EARs(self) -> List:
        if not self._all_EARs:
            self._all_EARs = self.workflow.get_EARs_from_IDs(self.all_EAR_IDs)
        return self._all_EARs

    @property
    def resources(self):
        return self._resources

    @property
    def dependencies(self) -> Dict[int, Dict]:
        deps = {}
        for block in self.blocks:
            for k, v in block.dependencies.items():
                if k[0] == self.index:
                    # block dependency is internal to this jobscript
                    continue
                else:
                    deps[k] = {"is_array": v["is_array"]}
        return deps

    @property
    @TimeIt.decorator
    def start_time(self):
        """Get the first start time from all EARs."""
        if not self.is_submitted:
            return
        all_times = [i.start_time for i in self.all_EARs if i.start_time]
        if all_times:
            return min(all_times)
        else:
            return None

    @property
    @TimeIt.decorator
    def end_time(self):
        """Get the last end time from all EARs."""
        if not self.is_submitted:
            return
        all_times = [i.end_time for i in self.all_EARs if i.end_time]
        if all_times:
            return max(all_times)
        else:
            return None

    @property
    def submit_time(self):
        if self._submit_time_obj is None and self._submit_time:
            self._submit_time_obj = (
                datetime.strptime(self._submit_time, self.workflow.ts_fmt)
                .replace(tzinfo=timezone.utc)
                .astimezone()
            )
        return self._submit_time_obj

    @property
    def submit_hostname(self):
        return self._submit_hostname

    @property
    def submit_machine(self):
        return self._submit_machine

    @property
    def submit_cmdline(self):
        return self._submit_cmdline

    @property
    def scheduler_job_ID(self):
        return self._scheduler_job_ID

    @property
    def process_ID(self):
        return self._process_ID

    @property
    def version_info(self):
        return self._version_info

    @property
    def index(self):
        return self._index

    @property
    def submission(self):
        return self._submission

    @property
    def workflow(self):
        return self.submission.workflow

    @property
    def is_array(self):
        return self._is_array

    @property
    def os_name(self) -> Union[str, None]:
        return self._os_name or self.resources.os_name

    @property
    def shell_name(self) -> Union[str, None]:
        return self._shell_name or self.resources.shell

    @property
    def scheduler_name(self) -> Union[str, None]:
        return self._scheduler_name or self.resources.scheduler

    def _get_submission_os_args(self):
        return {"linux_release_file": self.app.config.linux_release_file}

    def _get_submission_shell_args(self):
        return self.resources.shell_args

    def _get_submission_scheduler_args(self):
        return self.resources.scheduler_args

    def _get_shell(self, os_name, shell_name, os_args=None, shell_args=None):
        """Get an arbitrary shell, not necessarily associated with submission."""
        os_args = os_args or {}
        shell_args = shell_args or {}
        return get_shell(
            shell_name=shell_name,
            os_name=os_name,
            os_args=os_args,
            **shell_args,
        )

    @property
    def shell(self):
        """Retrieve the shell object for submission."""
        if self._shell_obj is None:
            self._shell_obj = self._get_shell(
                os_name=self.os_name,
                shell_name=self.shell_name,
                os_args=self._get_submission_os_args(),
                shell_args=self._get_submission_shell_args(),
            )
        return self._shell_obj

    @property
    def scheduler(self):
        """Retrieve the scheduler object for submission."""
        if self._scheduler_obj is None:
            self._scheduler_obj = self.app.get_scheduler(
                scheduler_name=self.scheduler_name,
                os_name=self.os_name,
                scheduler_args=self._get_submission_scheduler_args(),
            )
        return self._scheduler_obj

    @property
    def EAR_ID_file_name(self):
        return f"js_{self.index}_EAR_IDs.txt"

    @property
    def direct_stdout_file_name(self):
        """For direct execution stdout."""
        return f"js_{self.index}_stdout.log"

    @property
    def direct_stderr_file_name(self):
        """For direct execution stderr."""
        return f"js_{self.index}_stderr.log"

    @property
    def direct_win_pid_file_name(self):
        return f"js_{self.index}_pid.txt"

    @property
    def jobscript_name(self):
        return f"js_{self.index}{self.shell.JS_EXT}"

    @property
    def jobscript_functions_name(self):
        return f"js_funcs_{self.index}{self.shell.JS_EXT}"

    @property
    def EAR_ID_file_path(self):
        return self.submission.path / self.EAR_ID_file_name

    @property
    def jobscript_path(self):
        return self.submission.path / self.jobscript_name

    @property
    def jobscript_functions_path(self):
        return self.submission.path / self.jobscript_functions_name

    @property
    def direct_stdout_path(self):
        return self.submission.path / self.direct_stdout_file_name

    @property
    def direct_stderr_path(self):
        return self.submission.path / self.direct_stderr_file_name

    @property
    def direct_win_pid_file_path(self):
        return self.submission.path / self.direct_win_pid_file_name

    @property
    def is_scheduled(self) -> bool:
        return self.scheduler_name not in ("direct", "direct_posix")

    def _set_submit_time(self, submit_time: datetime) -> None:
        submit_time = submit_time.strftime(self.workflow.ts_fmt)
        self._submit_time = submit_time
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_time=submit_time,
        )

    def _set_submit_hostname(self, submit_hostname: str) -> None:
        self._submit_hostname = submit_hostname
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_hostname=submit_hostname,
        )

    def _set_submit_machine(self, submit_machine: str) -> None:
        self._submit_machine = submit_machine
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_machine=submit_machine,
        )

    def _set_submit_cmdline(self, submit_cmdline: List[str]) -> None:
        self._submit_cmdline = submit_cmdline
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_cmdline=submit_cmdline,
        )

    def _set_scheduler_job_ID(self, job_ID: str) -> None:
        """For scheduled submission only."""
        self._scheduler_job_ID = job_ID
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            scheduler_job_ID=job_ID,
        )

    def _set_process_ID(self, process_ID: str) -> None:
        """For direct submission only."""
        self._process_ID = process_ID
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            process_ID=process_ID,
        )

    def _set_version_info(self, version_info: Dict) -> None:
        self._version_info = version_info
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            version_info=version_info,
        )

    def _set_os_name(self) -> None:
        """Set the OS name for this jobscript. This is invoked at submit-time."""
        self._os_name = self.resources.os_name
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            os_name=self._os_name,
        )

    def _set_shell_name(self) -> None:
        """Set the shell name for this jobscript. This is invoked at submit-time."""
        self._shell_name = self.resources.shell
        self.workflow._store.set_jobscript_metadata(
            sub_idx=self.submission.index,
            js_idx=self.index,
            shell_name=self._shell_name,
        )

    def _set_scheduler_name(self) -> None:
        """Set the scheduler name for this jobscript. This is invoked at submit-time."""
        self._scheduler_name = self.resources.scheduler
        if self._scheduler_name:
            self.workflow._store.set_jobscript_metadata(
                sub_idx=self.submission.index,
                js_idx=self.index,
                scheduler_name=self._scheduler_name,
            )

    @TimeIt.decorator
    def compose_jobscript(
        self,
        shell,
        deps: Optional[Dict] = None,
        os_name: str = None,
        scheduler_name: Optional[str] = None,
        scheduler_args: Optional[Dict] = None,
    ) -> str:
        """Prepare the jobscript file string."""
        scheduler_name = scheduler_name or self.scheduler_name
        scheduler = self.app.get_scheduler(
            scheduler_name=scheduler_name,
            os_name=os_name,
            scheduler_args=scheduler_args or self._get_submission_scheduler_args(),
        )
        app_caps = self.app.package_name.upper()
        header_args = {
            "app_caps": app_caps,
            "jobscript_functions_path": self.jobscript_functions_name,
            "sub_idx": self.submission.index,
            "js_idx": self.index,
            "EAR_file_name": self.EAR_ID_file_name,
            "tmp_dir_name": self.submission.TMP_DIR_NAME,
            "log_dir_name": self.submission.LOG_DIR_NAME,
            "std_dir_name": self.submission.STD_DIR_NAME,
            "scripts_dir_name": self.submission.SCRIPTS_DIR_NAME,
        }

        shebang = shell.JS_SHEBANG.format(
            shebang_executable=" ".join(shell.shebang_executable),
            shebang_args=scheduler.shebang_args,
        )
        header = shell.JS_HEADER.format(**header_args)

        if self.is_scheduled:
            header = shell.JS_SCHEDULER_HEADER.format(
                shebang=shebang,
                scheduler_options=scheduler.format_options(
                    resources=self.resources,
                    num_elements=self.blocks[0].num_elements,  # only used for array jobs
                    is_array=self.is_array,
                    sub_idx=self.submission.index,
                ),
                header=header,
            )
        else:
            # the NullScheduler (direct submission)
            wait_cmd = shell.get_wait_command(
                workflow_app_alias=self.workflow_app_alias,
                sub_idx=self.submission.index,
                deps=deps,
            )
            header = shell.JS_DIRECT_HEADER.format(
                shebang=shebang,
                header=header,
                workflow_app_alias=self.workflow_app_alias,
                wait_command=wait_cmd,
            )

        out = header
        run_cmd = shell.JS_RUN_CMD.format(workflow_app_alias=self.workflow_app_alias)
        block_run = shell.JS_RUN.format(
            EAR_files_delimiter=self._EAR_files_delimiter,
            app_caps=app_caps,
            run_cmd=run_cmd,
            sub_tmp_dir=self.submission.tmp_path,
        )
        if len(self.blocks) == 1:
            # forgo element and action loops if not necessary:
            block = self.blocks[0]
            if block.num_actions > 1:
                block_act = shell.JS_ACT_MULTI.format(
                    num_actions=block.num_actions,
                    run_block=indent(block_run, shell.JS_INDENT),
                )
            else:
                block_act = shell.JS_ACT_SINGLE.format(run_block=block_run)

            main = shell.JS_MAIN.format(
                action=block_act,
                app_caps=app_caps,
                block_start_elem_idx=0,
            )

            out += shell.JS_BLOCK_HEADER.format(app_caps=app_caps)
            if self.is_array:
                out += shell.JS_ELEMENT_MULTI_ARRAY.format(
                    scheduler_command=scheduler.js_cmd,
                    scheduler_array_switch=scheduler.array_switch,
                    scheduler_array_item_var=scheduler.array_item_var,
                    num_elements=block.num_elements,
                    main=main,
                )
            elif block.num_elements == 1:
                out += shell.JS_ELEMENT_SINGLE.format(
                    block_start_elem_idx=0,
                    main=main,
                )
            else:
                out += shell.JS_ELEMENT_MULTI_LOOP.format(
                    block_start_elem_idx=0,
                    num_elements=block.num_elements,
                    main=indent(main, shell.JS_INDENT),
                )

        else:
            # use a shell loop for blocks, so always write the inner element and action
            # loops:
            block_act = shell.JS_ACT_MULTI.format(
                num_actions=shell.format_array_get_item("num_actions", "$block_idx"),
                run_block=indent(block_run, shell.JS_INDENT),
            )
            main = shell.JS_MAIN.format(
                action=block_act,
                app_caps=app_caps,
                block_start_elem_idx="$block_start_elem_idx",
            )

            # only non-array jobscripts will have multiple blocks:
            element_loop = shell.JS_ELEMENT_MULTI_LOOP.format(
                block_start_elem_idx="$block_start_elem_idx",
                num_elements=shell.format_array_get_item("num_elements", "$block_idx"),
                main=indent(main, shell.JS_INDENT),
            )
            out += shell.JS_BLOCK_LOOP.format(
                num_elements=shell.format_array([i.num_elements for i in self.blocks]),
                num_actions=shell.format_array([i.num_actions for i in self.blocks]),
                num_blocks=len(self.blocks),
                app_caps=app_caps,
                element_loop=indent(element_loop, shell.JS_INDENT),
            )

        out += shell.JS_FOOTER

        return out

    def compose_functions_file(self, shell):
        # TODO: refactor with write_jobscript

        cfg_invocation = self.app.config._file.get_invocation(self.app.config._config_key)
        env_setup = cfg_invocation["environment_setup"]
        if env_setup:
            env_setup = indent(env_setup.strip(), shell.JS_ENV_SETUP_INDENT)
            env_setup += "\n\n" + shell.JS_ENV_SETUP_INDENT
        else:
            env_setup = shell.JS_ENV_SETUP_INDENT
        app_invoc = list(self.app.run_time_info.invocation_command)

        app_caps = self.app.package_name.upper()
        func_file_args = shell.process_JS_header_args(  # TODO: rename?
            {
                "workflow_app_alias": self.workflow_app_alias,
                "env_setup": env_setup,
                "app_invoc": app_invoc,
                "app_caps": app_caps,
                "config_dir": str(self.app.config.config_directory),
                "config_invoc_key": self.app.config.config_key,
            }
        )
        out = shell.JS_FUNCS.format(**func_file_args)
        return out

    @TimeIt.decorator
    def write_jobscript(
        self,
        os_name: str = None,
        shell_name: str = None,
        deps: Optional[Dict] = None,
        os_args: Optional[Dict] = None,
        shell_args: Optional[Dict] = None,
        scheduler_name: Optional[str] = None,
        scheduler_args: Optional[Dict] = None,
    ):
        os_name = os_name or self.os_name
        shell_name = shell_name or self.shell_name
        if not os_name:
            raise RuntimeError(
                f"Jobscript {self.index} `os_name` is not yet set. Pass the `os_name` as "
                f"a method argument to compose the jobscript for a given `os_name`."
            )
        if not shell_name:
            raise RuntimeError(
                f"Jobscript {self.index} `shell_name` is not yet set. Pass the "
                f"`shell_name` as a method argument to compose the jobscript for a given "
                f"`shell_name`."
            )
        shell = self._get_shell(
            os_name=os_name,
            shell_name=shell_name,
            os_args=os_args or self._get_submission_os_args(),
            shell_args=shell_args or self._get_submission_shell_args(),
        )

        js_str = self.compose_jobscript(
            deps=deps,
            shell=shell,
            os_name=os_name,
            scheduler_name=scheduler_name,
            scheduler_args=scheduler_args,
        )
        js_funcs_str = self.compose_functions_file(shell)

        with self.jobscript_path.open("wt", newline="\n") as fp:
            fp.write(js_str)

        with self.jobscript_functions_path.open("wt", newline="\n") as fp:
            fp.write(js_funcs_str)

        return self.jobscript_path

    @TimeIt.decorator
    def _launch_direct_js_win(self):
        # this is a "trick" to ensure we always get a fully detached new process (with no
        # parent); the `powershell.exe -Command` process exits after running the inner
        # `Start-Process`, which is where the jobscript is actually invoked. I could not
        # find a way using `subprocess.Popen()` to ensure the new process was fully
        # detached when submitting jobscripts via a Jupyter notebook in Windows.

        # Note we need powershell.exe for this "launcher process", but the shell used for
        # the jobscript itself need not be powershell.exe
        exe_path, arg_list = self.submit_cmdline[0], self.submit_cmdline[1:]

        # note powershell-escaped quotes, in case of spaces in arguments (this seems to
        # work okay even though we might have switch like arguments in this list, like
        # "-File"):
        arg_list_str = ",".join(f'"`"{i}`""' for i in arg_list)

        args = [
            "powershell.exe",
            "-Command",
            (
                f"$JS_proc = Start-Process "
                f'-Passthru -NoNewWindow -FilePath "{exe_path}" '
                f'-RedirectStandardOutput "{self.direct_stdout_path}" '
                f'-RedirectStandardError "{self.direct_stderr_path}" '
                f'-WorkingDirectory "{self.workflow.path}" '
                f"-ArgumentList {arg_list_str}; "
                f'Set-Content -Path "{self.direct_win_pid_file_path}" -Value $JS_proc.Id'
            ),
        ]

        self.app.submission_logger.info(
            f"running direct Windows jobscript launcher process: {args!r}"
        )
        # for some reason we still need to create a "detached" process here as well:
        init_proc = subprocess.Popen(
            args=args,
            cwd=str(self.workflow.path),
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        init_proc.wait()  # wait for the process ID file to be written
        process_ID = int(self.direct_win_pid_file_path.read_text())
        return process_ID

    @TimeIt.decorator
    def _launch_direct_js_posix(self) -> int:
        # direct submission; submit jobscript asynchronously:
        # detached process, avoid interrupt signals propagating to the subprocess:
        with self.direct_stdout_path.open("wt") as fp_stdout:
            with self.direct_stderr_path.open("wt") as fp_stderr:
                # note: Popen copies the file objects, so this works!
                proc = subprocess.Popen(
                    args=self.submit_cmdline,
                    stdout=fp_stdout,
                    stderr=fp_stderr,
                    cwd=str(self.workflow.path),
                    start_new_session=True,
                )
                process_ID = proc.pid

        return process_ID

    @TimeIt.decorator
    def submit(
        self,
        scheduler_refs: Dict[int, (str, bool)],
        print_stdout: Optional[bool] = False,
    ) -> str:
        # map each dependency jobscript index to the JS ref (job/process ID) and if the
        # dependency is an array dependency:
        deps = {}
        for (js_idx, _), deps_i in self.dependencies.items():
            dep_js_ref, dep_js_is_arr = scheduler_refs[js_idx]
            # only submit an array dependency if both this jobscript and the dependency
            # are array jobs:
            dep_is_arr = deps_i["is_array"] and self.is_array and dep_js_is_arr
            deps[js_idx] = (dep_js_ref, dep_is_arr)

        if self.index > 0:
            # prevent this jobscript executing if jobscript parallelism is not available:
            use_parallelism = (
                self.submission.JS_parallelism is True
                or {0: "direct", 1: "scheduled"}[self.is_scheduled]
                == self.submission.JS_parallelism
            )
            if not use_parallelism:
                # add fake dependencies to all previously submitted jobscripts to avoid
                # simultaneous execution:
                for js_idx, (js_ref, _) in scheduler_refs.items():
                    if js_idx not in deps:
                        deps[js_idx] = (js_ref, False)

        with self.EAR_ID_file_path.open(mode="wt", newline="\n") as ID_fp:
            for block in self.blocks:
                block.make_artifact_dirs()
                block.write_EAR_ID_file(ID_fp)

        js_path = self.write_jobscript(deps=deps)
        js_path = self.shell.prepare_JS_path(js_path)
        submit_cmd = self.scheduler.get_submit_command(self.shell, js_path, deps)
        self.app.submission_logger.info(
            f"submitting jobscript {self.index!r} with command: {submit_cmd!r}"
        )
        self._set_submit_cmdline(submit_cmd)
        self._set_submit_hostname(socket.gethostname())
        self._set_submit_machine(self.app.config.get("machine"))

        err_args = {
            "js_idx": self.index,
            "js_path": js_path,
            "subprocess_exc": None,
            "job_ID_parse_exc": None,
        }
        is_scheduler = isinstance(self.scheduler, Scheduler)
        job_ID = None
        process_ID = None
        try:
            if is_scheduler:
                # scheduled submission, wait for submission so we can parse the job ID:
                proc = subprocess.run(
                    args=submit_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=str(self.workflow.path),
                )
                stdout = proc.stdout.decode().strip()
                stderr = proc.stderr.decode().strip()
                err_args["stdout"] = stdout
                err_args["stderr"] = stderr
                if print_stdout and stdout:
                    print(stdout)
                if stderr:
                    print(stderr)
            else:
                if os.name == "nt":
                    process_ID = self._launch_direct_js_win()
                else:
                    process_ID = self._launch_direct_js_posix()

        except Exception as subprocess_exc:
            err_args["message"] = f"Failed to execute submit command."
            err_args["submit_cmd"] = submit_cmd
            err_args["stdout"] = None
            err_args["stderr"] = None
            err_args["subprocess_exc"] = subprocess_exc
            raise JobscriptSubmissionFailure(**err_args)

        if is_scheduler:
            # scheduled submission
            if stderr:
                err_args["message"] = "Non-empty stderr from submit command."
                err_args["submit_cmd"] = submit_cmd
                raise JobscriptSubmissionFailure(**err_args)

            try:
                job_ID = self.scheduler.parse_submission_output(stdout)

            except Exception as job_ID_parse_exc:
                # TODO: maybe handle this differently. If there is no stderr, then the job
                # probably did submit fine, but the issue is just with parsing the job ID
                # (e.g. if the scheduler version was updated and it now outputs
                # differently).
                err_args["message"] = "Failed to parse job ID from stdout."
                err_args["submit_cmd"] = submit_cmd
                err_args["job_ID_parse_exc"] = job_ID_parse_exc
                raise JobscriptSubmissionFailure(**err_args)

            self._set_scheduler_job_ID(job_ID)
            ref = job_ID

        else:
            # direct submission
            self._set_process_ID(process_ID)
            # a downstream direct jobscript might need to wait for this jobscript, which
            # means this jobscript's process ID must be committed:
            self.workflow._store._pending.commit_all()
            ref = process_ID

        self._set_submit_time(datetime.utcnow())

        return ref

    @property
    def is_submitted(self):
        """Return True if this jobscript has been submitted."""
        return self.index in self.submission.submitted_jobscripts

    @property
    def scheduler_js_ref(self):
        if isinstance(self.scheduler, Scheduler):
            return self.scheduler_job_ID
        else:
            return (self.process_ID, self.submit_cmdline)

    @property
    def scheduler_ref(self):
        out = {"js_refs": [self.scheduler_js_ref]}
        return out

    @TimeIt.decorator
    def get_active_states(
        self, as_json: bool = False
    ) -> Dict[int, Dict[int, JobscriptElementState]]:
        """If this jobscript is active on this machine, return the state information from
        the scheduler."""
        # this returns: {BLOCK_IDX: {JS_ELEMENT_IDX: STATE}}
        if not self.is_submitted:
            out = {}

        else:
            self.app.submission_logger.debug(
                "checking if the jobscript is running according to EAR submission "
                "states."
            )

            not_run_states = EARStatus.get_non_running_submitted_states()
            all_EAR_states = set(i.status for i in self.all_EARs)
            self.app.submission_logger.debug(f"Unique EAR states are: {all_EAR_states!r}")
            if all_EAR_states.issubset(not_run_states):
                self.app.submission_logger.debug(
                    f"All jobscript EARs are in a non-running state"
                )
                out = {}

            elif self.app.config.get("machine") == self.submit_machine:
                self.app.submission_logger.debug(
                    "Checking if jobscript is running according to the scheduler/process "
                    "ID."
                )
                out = self.scheduler.get_job_state_info(js_refs=[self.scheduler_js_ref])
                if out:
                    # remove scheduler ref (should be only one):
                    out = next(iter(out.values()))

                    if self.is_array:
                        # out values are dicts keyed by array index
                        # there will be exactly one block
                        out = {0: out}
                    else:
                        # out values are single states
                        out = {
                            idx: {i: out for i in range(block.num_elements)}
                            for idx, block in enumerate(self.blocks)
                        }

                    if as_json:
                        out = {
                            block_idx: {k: v.name for k, v in block_data.items()}
                            for block_idx, block_data in out.items()
                        }

            else:
                raise NotSubmitMachineError(
                    "Cannot get active state of the jobscript because the current machine "
                    "is not the machine on which the jobscript was submitted."
                )

        self.app.submission_logger.info(f"Jobscript is {'in' if not out else ''}active.")
        return out
