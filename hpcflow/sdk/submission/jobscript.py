from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
import subprocess
from textwrap import indent
from typing import Dict, List, Optional, Tuple

import numpy as np
from numpy.typing import NDArray
from hpcflow.sdk.core.actions import ElementID
from hpcflow.sdk.core.errors import JobscriptSubmissionFailure

from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.submission.jobscript_templates import (
    BASH_ELEMENT_ARRAY,
    BASH_ELEMENT_LOOP,
    BASH_HEADER,
    BASH_SHEBANG,
    BASH_SCHEDULER_HEADER,
    BASH_DIRECT_HEADER,
    BASH_MAIN,
)
from hpcflow.sdk.submission.schedulers import Scheduler
from hpcflow.sdk.submission.schedulers.direct import DirectPosix, DirectWindows
from hpcflow.sdk.submission.schedulers.sge import SGEPosix
from hpcflow.sdk.submission.schedulers.slurm import SlurmPosix


scheduler_cls_lookup = {
    ("sge", "posix"): SGEPosix,
    ("slurm", "posix"): SlurmPosix,
    ("slurm", "nt"): SlurmPosix,  # TEMP!
    ("direct", "posix"): DirectPosix,
    ("direct", "nt"): DirectWindows,
}


def generate_EAR_resource_map(
    task: WorkflowTask,
) -> Tuple[List[ElementResources], List[int], NDArray, NDArray]:
    """Generate an integer array whose rows represent actions and columns represent task
    elements and whose values index unique resources."""
    # TODO: assume single iteration for now; later we will loop over Loop tasks for each
    # included task and call this func with specific loop indices
    none_val = -1
    resources = []
    resource_hashes = []

    arr_shape = (task.num_actions, task.num_elements)
    resource_map = np.empty(arr_shape, dtype=int)
    EAR_idx_map = np.empty(
        shape=arr_shape,
        dtype=[("EAR_idx", np.int32), ("run_idx", np.int32), ("iteration_idx", np.int32)],
    )
    resource_map[:] = none_val
    EAR_idx_map[:] = (none_val, none_val, none_val)  # TODO: add iteration_idx as well

    for element in task.elements:
        for iter_i in element.iterations:
            if iter_i.EARs_initialised:  # not strictly needed (actions will be empty)
                for act_idx, action in iter_i.actions.items():
                    for run in action.runs:
                        if run.submission_status.name == "PENDING":
                            res_hash = hash(run.resources)
                            if res_hash not in resource_hashes:
                                resource_hashes.append(res_hash)
                                resources.append(run.resources)
                            resource_map[act_idx][element.index] = resource_hashes.index(
                                res_hash
                            )
                            EAR_idx_map[act_idx, element.index] = (
                                run.index,
                                run.run_idx,
                                iter_i.index,
                            )

    return (
        resources,
        resource_hashes,
        resource_map,
        EAR_idx_map,
    )


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


def resolve_jobscript_dependencies(jobscripts, element_deps):

    # first pass is to find the mappings between jobscript elements:
    jobscript_deps = {}
    for js_idx, elem_deps in element_deps.items():

        # keys of new dict are other jobscript indices on which this jobscript (js_idx)
        # depends:
        jobscript_deps[js_idx] = {}

        for js_elem_idx_i, EAR_deps_i in elem_deps.items():

            for EAR_dep_j in EAR_deps_i:

                # locate which jobscript(s) this element depends on:
                for js_k_idx, js_k in jobscripts.items():

                    if js_k_idx == js_idx:
                        break

                    if EAR_dep_j in js_k["EARs"]:

                        if js_k_idx not in jobscript_deps[js_idx]:
                            jobscript_deps[js_idx][js_k_idx] = {"js_element_mapping": {}}

                        if (
                            js_elem_idx_i
                            not in jobscript_deps[js_idx][js_k_idx]["js_element_mapping"]
                        ):
                            jobscript_deps[js_idx][js_k_idx]["js_element_mapping"][
                                js_elem_idx_i
                            ] = []

                        dep_j_task_iID = EAR_dep_j[0]
                        dep_j_elem_idx = EAR_dep_j[1]
                        js_elem_idx_k = js_k["task_elements"][dep_j_task_iID].index(
                            dep_j_elem_idx
                        )
                        jobscript_deps[js_idx][js_k_idx]["js_element_mapping"][
                            js_elem_idx_i
                        ].append(js_elem_idx_k)

    # next we can determine if two jobscripts have a one-to-one element mapping, which
    # means they can be submitted with a "job array" dependency relationship:
    for js_i_idx, deps_i in jobscript_deps.items():

        for js_k_idx, deps_j in deps_i.items():
            # is this an array dependency?

            js_i_num_js_elements = jobscripts[js_i_idx]["EAR_idx"].shape[1]
            js_k_num_js_elements = jobscripts[js_k_idx]["EAR_idx"].shape[1]

            is_all_i_elems = list(
                sorted(set(deps_j["js_element_mapping"].keys()))
            ) == list(range(js_i_num_js_elements))

            is_all_k_single = set(
                len(i) for i in deps_j["js_element_mapping"].values()
            ) == {1}

            is_all_k_elems = list(
                sorted(set(i[0] for i in deps_j["js_element_mapping"].values()))
            ) == list(range(js_k_num_js_elements))

            is_arr = is_all_i_elems and is_all_k_single and is_all_k_elems
            jobscript_deps[js_i_idx][js_k_idx]["is_array"] = is_arr

    return jobscript_deps


def merge_jobscripts_across_tasks(jobscripts: Dict) -> Dict:
    """Try to merge jobscripts between tasks.

    This is possible if two jobscripts share the same resources and have an array
    dependency (i.e. one-to-one element dependency mapping).

    """

    for js_idx, js in jobscripts.items():

        # for now only attempt to merge a jobscript with a single dependency:
        if len(js["dependencies"]) == 1:

            js_j_idx = next(iter(js["dependencies"]))
            dep_info = js["dependencies"][js_j_idx]
            js_j = jobscripts[js_j_idx]  # the jobscript we are merging `js` into

            # can only merge if resources are the same and is array dependency:
            if js["resource_hash"] == js_j["resource_hash"] and dep_info["is_array"]:

                # append task_insert_IDs
                js_j["task_insert_IDs"].append(js["task_insert_IDs"][0])

                js_j["task_actions"].extend(js["task_actions"])
                js_j["task_elements"].update(js["task_elements"])

                # update EARs dict
                js_j["EARs"].update(js["EARs"])

                # append to elements and elements_idx list
                js_j["EAR_idx"] = np.vstack((js_j["EAR_idx"], js["EAR_idx"]))

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


def jobscripts_to_list(jobscripts: Dict[int, Dict]) -> List[Dict]:
    """Convert the jobscripts dict to a list, normalising jobscript indices so they refer
    to list indices; also remove `resource_hash`."""
    lst = []
    for js_idx, js in jobscripts.items():
        new_idx = len(lst)
        if js_idx != new_idx:
            # need to reindex jobscripts that depend on this one
            for js_j_idx, js_j in jobscripts.items():
                if js_j_idx <= js_idx:
                    continue
                if js_idx in js_j["dependencies"]:
                    jobscripts[js_j_idx]["dependencies"][new_idx] = jobscripts[js_j_idx][
                        "dependencies"
                    ].pop(js_idx)
        del jobscripts[js_idx]["resource_hash"]
        lst.append(js)

    return lst


class Jobscript(JSONLike):

    _app_attr = "app"
    _EAR_files_delimiter = ":"

    _child_objects = (
        ChildObjectSpec(
            name="resources",
            class_name="ElementResources",
        ),
    )

    def __init__(
        self,
        task_insert_IDs: List[int],
        task_actions: List[Tuple],
        task_elements: Dict[int, List[int]],
        EARs: Dict[Tuple[int] : Tuple[int]],
        EAR_idx: NDArray,
        resources: ElementResources,
        loop_idx: Dict,
        dependencies: Dict[int:Dict],
        submit_time: Optional[datetime] = None,
        scheduler_job_ID: Optional[str] = None,
        version_info: Optional[Tuple[str]] = None,
    ):
        self._task_insert_IDs = task_insert_IDs
        self._task_actions = task_actions
        self._task_elements = task_elements
        self._EARs = EARs
        self._EAR_idx = EAR_idx
        self._resources = resources
        self._loop_idx = loop_idx
        self._dependencies = dependencies

        # assigned on parent `Submission.submit` (or retrieved form persistent store):
        self._submit_time = submit_time
        self._scheduler_job_ID = scheduler_job_ID
        self._version_info = version_info

        self._submission = None  # assigned by parent Submission
        self._index = None  # assigned by parent Submission
        self._scheduler_obj = None  # assigned on first access to `scheduler` property

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"index={self.index!r}, "
            f"task_insert_IDs={self.task_insert_IDs!r}, "
            f"resources={self.resources!r}, "
            f"dependencies={self.dependencies!r}"
            f")"
        )

    def to_dict(self):
        dct = super().to_dict()
        del dct["_index"]
        del dct["_scheduler_obj"]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        dct["EAR_idx"] = dct["EAR_idx"].tolist()
        dct["EARs"] = [[list(k), list(v)] for k, v in dct["EARs"].items()]
        if dct.get("scheduler_version_info"):
            dct["scheduler_version_info"] = list(dct["scheduler_version_info"])
        return dct

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        json_like["EAR_idx"] = np.array(json_like["EAR_idx"])
        json_like["EARs"] = {tuple(i[0]): tuple(i[1]) for i in json_like["EARs"]}
        if json_like.get("scheduler_version_info"):
            json_like["scheduler_version_info"] = tuple(
                json_like["scheduler_version_info"]
            )
        return super().from_json_like(json_like, shared_data)

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
    def EARs(self):
        return self._EARs

    @property
    def EAR_idx(self):
        return self._EAR_idx

    @property
    def resources(self):
        return self._resources

    @property
    def loop_idx(self):
        return self._loop_idx

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def submit_time(self):
        return self._submit_time

    @property
    def scheduler_job_ID(self):
        return self._scheduler_job_ID

    @property
    def scheduler_version_info(self):
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
    def num_actions(self):
        return self.EAR_idx.shape[0]

    @property
    def num_elements(self):
        return self.EAR_idx.shape[1]

    @property
    def is_array(self):
        if self.scheduler_name == "direct":
            return False

        if self.resources.use_job_array is None:
            if self.num_elements > 1:
                return True
            else:
                return False
        else:
            return self.resources.use_job_array

    @property
    def scheduler_name(self):
        return self.resources.scheduler.lower()

    @property
    def scheduler(self):
        if self._scheduler_obj is None:
            key = (self.scheduler_name, self.resources.os_name or os.name)
            try:
                scheduler_cls = scheduler_cls_lookup[key]
            except KeyError:
                raise ValueError(
                    f"Unsupported combination of scheduler and operation system: {key!r}"
                )
            self._scheduler_obj = scheduler_cls(**self.resources.scheduler_commands)
        return self._scheduler_obj

    @property
    def need_EAR_file_name(self):
        return f"{self.index}_need_EARs.txt"

    @property
    def element_run_dir_file_name(self):
        return f"{self.index}_run_dirs.txt"

    @property
    def jobscript_name(self):
        return f"{self.index}_jobscript.sh"

    @property
    def need_EAR_file_path(self):
        return self.submission.path / self.need_EAR_file_name

    @property
    def element_run_dir_file_path(self):
        return self.submission.path / self.element_run_dir_file_name

    @property
    def jobscript_path(self):
        return self.submission.path / self.jobscript_name

    def _set_submit_time(self, value: str) -> None:
        self._scheduler_job_ID = value
        self.workflow._store.set_jobscript_submit_time(
            sub_idx=self.submission.index,
            js_idx=self.index,
            submit_time=value,
        )

    def _set_scheduler_job_ID(self, value: str) -> None:
        self._scheduler_job_ID = value
        self.workflow._store.set_jobscript_job_ID(
            sub_idx=self.submission.index,
            js_idx=self.index,
            job_ID=value,
        )

    def _set_version_info(self, vers_info):
        self._version_info = vers_info
        self.workflow._store.set_jobscript_version_info(
            sub_idx=self.submission.index,
            js_idx=self.index,
            vers_info=vers_info,
        )

    def get_task_insert_IDs_array(self):
        task_insert_IDs = np.empty_like(self.EAR_idx)
        task_insert_IDs[:] = np.array([i[0] for i in self.task_actions]).reshape(
            (len(self.task_actions), 1)
        )
        return task_insert_IDs

    def get_task_element_idx_array(self):
        element_idx = np.empty_like(self.EAR_idx)
        for task_iID, elem_idx in self.task_elements.items():
            rows_idx = [
                idx for idx, i in enumerate(self.task_actions) if i[0] == task_iID
            ]
            element_idx[rows_idx] = elem_idx
        return element_idx

    def get_EAR_run_idx_array(self):
        task_insert_ID_arr = self.get_task_insert_IDs_array()
        element_idx = self.get_task_element_idx_array()
        run_idx = np.empty_like(self.EAR_idx)
        for js_act_idx in range(self.num_actions):
            for js_elem_idx in range(self.num_elements):
                EAR_idx_i = self.EAR_idx[js_act_idx, js_elem_idx]
                task_iID_i = task_insert_ID_arr[js_act_idx, js_elem_idx]
                elem_idx_i = element_idx[js_act_idx, js_elem_idx]
                (iter_idx_i, act_idx_i, run_idx_i) = self.EARs[
                    (task_iID_i, elem_idx_i, EAR_idx_i)
                ]
                run_idx[js_act_idx, js_elem_idx] = run_idx_i
        return run_idx

    def write_need_EARs_file(self):
        """Write a text file with `num_elements` lines and `num_actions` delimited tokens
        per line, representing whether a given EAR must be executed."""
        np.savetxt(
            fname=self.need_EAR_file_path,
            X=(self.EAR_idx != -1).T,
            fmt="%.0f",
            delimiter=self._EAR_files_delimiter,
        )

    def write_element_run_dir_file(self, run_dirs: List[List[Path]]):
        """Write a text file with `num_elements` lines and `num_actions` delimited tokens
        per line, representing the working directory for each EAR.

        We assume a given task element's actions all run in the same directory, but in
        general a jobscript "element" may cross task boundaries, so we need to provide
        the directory for each jobscript-element/jobscript-action combination.

        """
        np.savetxt(
            fname=self.element_run_dir_file_path,
            X=np.array(run_dirs).astype(str),
            fmt="%s",
            delimiter=":",
        )

    def compose_jobscript(self) -> str:
        """Prepare the jobscript file string."""
        # workflows should be submitted from the workflow root directory
        header_args = {
            "app_invoc": " ".join(self.app.run_time_info.invocation_command),
            "workflow_path": self.workflow.path,
            "sub_idx": self.submission.index,
            "js_idx": self.index,
            "EAR_file_name": self.need_EAR_file_name,
            "element_run_dirs_file_path": self.element_run_dir_file_name,
        }

        os_name = self.resources.os_name or os.name
        if os_name == "posix":

            bash_shebang = BASH_SHEBANG.format(
                shell_executable=self.scheduler.shell_executable,
                shell_args=self.scheduler.shell_args,
            )
            bash_header = BASH_HEADER.format(**header_args)

            if isinstance(self.scheduler, Scheduler):
                bash_header = BASH_SCHEDULER_HEADER.format(
                    bash_shebang=bash_shebang,
                    scheduler_options=self.scheduler.format_options(
                        self.resources,
                        self.num_elements,
                    ),
                    bash_header=bash_header,
                )

            else:
                bash_header = BASH_DIRECT_HEADER.format(
                    bash_shebang=bash_shebang,
                    bash_header=bash_header,
                )

            bash_main = BASH_MAIN.format(
                num_actions=self.num_actions,
                EAR_files_delimiter=self._EAR_files_delimiter,
            )

            out = bash_header

            if self.is_array:
                out += BASH_ELEMENT_ARRAY.format(
                    scheduler_command=self.scheduler.js_cmd,
                    scheduler_array_switch=self.scheduler.array_switch,
                    scheduler_array_item_var=self.scheduler.array_item_var,
                    num_elements=self.num_elements,
                    bash_main=bash_main,
                )

            else:
                out += BASH_ELEMENT_LOOP.format(
                    num_elements=self.num_elements,
                    bash_main=indent(bash_main, "  "),
                )

        elif os_name == "nt":
            out = ""

        return out

    def write_jobscript(self):
        js_str = self.compose_jobscript()
        with self.jobscript_path.open("wt") as fp:
            fp.write(js_str)
        return self.jobscript_path

    def make_artifact_dirs(self, task_artifacts_path):

        # TODO: consider iteration paths - this will change the task dir?

        task_insert_ID_arr = self.get_task_insert_IDs_array()
        element_idx = self.get_task_element_idx_array()
        run_idx = self.get_EAR_run_idx_array()

        run_dirs = []
        for js_elem_idx in range(self.num_elements):

            run_dirs_i = []
            for js_act_idx in range(self.num_actions):

                t_iID = task_insert_ID_arr[js_act_idx, js_elem_idx].item()
                e_idx = element_idx[js_act_idx, js_elem_idx].item()
                r_idx = run_idx[js_act_idx, js_elem_idx].item()

                task_dir = self.workflow.tasks.get(insert_ID=t_iID).dir_name
                # .get_dir_name(loop_idx)

                # TODO: don't load element, but make sure format is the same
                element_ID = ElementID(task_insert_ID=t_iID, element_idx=e_idx)
                element = self.workflow.get_elements_from_IDs([element_ID])[0]
                elem_dir = element.dir_name

                run_dir = f"r_{r_idx}"

                EAR_dir = Path(task_artifacts_path, task_dir, elem_dir, run_dir)
                EAR_dir.mkdir(parents=True)

                run_dirs_i.append(EAR_dir.relative_to(self.workflow.path))

            run_dirs.append(run_dirs_i)

        return run_dirs

    def submit(self, task_artifacts_path: Path, scheduler_refs: Dict[int, str]) -> str:

        run_dirs = self.make_artifact_dirs(task_artifacts_path)
        self.write_need_EARs_file()
        self.write_element_run_dir_file(run_dirs)
        js_path = self.write_jobscript()

        deps = []
        for js_idx, deps_i in self.dependencies.items():
            dep_job_ID = scheduler_refs[js_idx]
            deps.append((dep_job_ID, deps_i["is_array"]))

        err_args = {
            "js_idx": self.index,
            "js_path": js_path,
            "subprocess_exc": None,
        }
        try:
            submit_cmd = self.scheduler.get_submit_command(js_path, deps)
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

        except Exception as subprocess_exc:
            err_args["message"] = f"Failed to execute submit command: {submit_cmd!r}."
            err_args["stdout"] = None
            err_args["stderr"] = None
            err_args["subprocess_exc"] = subprocess_exc
            raise JobscriptSubmissionFailure(**err_args)

        if stderr:
            err_args["message"] = "Non-empty stderr from submit command."
            raise JobscriptSubmissionFailure(**err_args)

        try:
            job_ID = self.scheduler.parse_submission_output(stdout)
        except Exception:
            # TODO: maybe handle this differently. If there is no stderr, then the job
            # probably did submit fine, but the issue is just with parsing the job ID
            # (e.g. if the scheduler version was updated and it now outputs differently).
            err_args["message"] = "Failed to parse job ID from stdout."
            raise JobscriptSubmissionFailure(**err_args)

        self._set_submit_time(datetime.utcnow())
        self._set_scheduler_job_ID(job_ID)

        return job_ID
