from __future__ import annotations
from abc import ABC, abstractmethod
from contextlib import contextmanager
import copy
from dataclasses import dataclass
from datetime import datetime
import random
import shutil
import string
import time
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple, Union
from pathlib import Path

from reretry import retry
from hpcflow.sdk.core.errors import WorkflowNotFoundError
from hpcflow.sdk.core.parameters import ParameterValue

from hpcflow.sdk.core.utils import get_in_container, get_relative_path, set_in_container
from hpcflow.sdk.typing import PathLike


PRIMITIVES = (
    int,
    float,
    str,
    type(None),
)


def dropbox_retry_fail(err: Exception) -> None:
    # TODO: this should log instead of printing!
    print("retrying...")


# TODO: maybe this is only an issue on Windows?
dropbox_permission_err_retry = retry(
    (PermissionError, OSError),
    tries=10,
    delay=1,
    backoff=2,
    fail_callback=dropbox_retry_fail,
)


@dropbox_permission_err_retry
def remove_dir(dir_path: Path) -> None:
    """Try very hard to delete a directory.

    Dropbox (on Windows, at least) seems to try to re-sync files if the parent directory
    is deleted soon after creation, which is the case on a failed workflow creation (e.g.
    missing inputs), so in addition to catching PermissionErrors generated when
    Dropbox has a lock on files, we repeatedly try deleting the directory tree.

    """
    while dir_path.is_dir():
        shutil.rmtree(dir_path)
        time.sleep(0.5)


@dropbox_permission_err_retry
def rename_dir(replaced_dir, original_dir) -> None:
    replaced_dir.rename(original_dir)


@dataclass
class PersistentStoreFeatures:
    """Class to represent the features provided by a persistent store.

    Attributes
    ----------
    jobscript_parallelism
        If True, the store supports workflows running multiple independent jobscripts
        simultaneously.
    EAR_parallelism
        If True, the store supports workflows running multiple EARs simultaneously.
    schedulers
        If True, the store supports submitting workflows to a scheduler
    submission
        If True, the store supports submission. If False, the store can be considered to
        be an archive, which would need transforming to another store type before
        submission.
    """

    jobscript_parallelism: bool = False
    EAR_parallelism: bool = False
    schedulers: bool = False
    submission: bool = False


class PersistentStore(ABC):

    _parameter_encoders = {}
    _parameter_decoders = {}
    _features = None

    def __init__(self, workflow: Workflow) -> None:
        self._workflow = workflow
        self._pending = self._get_pending_dct()
        if not self.exists():
            raise WorkflowNotFoundError(
                f"No workflow found at path: {self.workflow_path}"
            )

    @property
    def features(self):
        return self._features

    @property
    def store_name(self):
        return self._name

    @property
    def workflow(self) -> Workflow:
        return self._workflow

    @property
    def workflow_path(self) -> Path:
        return self.workflow.path

    @property
    def has_pending(self) -> bool:
        """Returns True if there are pending changes that are not yet committed."""
        return any(bool(v) for v in self._pending.values())

    def _get_pending_dct(self) -> Dict:
        return {
            "tasks": {},  # keys are new task indices
            "loops": [],
            "submissions": [],
            "submission_attempts": {},  # keys are submission indices, values are list of jobscript indices
            "jobscript_version_info": {},  # keys are submission indices, values are dicts with jobscript index keys
            "jobscript_submit_times": {},  # keys are submission indices, values are dicts with jobscript index keys
            "jobscript_job_IDs": {},  # keys are submission indices, values are dicts with jobscript index keys
            "loops_added_iters": {},  # keys are loop indices, values are num added iterations
            "template_tasks": {},  # keys are new task indices
            "template_loops": [],
            "template_components": {},
            "element_sets": {},  # keys are task indices
            "element_iterations": {},  # keys are (task index, task insert ID)
            "element_iterations_idx": {},  # keys are (task index, task insert ID), then element_idx
            "elements": {},  # keys are (task index, task insert ID)
            "EARs": {},  # keys are (task index, task insert ID, element_iter idx)
            "loop_idx": {},  # keys are (task index, task insert ID, element iteration index)
            "parameter_data": {},  # keys are parameter indices
            "parameter_sources": {},  # keys are parameter indices
            "parameter_source_updates": {},  # keys are parameter indices
            "remove_replaced_dir_record": False,
            "EAR_submission_idx": {},  # keys are (task insert ID, element_iter idx, action idx, run idx)
            "EAR_start_times": {},  # keys are (task insert ID, element_iter idx, action idx, run idx)
            "EAR_end_times": {},  # keys are (task insert ID, element_iter idx, action idx, run idx)
        }

    def reject_pending(self) -> None:
        self.clear_pending()

    def clear_pending(self) -> None:
        self._pending = self._get_pending_dct()

    def save(self) -> None:
        if not self.workflow._in_batch_mode:
            self.commit_pending()

    @contextmanager
    def cached_load(self) -> Iterator[None]:
        """Override this if a more performant implementation, is possible.

        For example, in a JSON persistent store, we need to load the whole document from
        disk to read anything from it, so we can temporarily cache the document if we know
        we will be making multiple reads."""

        yield

    def get_task_elements_islice(
        self,
        task_idx: int,
        task_insert_ID: int,
        selection: Union[int, slice],
    ) -> Iterator[Dict]:
        """Override this for a more performant implementation."""
        for idx in range(selection.start, selection.stop, selection.step):
            yield self.get_task_elements(
                task_idx, task_insert_ID, slice(idx, idx + 1, 1)
            )[0]

    def delete(self) -> None:
        """Delete the persistent workflow."""
        confirm = input(
            f"Permanently delete the workflow at path {self.workflow.path}; "
            f"[y]es or [n]o?"
        )
        if confirm.strip().lower() == "y":
            self.delete_no_confirm()

    def delete_no_confirm(self) -> None:
        """Permanently delete the workflow data with no confirmation."""
        remove_dir(self.workflow.path)

    def _merge_pending_template_components(self, template_components: Dict) -> bool:
        # assumes we have already checked for duplicates when adding to pending:
        is_modified = False
        for name, dat in self._pending["template_components"].items():
            if name not in template_components:
                template_components[name] = {}
            for k, v in dat.items():
                template_components[name][k] = v
                is_modified = True
        return is_modified

    def get_template_components(self) -> Dict:
        """Get all template components, including pending."""
        tc = self._get_persistent_template_components()
        if self._pending["template_components"]:
            tc = copy.deepcopy(tc)
            self._merge_pending_template_components(tc)
        return tc

    def add_template_components(self, template_components: Dict) -> None:

        ptc = self._get_persistent_template_components()
        pending = self._pending["template_components"]

        for name, dat in template_components.items():

            if name in ptc and name in pending:
                for hash, dat_i in dat.items():
                    if hash not in ptc[name] and hash not in pending[name]:
                        pending[name][hash] = dat_i

            elif name in pending:
                for hash, dat_i in dat.items():
                    if hash not in pending[name]:
                        pending[name][hash] = dat_i

            else:
                pending[name] = dat

        self.save()

    def add_empty_task(self, task_idx: int, task_js: Dict) -> None:
        self._pending["template_tasks"][task_idx] = task_js
        self._pending["tasks"][task_idx] = {"elements": [], "element_iterations": []}
        self.save()

    def add_element_set(self, task_idx: int, element_set_js: Dict) -> None:
        if task_idx not in self._pending["element_sets"]:
            self._pending["element_sets"][task_idx] = []
        self._pending["element_sets"][task_idx].append(element_set_js)
        self.save()

    def add_elements(
        self,
        task_idx: int,
        task_insert_ID: int,
        elements: List[Dict],
        element_iterations: List[Dict],
    ) -> None:

        key = (task_idx, task_insert_ID)
        if key not in self._pending["elements"]:
            self._pending["elements"][key] = []
        if key not in self._pending["element_iterations"]:
            self._pending["element_iterations"][key] = []
        self._pending["elements"][key].extend(elements)
        self._pending["element_iterations"][key].extend(element_iterations)
        self.save()

    def add_element_iterations(
        self,
        task_idx: int,
        task_insert_ID: int,
        element_iterations: List[Dict],
        element_iters_idx: Dict[int, List[int]],
    ) -> None:

        key = (task_idx, task_insert_ID)
        if key not in self._pending["element_iterations"]:
            self._pending["element_iterations"][key] = []
        if key not in self._pending["element_iterations_idx"]:
            self._pending["element_iterations_idx"][key] = {}

        self._pending["element_iterations"][key].extend(element_iterations)

        for elem_idx, iters_idx in element_iters_idx.items():
            if elem_idx not in self._pending["element_iterations_idx"][key]:
                self._pending["element_iterations_idx"][key][elem_idx] = []
            self._pending["element_iterations_idx"][key][elem_idx].extend(iters_idx)

        self.save()

    def update_loop_num_added_iters(self, loop_idx: int, num_added_iters: int):
        self._pending["loops_added_iters"][loop_idx] = num_added_iters
        self.save()

    def add_EARs(
        self,
        task_idx: int,
        task_insert_ID: int,
        element_iter_idx: int,
        EARs: Dict,
        param_src_updates: Dict,
    ) -> None:
        key = (task_idx, task_insert_ID, element_iter_idx)
        if key not in self._pending["EARs"]:
            self._pending["EARs"][key] = {}
        self._pending["EARs"][key].update(EARs)
        self._pending["parameter_source_updates"].update(param_src_updates)
        self.save()

    def add_loop(
        self,
        task_indices: List[int],
        loop_js: Dict,
        iterable_parameters: Dict[str:Dict],
    ) -> None:
        """Initialise the zeroth iterations of a named loop across the specified task
        subset.

        Parameters
        ----------
        task_indices
            List of task indices that identifies the task subset over which the new loop
            should iterate.

        """
        self._pending["template_loops"].append(loop_js)
        self._pending["loops"].append(
            {
                "num_added_iterations": 1,
                "iterable_parameters": iterable_parameters,
            }
        )

        for task_idx, task_insert_ID in zip(task_indices, loop_js["task_insert_IDs"]):
            all_elements = slice(0, self.workflow.tasks[task_idx].num_elements, 1)
            self._init_task_loop(
                task_idx=task_idx,
                task_insert_ID=task_insert_ID,
                name=loop_js["name"],
                element_sel=all_elements,
            )

        self.save()

    def add_submission(self, submission_js: Dict):
        """Add a new submission to the workflow."""
        self._pending["submissions"].append(submission_js)
        self.save()

    def append_submission_attempt(self, sub_idx, submitted_js_idx: int) -> None:
        if sub_idx not in self._pending["submission_attempts"]:
            self._pending["submission_attempts"][sub_idx] = []
        self._pending["submission_attempts"][sub_idx].append(submitted_js_idx)
        self.save()

    def set_jobscript_submit_time(
        self, sub_idx: int, js_idx: int, submit_time: datetime
    ) -> None:
        if sub_idx not in self._pending["jobscript_submit_times"]:
            self._pending["jobscript_submit_times"][sub_idx] = {}
        self._pending["jobscript_submit_times"][sub_idx][js_idx] = submit_time
        self.save()

    def set_jobscript_job_ID(self, sub_idx: int, js_idx: int, job_ID: int) -> None:
        if sub_idx not in self._pending["jobscript_job_IDs"]:
            self._pending["jobscript_job_IDs"][sub_idx] = {}
        self._pending["jobscript_job_IDs"][sub_idx][js_idx] = job_ID
        self.save()

    def set_jobscript_version_info(
        self,
        sub_idx: int,
        js_idx: int,
        vers_info: Tuple,
    ) -> None:
        if sub_idx not in self._pending["jobscript_version_info"]:
            self._pending["jobscript_version_info"][sub_idx] = {}
        self._pending["jobscript_version_info"][sub_idx][js_idx] = vers_info
        self.save()

    def add_parameter_data(self, data: Any, source: Dict) -> int:
        return self._add_parameter_data({"data": data}, source)

    def add_unset_parameter_data(self, source: Dict) -> int:
        return self._add_parameter_data(None, source)

    def _encode_parameter_data(
        self,
        obj: Any,
        path: List = None,
        type_lookup: Optional[Dict] = None,
        **kwargs,
    ) -> Any:

        path = path or []
        if type_lookup is None:
            type_lookup = {
                "tuples": [],
                "sets": [],
                **{k: [] for k in self._parameter_decoders.keys()},
            }

        if len(path) > 50:
            raise RuntimeError("I'm in too deep!")

        if isinstance(obj, ParameterValue):
            encoded = self._encode_parameter_data(
                obj=obj.to_dict(),
                path=path,
                type_lookup=type_lookup,
                **kwargs,
            )
            data, type_lookup = encoded["data"], encoded["type_lookup"]

        elif isinstance(obj, (list, tuple, set)):
            data = []
            for idx, item in enumerate(obj):
                encoded = self._encode_parameter_data(
                    obj=item,
                    path=path + [idx],
                    type_lookup=type_lookup,
                    **kwargs,
                )
                item, type_lookup = encoded["data"], encoded["type_lookup"]
                data.append(item)

            if isinstance(obj, tuple):
                type_lookup["tuples"].append(path)

            elif isinstance(obj, set):
                type_lookup["sets"].append(path)

        elif isinstance(obj, dict):
            data = {}
            for dct_key, dct_val in obj.items():
                encoded = self._encode_parameter_data(
                    obj=dct_val,
                    path=path + [dct_key],
                    type_lookup=type_lookup,
                    **kwargs,
                )
                dct_val, type_lookup = encoded["data"], encoded["type_lookup"]
                data[dct_key] = dct_val

        elif isinstance(obj, PRIMITIVES):
            data = obj

        elif type(obj) in self._parameter_encoders:
            data = self._parameter_encoders[type(obj)](
                obj=obj,
                path=path,
                type_lookup=type_lookup,
                **kwargs,
            )

        else:
            raise ValueError(
                f"Parameter data with type {type(obj)} cannot be serialised into a "
                f"{self.__class__.__name__}: {obj}."
            )

        return {"data": data, "type_lookup": type_lookup}

    def _decode_parameter_data(
        self,
        data: Union[None, Dict],
        path: Optional[List[str]] = None,
        **kwargs,
    ) -> Any:

        if data is None:
            return None

        path = path or []

        obj = get_in_container(data["data"], path)

        for tuple_path in data["type_lookup"]["tuples"]:
            try:
                rel_path = get_relative_path(tuple_path, path)
            except ValueError:
                continue
            if rel_path:
                set_in_container(obj, rel_path, tuple(get_in_container(obj, rel_path)))
            else:
                obj = tuple(obj)

        for set_path in data["type_lookup"]["sets"]:
            try:
                rel_path = get_relative_path(set_path, path)
            except ValueError:
                continue
            if rel_path:
                set_in_container(obj, rel_path, set(get_in_container(obj, rel_path)))
            else:
                obj = set(obj)

        for data_type in self._parameter_decoders:
            obj = self._parameter_decoders[data_type](
                obj=obj,
                type_lookup=data["type_lookup"],
                path=path,
                **kwargs,
            )

        return obj

    def get_creation_info(self):
        """Get information about the app that created the workflow."""
        return self.load_metadata()["creation_info"]

    @classmethod
    @abstractmethod
    def path_has_store(cls, path):
        """Is a given workflow path of this store type?"""
        pass

    @property
    @abstractmethod
    def store_path(self):
        """Get the store path, which may be the same as the workflow path."""
        pass

    @classmethod
    @abstractmethod
    def write_empty_workflow(
        cls,
        template_js: Dict,
        template_components_js: Dict,
        workflow_path: Path,
        replaced_dir: Path,
        creation_info: Dict,
    ) -> None:
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def commit_pending(self) -> None:
        pass

    @abstractmethod
    def _get_persistent_template_components(self) -> Dict:
        """Get currently persistent template components, excluding pending."""

    @abstractmethod
    def get_template(self) -> Dict:
        pass

    @abstractmethod
    def get_all_tasks_metadata(self) -> List[Dict]:
        pass

    @abstractmethod
    def get_loops(self) -> List[Dict]:
        pass

    @abstractmethod
    def get_submissions(self) -> List[Dict]:
        pass

    @abstractmethod
    def get_task_elements(
        self,
        task_idx: int,
        task_insert_ID: int,
        selection: slice,
    ) -> List:
        pass

    @abstractmethod
    def _add_parameter_data(self, data: Any, source: Dict) -> int:
        pass

    @abstractmethod
    def get_parameter_data(self, index: int) -> Tuple[bool, Any]:
        pass

    @abstractmethod
    def get_parameter_source(self, index: int) -> Dict:
        pass

    @abstractmethod
    def get_all_parameter_data(self) -> Dict[int, Any]:
        pass

    @abstractmethod
    def is_parameter_set(self, index: int) -> bool:
        pass

    @abstractmethod
    def set_parameter(self, index: int, data: Any) -> None:
        """Set the value of a pre-allocated parameter."""
        pass

    @abstractmethod
    def check_parameters_exist(
        self, indices: Union[int, List[int]]
    ) -> Union[bool, List[bool]]:
        pass

    @abstractmethod
    def _init_task_loop(
        self,
        task_idx: int,
        task_insert_ID: int,
        element_sel: slice,
        name: str,
    ) -> None:
        """Initialise the zeroth iteration of a named loop for a specified task."""

    @abstractmethod
    def remove_replaced_dir(self) -> None:
        pass

    @abstractmethod
    def reinstate_replaced_dir(self) -> None:
        pass

    @abstractmethod
    def copy(self, path: PathLike = None) -> None:
        """Make a copy of the store."""
        pass

    @abstractmethod
    def is_modified_on_disk(self) -> bool:
        """Check if the workflow (metadata) has been modified on disk since initial
        load (this is bad)."""
        pass

    @abstractmethod
    def get_num_added_tasks(self) -> int:
        """Get the total number of tasks ever added to the workflow, regardless of whether
        any of those tasks were subsequently removed from the workflow."""

    @property
    def ts_fmt(self) -> str:
        return self.workflow.ts_fmt

    def set_EAR_submission_indices(
        self,
        sub_idx: int,
        EAR_indices: Tuple[int, int, int, int],
    ) -> None:
        for key in EAR_indices:
            self._pending["EAR_submission_idx"][key] = sub_idx
        self.save()

    def set_EAR_start(
        self,
        task_insert_ID: int,
        element_iteration_idx: int,
        action_idx: int,
        run_idx: int,
    ) -> None:
        key = (task_insert_ID, element_iteration_idx, action_idx, run_idx)
        self._pending["EAR_start_times"][key] = datetime.utcnow()
        self.save()

    def set_EAR_end(
        self,
        task_insert_ID: int,
        element_iteration_idx: int,
        action_idx: int,
        run_idx: int,
    ) -> None:
        key = (task_insert_ID, element_iteration_idx, action_idx, run_idx)
        self._pending["EAR_end_times"][key] = datetime.utcnow()
        self.save()
