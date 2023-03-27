from __future__ import annotations
from abc import ABC, abstractmethod
from contextlib import contextmanager
import copy
import random
import string
from typing import Any, Dict, Generator, Iterator, List, Optional, Union
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


class PersistentStore(ABC):

    _parameter_encoders = {}
    _parameter_decoders = {}

    def __init__(self, workflow: Workflow) -> None:
        self._workflow = workflow
        self._pending = self._get_pending_dct()
        if not self.exists():
            raise WorkflowNotFoundError(f"No workflow found at path: {self.path}")

    @property
    def workflow(self) -> Workflow:
        return self._workflow

    @property
    def path(self) -> Path:
        return self.workflow.path

    @property
    def has_pending(self) -> bool:
        """Returns True if there are pending changes that are not yet committed."""
        return any(bool(v) for v in self._pending.values())

    def _get_pending_dct(self) -> Dict:
        return {
            "tasks": {},  # keys are new task indices
            "template_tasks": {},  # keys are new task indices
            "template_components": {},
            "element_sets": {},  # keys are task indices
            "elements": {},  # keys are (task index, task insert ID)
            "parameter_data": {},  # keys are parameter indices
            "parameter_sources": {},  # keys are parameter indices
            "remove_replaced_file_record": False,
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

    def _merge_pending_template_components(self, template_components: Dict) -> None:
        # assumes we have already checked for duplicates when adding to pending:
        for name, dat in self._pending["template_components"].items():
            if name not in template_components:
                template_components[name] = {}
            for k, v in dat.items():
                template_components[name][k] = v

    @classmethod
    def _rename_existing(cls, path):
        # rename existing file so we can restore it if workflow creation fails:
        temp_ext = "".join(random.choices(string.ascii_letters, k=8))
        replaced_file = path.with_suffix(f"{path.suffix}.{temp_ext}")
        path.rename(replaced_file)
        return replaced_file

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
        self._pending["tasks"][task_idx] = {"elements": []}
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
    ) -> None:
        key = (task_idx, task_insert_ID)
        if key not in self._pending["elements"]:
            self._pending["elements"][key] = []
        self._pending["elements"][key].extend(elements)
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

    @classmethod
    @abstractmethod
    def write_empty_workflow(
        cls,
        template_js: Dict,
        template_components_js: Dict,
        path: Path,
        overwrite: bool,
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
    def get_parameter_data(self, index: int) -> Any:
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
    def delete_no_confirm(self) -> None:
        """Permanently delete the workflow data with no confirmation."""

    @abstractmethod
    def remove_replaced_file(self) -> None:
        pass

    @abstractmethod
    def reinstate_replaced_file(self) -> None:
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
