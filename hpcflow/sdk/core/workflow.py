from __future__ import annotations
from contextlib import contextmanager
import copy
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import shutil
import time
from typing import Dict, Iterator, List, Optional, Tuple, Type, Union
from warnings import warn

import numpy as np
import zarr

from hpcflow.sdk.typing import PathLike


from .json_like import ChildObjectSpec, JSONLike
from .parameters import InputSource
from .task import ElementSet, Task
from .utils import get_md5_hash, read_YAML, read_YAML_file
from .errors import (
    InvalidInputSourceTaskReference,
    WorkflowBatchUpdateFailedError,
)

from hpcflow.sdk.persistence.json import JSONPersistentStore
from hpcflow.sdk.persistence.zarr import ZarrPersistentStore

TS_NAME_FMT = r"%Y-%m-%d_%H%M%S"


@dataclass
class BatchUpdateData:
    """Class to store batch update instructions, ready for writing to the persistent
    workflow at the end of the batch update."""


@dataclass
class WorkflowTemplate(JSONLike):
    """Class to represent initial parametrisation of a workflow, with limited validation
    logic."""

    _child_objects = (
        ChildObjectSpec(
            name="tasks",
            class_name="Task",
            is_multiple=True,
            parent_ref="workflow_template",
        ),
    )

    name: str
    tasks: Optional[List[Task]] = field(default_factory=lambda: [])
    workflow: Optional[Workflow] = None

    def __post_init__(self):
        self._set_parent_refs()

    @classmethod
    def _from_data(cls, data: Dict) -> WorkflowTemplate:
        # use element_sets if not already:
        for task_idx, task_dat in enumerate(data["tasks"]):
            if "element_sets" not in task_dat:
                # add a single element set:
                elem_set = {}
                for chd_obj in ElementSet._child_objects:
                    if chd_obj.name in task_dat:
                        elem_set[chd_obj.name] = task_dat.pop(chd_obj.name)
                data["tasks"][task_idx]["element_sets"] = [elem_set]

        return cls.from_json_like(data, shared_data=cls.app.template_components)

    @classmethod
    def from_YAML_string(cls, string: str) -> WorkflowTemplate:
        return cls._from_data(read_YAML(string))

    @classmethod
    def from_YAML_file(cls, path: PathLike) -> WorkflowTemplate:
        return cls._from_data(read_YAML_file(path))

    def _add_empty_task(self, task: Task, new_index: int, insert_ID: int) -> None:
        """Called by `Workflow._add_empty_task`."""
        new_task_name = self.workflow._get_new_task_unique_name(task, new_index)

        task._insert_ID = insert_ID
        task._dir_name = f"task_{task.insert_ID}_{new_task_name}"
        task._element_sets = []  # element sets are added to the Task during add_elements
        task.workflow_template = self
        self.tasks.insert(new_index, task)


class Workflow:

    _app_attr = "app"

    _persistent_store_ext_lookup = {
        "json": ".json",
        "zarr": "",
    }

    _persistent_store_cls_lookup = {
        ".json": JSONPersistentStore,
        "": ZarrPersistentStore,
    }

    def __init__(self, path: PathLike) -> None:
        self.path = Path(path)

        # assigned on first access to corresponding properties:
        self._template = None
        self._template_components = None
        self._tasks = None

        self._store = self._get_store_class_from_ext(self.path)(self)

        self._in_batch_mode = False  # flag to track when processing batch updates

        # store indices of updates during batch update, so we can revert on failure:
        self._pending = self._get_empty_pending()

    def _get_empty_pending(self) -> Dict:
        return {
            "template_components": {k: [] for k in self.app._template_component_types},
            "tasks": [],
        }

    def _accept_pending(self) -> None:
        self._reset_pending()

    def _reset_pending(self) -> None:
        self._pending = self._get_empty_pending()

    def _reject_pending(self) -> None:
        """Revert pending changes to the in-memory representation of the workflow.

        This deletes new tasks and new template component data. Element additions to
        existing (non-pending) tasks are separately rejected/accepted by the
        WorkflowTask object.

        """
        for task_idx in self._pending["tasks"][::-1]:
            self.tasks._remove_object(task_idx)
            self.template.tasks.pop(task_idx)

        for comp_type, comp_indices in self._pending["template_components"].items():
            for comp_idx in comp_indices[::-1]:
                self.template_components[comp_type]._remove_object(comp_idx)

        self._reset_pending()

    @classmethod
    def _get_store_class_from_ext(cls, path: str) -> Type:
        return cls._persistent_store_cls_lookup[path.suffix.lower()]

    @property
    def store_format(self):
        # TODO: make this info cleaner to access
        for k, v in self._persistent_store_cls_lookup.items():
            if v == type(self._store):
                for k2, v2 in self._persistent_store_ext_lookup.items():
                    if v2 == k:
                        return k2

    @classmethod
    def from_template(
        cls,
        template: WorkflowTemplate,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = "zarr",
    ) -> Workflow:
        wk = cls._write_empty_workflow(template, path, name, overwrite, store)
        with wk._store.cached_load():
            with wk.batch_update(is_workflow_creation=True):
                for task in template.tasks:
                    wk._add_task(task)
        return wk

    @classmethod
    def from_YAML_file(
        cls,
        YAML_path: PathLike,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
    ) -> Workflow:
        template = cls.app.WorkflowTemplate.from_YAML_file(YAML_path)
        return cls.from_template(template, path, name, overwrite)

    @classmethod
    def from_tasks(
        cls,
        name: str,
        tasks: List[Task],
        path: Optional[PathLike] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = "zarr",
    ) -> Workflow:
        raise NotImplementedError

    @contextmanager
    def batch_update(self, is_workflow_creation: bool = False) -> Iterator[None]:
        """A context manager that batches up structural changes to the workflow and
        commits them to disk all together when the context manager exits."""

        if self._in_batch_mode:
            yield
        else:
            try:
                self._in_batch_mode = True
                yield

            except Exception as err:

                print("batch update exception!")

                self._in_batch_mode = False
                self._store.reject_pending()

                for task in self.tasks:
                    task._reset_pending_elements()

                self._reject_pending()

                if is_workflow_creation:
                    # creation failed, so no need to keep the newly generated workflow:
                    self._store.delete_no_confirm()
                    self._store.reinstate_replaced_file()

                raise err

            else:

                if self._store.has_pending:

                    if self._store.is_modified_on_disk():
                        raise WorkflowBatchUpdateFailedError(
                            "Workflow modified on disk since it was loaded!"
                        )

                    for task in self.tasks:
                        task._accept_pending_elements()

                    self._store.remove_replaced_file()
                    self._store.commit_pending()
                    self._accept_pending()
                    self._in_batch_mode = False

    @classmethod
    def _write_empty_workflow(
        cls,
        template: WorkflowTemplate,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = "zarr",
    ) -> Workflow:

        timestamp = datetime.utcnow()

        path = Path(path or "").resolve()
        name = name or f"{template.name}_{timestamp.strftime(TS_NAME_FMT)}"
        ext = cls._persistent_store_ext_lookup[store.lower()]
        path = path.joinpath(name + ext)

        store_cls = cls._persistent_store_cls_lookup[ext]
        template_js, template_sh = template.to_json_like(exclude=["tasks"])
        template_js["tasks"] = []
        store_cls.write_empty_workflow(template_js, template_sh, path, overwrite)

        return cls(path)

    @property
    def num_tasks(self) -> int:
        return len(self.tasks)

    @property
    def num_added_tasks(self) -> int:
        with self._store.cached_load():
            return self._store.get_num_added_tasks()

    @property
    def num_elements(self) -> int:
        return sum(task.num_elements for task in self.tasks)

    @property
    def template_components(self) -> Dict:
        if self._template_components is None:
            with self._store.cached_load():
                tc_js = self._store.get_template_components()
            self._template_components = self.app.template_components_from_json_like(tc_js)
        return self._template_components

    @property
    def template(self) -> WorkflowTemplate:
        if self._template is None:
            with self._store.cached_load():
                temp_js = self._store.get_template()
                template = self.app.WorkflowTemplate.from_json_like(
                    temp_js, self.template_components
                )
                template.workflow = self
            self._template = template

        return self._template

    @property
    def tasks(self) -> WorkflowTaskList:
        if self._tasks is None:
            with self._store.cached_load():
                tasks_meta = self._store.get_all_tasks_metadata()
                wk_tasks = []
                for idx, i in enumerate(tasks_meta):
                    wk_task = self.app.WorkflowTask(
                        workflow=self,
                        template=self.template.tasks[idx],
                        index=idx,
                        num_elements=i["num_elements"],
                    )
                    wk_tasks.append(wk_task)
                self._tasks = self.app.WorkflowTaskList(wk_tasks)
        return self._tasks

    def elements(self) -> Iterator[Element]:
        for task in self.tasks:
            for element in task.elements:
                yield element

    def copy(self, path=None) -> Workflow:
        """Copy the workflow to a new path and return the copied workflow."""
        if path is None:
            path = self.path.parent / Path(self.path.stem + "_copy" + self.path.suffix)
        if path.exists():
            raise ValueError(f"Path already exists: {path}.")
        self._store.copy(path=path)
        return self.app.Workflow(path=path)

    def delete(self):
        self._store.delete()

    def _delete_no_confirm(self):
        self._store.delete_no_confirm()

    def rename(self, new_name: str):
        raise NotImplementedError

    def submit(self):
        raise NotImplementedError

    def add_submission(self, filter):
        raise NotImplementedError

    def get_task_unique_names(
        self, map_to_insert_ID: bool = False
    ) -> Union[List[str], Dict[str, int]]:
        """Return the unique names of all workflow tasks.

        Parameters
        ----------
        map_to_insert_ID : bool, optional
            If True, return a dict whose values are task insert IDs, otherwise return a
            list.

        """
        names = Task.get_task_unique_names(self.template.tasks)
        if map_to_insert_ID:
            insert_IDs = (i.insert_ID for i in self.template.tasks)
            return dict(zip(names, insert_IDs))
        else:
            return names

    def _get_new_task_unique_name(self, new_task: Task, new_index: int) -> str:

        task_templates = list(self.template.tasks)
        task_templates.insert(new_index, new_task)
        uniq_names = Task.get_task_unique_names(task_templates)

        return uniq_names[new_index]

    def _add_empty_task(
        self,
        task: Task,
        new_index: Optional[int] = None,
    ) -> WorkflowTask:

        if new_index is None:
            new_index = self.num_tasks

        insert_ID = self.num_added_tasks

        # make a copy with persistent schema inputs:
        task_c, _ = task.to_persistent(self, insert_ID)

        # add to the WorkflowTemplate:
        self.template._add_empty_task(task_c, new_index, insert_ID)

        # create and insert a new WorkflowTask:
        self.tasks.add_object(
            self.app.WorkflowTask.new_empty_task(self, task_c, new_index),
            index=new_index,
        )

        # update persistent store:
        task_js, temp_comps_js = task_c.to_json_like()
        self._store.add_template_components(temp_comps_js)
        self._store.add_empty_task(new_index, task_js)

        # update in-memory workflow template components:
        temp_comps = self.app.template_components_from_json_like(temp_comps_js)
        for comp_type, comps in temp_comps.items():
            for comp in comps:
                comp._set_hash()
                if comp not in self.template_components[comp_type]:
                    idx = self.template_components[comp_type].add_object(comp)
                    self._pending["template_components"][comp_type].append(idx)

        self._pending["tasks"].append(new_index)

        return self.tasks[new_index]

    def _add_task(self, task: Task, new_index: Optional[int] = None) -> None:
        new_wk_task = self._add_empty_task(task=task, new_index=new_index)
        new_wk_task._add_elements(element_sets=task.element_sets)

    def add_task(self, task: Task, new_index: Optional[int] = None) -> None:
        with self._store.cached_load():
            with self.batch_update():
                self._add_task(task, new_index=new_index)

    def add_task_after(self, task_ref):
        # TODO: find position of new task, then call add_task
        # TODO: add new downstream elements?
        pass

    def add_task_before(self, task_ref):
        # TODO: find position of new task, then call add_task
        # TODO: add new downstream elements?
        pass

    def _get_parameter_data(self, index: int) -> Any:
        return self._store.get_parameter_data(index)

    def _get_parameter_source(self, index: int) -> Dict:
        return self._store.get_parameter_source(index)

    def get_all_parameter_data(self) -> Dict[int, Any]:
        return self._store.get_all_parameter_data()

    def is_parameter_set(self, index: int) -> bool:
        return self._store.is_parameter_set(index)

    def check_parameters_exist(
        self, indices: Union[int, List[int]]
    ) -> Union[bool, List[bool]]:
        return self._store.check_parameters_exist(indices)

    def _add_unset_parameter_data(self, source: Dict) -> int:
        return self._store.add_unset_parameter_data(source)

    def _add_parameter_data(self, data, source: Dict) -> int:
        return self._store.add_parameter_data(data, source)

    def _resolve_input_source_task_reference(
        self, input_source: InputSource, new_task_name: str
    ) -> None:
        """Normalise the input source task reference and convert a source to a local type
        if required."""

        # TODO: test thoroughly!

        if isinstance(input_source.task_ref, str):
            if input_source.task_ref == new_task_name:
                if input_source.task_source_type is self.app.TaskSourceType.OUTPUT:
                    raise InvalidInputSourceTaskReference(
                        f"Input source {input_source.to_string()!r} cannot refer to the "
                        f"outputs of its own task!"
                    )
                else:
                    warn(
                        f"Changing input source {input_source.to_string()!r} to a local "
                        f"type, since the input source task reference refers to its own "
                        f"task."
                    )
                    # TODO: add an InputSource source_type setter to reset
                    # task_ref/source_type?
                    input_source.source_type = self.app.InputSourceType.LOCAL
                    input_source.task_ref = None
                    input_source.task_source_type = None
            else:
                try:
                    uniq_names_cur = self.get_task_unique_names(map_to_insert_ID=True)
                    input_source.task_ref = uniq_names_cur[input_source.task_ref]
                except KeyError:
                    raise InvalidInputSourceTaskReference(
                        f"Input source {input_source.to_string()!r} refers to a missing "
                        f"or inaccessible task: {input_source.task_ref!r}."
                    )

    def get_task_elements(self, task: Task, selection: slice) -> List[Element]:
        return [
            self.app.Element(task=task, **i)
            for i in self._store.get_task_elements(task.index, task.insert_ID, selection)
        ]

    def get_task_elements_islice(self, task: Task, selection: slice) -> Iterator[Element]:
        for i in self._store.get_task_elements_islice(
            task.index, task.insert_ID, selection
        ):
            yield self.app.Element(task=task, **i)

    def get_EARs_from_indices(
        self, indices: List[Tuple[int, int, int, int]]
    ) -> List[ElementActionRun]:
        """Return element action run objects from a list of four-tuples, representing the
        task insert ID, (local) element index, action index, and run index, respectively.
        """
        objs = []
        for src_i in indices:
            task = self.tasks.get(insert_ID=src_i[0])
            EAR_i = task.elements[src_i[1]].actions[src_i[2]].runs[src_i[3]]
            objs.append(EAR_i)
        return objs

    def get_elements_from_indices(self, indices: List[Tuple[int, int]]) -> List[Element]:
        """Return element objects from a list of two-tuples, representing the task insert
        ID, and (local) element index, respectively."""
        return [self.tasks.get(insert_ID=idx[0]).elements[idx[1]] for idx in indices]


@dataclass
class WorkflowBlueprint:
    """Pre-built workflow templates that are simpler to parametrise (e.g. fitting workflows)."""

    workflow_template: WorkflowTemplate
