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
from hpcflow.sdk.core.actions import EAR_ID
from hpcflow.sdk.core.submission import (
    generate_EAR_resource_map,
    group_resource_map_into_jobscripts,
)

from hpcflow.sdk.typing import E_idx_type, EAR_idx_type, EI_idx_type, PathLike


from .json_like import ChildObjectSpec, JSONLike
from .parameters import InputSource
from .task import ElementSet, Task
from .utils import get_md5_hash, read_YAML, read_YAML_file
from .errors import (
    InvalidInputSourceTaskReference,
    LoopAlreadyExistsError,
    WorkflowBatchUpdateFailedError,
)

from hpcflow.sdk.persistence.json import JSONPersistentStore
from hpcflow.sdk.persistence.zarr import ZarrPersistentStore

TS_NAME_FMT = r"%Y-%m-%d_%H%M%S"


class _DummyPersistentWorkflow:
    """An object to pass to ResourceSpec.make_persistent that pretends to be a
    Workflow object, so we can pretend to make template-level inputs/resources
    persistent before the workflow exists."""

    def __init__(self):
        self._parameters = []
        self._sources = []

    def _add_parameter_data(self, data, source: Dict) -> int:
        self._parameters.append(data)
        self._sources.append(source)
        data_ref = len(self._parameters) - 1
        return data_ref

    def make_persistent(self, workflow: Workflow):
        for dat_i, source_i in zip(self._parameters, self._sources):
            workflow._add_parameter_data(dat_i, source_i)


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
        ChildObjectSpec(
            name="loops",
            class_name="Loop",
            is_multiple=True,
            parent_ref="_workflow_template",
        ),
        ChildObjectSpec(
            name="resources",
            class_name="ResourceList",
            parent_ref="_workflow_template",
        ),
    )

    name: str
    tasks: Optional[List[Task]] = field(default_factory=lambda: [])
    loops: Optional[List[Loop]] = field(default_factory=lambda: [])
    workflow: Optional[Workflow] = None
    resources: Optional[Dict[str, Dict]] = None

    def __post_init__(self):

        if isinstance(self.resources, dict):
            self.resources = self.app.ResourceList.from_json_like(self.resources)
        elif isinstance(self.resources, list):
            self.resources = self.app.ResourceList(self.resources)
        elif not self.resources:
            self.resources = self.app.ResourceList([self.app.ResourceSpec()])

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

    def _add_empty_loop(self, loop: Loop) -> None:
        """Called by `Workflow._add_empty_loop`."""

        if not loop.name:
            existing = [i.name for i in self.loops]
            new_idx = len(self.loops)
            name = f"loop_{new_idx}"
            while name in existing:
                new_idx += 1
                name = f"loop_{new_idx}"
            loop._name = name
        elif loop.name in self.workflow.loops.list_attrs():
            raise LoopAlreadyExistsError(
                f"A loop with the name {loop.name!r} already exists in the workflow: "
                f"{getattr(self.workflow.loops, loop.name)!r}."
            )

        loop._workflow_template = self
        self.loops.append(loop)


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

    _default_ts_fmt = "%Y-%m-%d %H:%M:%S.%f"

    def __init__(self, path: PathLike) -> None:
        self.path = Path(path)

        # assigned on first access to corresponding properties:
        self._template = None
        self._template_components = None
        self._tasks = None
        self._loops = None

        self._store = self._get_store_class_from_ext(self.path)(self)

        self._in_batch_mode = False  # flag to track when processing batch updates

        # store indices of updates during batch update, so we can revert on failure:
        self._pending = self._get_empty_pending()

    def _get_empty_pending(self) -> Dict:
        return {
            "template_components": {k: [] for k in self.app._template_component_types},
            "tasks": [],  # list of int
            "loops": [],  # list of int
        }

    def _accept_pending(self) -> None:
        self._reset_pending()

    def _reset_pending(self) -> None:
        self._pending = self._get_empty_pending()

    def _reject_pending(self) -> None:
        """Revert pending changes to the in-memory representation of the workflow.

        This deletes new tasks, new template component data, and new loops. Element
        additions to existing (non-pending) tasks are separately rejected/accepted by the
        WorkflowTask object.

        """
        for task_idx in self._pending["tasks"][::-1]:
            # iterate in reverse so the index references are correct
            self.tasks._remove_object(task_idx)
            self.template.tasks.pop(task_idx)

        for comp_type, comp_indices in self._pending["template_components"].items():
            for comp_idx in comp_indices[::-1]:
                # iterate in reverse so the index references are correct
                self.template_components[comp_type]._remove_object(comp_idx)

        for loop_idx in self._pending["loops"][::-1]:
            # iterate in reverse so the index references are correct
            self.loops._remove_object(loop_idx)
            self.template.loops.pop(loop_idx)

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
                for loop in template.loops:
                    wk._add_loop(loop)
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

                for loop in self.loops:
                    loop._reset_pending_num_added_iters()

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

                    for loop in self.loops:
                        loop._accept_pending_num_added_iters()

                    self._store.remove_replaced_file()
                    # TODO: handle errors in commit pending?
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

        # make template-level inputs/resources think they are persistent:
        wk_dummy = _DummyPersistentWorkflow()
        param_src = {"type": "workflow_resources"}
        for res_i in template.resources:
            res_i.make_persistent(wk_dummy, param_src)

        store_cls = cls._persistent_store_cls_lookup[ext]
        template_js, template_sh = template.to_json_like(exclude=["tasks", "loops"])
        template_js["tasks"] = []
        template_js["loops"] = []
        store_cls.write_empty_workflow(template_js, template_sh, path, overwrite)
        wk = cls(path)

        # actually make template inputs/resources persistent, now the workflow exists:
        wk_dummy.make_persistent(wk)

        return wk

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
    def num_element_iterations(self) -> int:
        return sum(task.num_element_iterations for task in self.tasks)

    @property
    def num_loops(self) -> int:
        return len(self.loops)

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
                        num_element_iterations=i["num_element_iterations"],
                        num_EARs=i["num_EARs"],
                    )
                    wk_tasks.append(wk_task)
                self._tasks = self.app.WorkflowTaskList(wk_tasks)
        return self._tasks

    @property
    def loops(self) -> WorkflowLoopList:
        if self._loops is None:
            with self._store.cached_load():
                wk_loops = []
                for idx, loop_dat in enumerate(self._store.get_loops()):
                    wk_loop = self.app.WorkflowLoop(
                        index=idx,
                        workflow=self,
                        template=self.template.loops[idx],
                        **loop_dat,
                    )
                    wk_loops.append(wk_loop)
                self._loops = self.app.WorkflowLoopList(wk_loops)
        return self._loops

    @property
    def _timestamp_format(self) -> int:
        # TODO: allow customisation on workflow creation
        return self._default_ts_fmt

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

    def _add_empty_loop(self, loop: Loop) -> WorkflowLoop:
        """Add a new loop (zeroth iterations only) to the workflow."""

        new_index = self.num_loops

        # don't modify passed object:
        loop_c = copy.deepcopy(loop)

        # add to the WorkflowTemplate:
        self.template._add_empty_loop(loop_c)

        # create and insert a new WorkflowLoop:
        self.loops.add_object(
            self.app.WorkflowLoop.new_empty_loop(
                index=new_index,
                workflow=self,
                template=loop_c,
            )
        )
        wk_loop = self.loops[new_index]

        # update persistent store:
        loop_js, _ = loop_c.to_json_like()
        task_indices = [self.tasks.get(insert_ID=i).index for i in loop_c.task_insert_IDs]
        self._store.add_loop(
            task_indices=task_indices,
            loop_js=loop_js,
            iterable_parameters=wk_loop.iterable_parameters,
        )

        self._pending["loops"].append(new_index)

        return wk_loop

    def _add_loop(self, loop: Loop) -> None:
        new_wk_loop = self._add_empty_loop(loop)
        if loop.num_iterations is not None:
            # fixed number of iterations, so add remaining N > 0 iterations:
            for _ in range(loop.num_iterations - 1):
                new_wk_loop.add_iteration()

    def add_loop(self, loop: Loop) -> None:
        """Add a loop to a subset of workflow tasks."""
        with self._store.cached_load():
            with self.batch_update():
                self._add_loop(loop)

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

    def _get_parameter_data(self, index: int) -> Tuple[bool, Any]:
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

    def get_EARs_from_IDs(self, indices: List[EAR_ID]) -> List[ElementActionRun]:
        """Return element action run objects from a list of five-tuples, representing the
        task insert ID, element index, iteration index, action index, and run index,
        respectively.
        """
        objs = []
        for _EAR_ID in indices:
            task = self.tasks.get(insert_ID=_EAR_ID.task_insert_ID)
            elem_iters = task.elements[_EAR_ID.element_idx].iterations
            for i in elem_iters:
                if i.index == _EAR_ID.iteration_idx:
                    iter_i = i
                    break
            EAR_i = iter_i.actions[_EAR_ID.action_idx].runs[_EAR_ID.run_idx]
            objs.append(EAR_i)
        return objs

    def get_element_iterations_from_IDs(
        self, indices: List[IterationID]
    ) -> List[ElementIteration]:
        """Return element iteration objects from a list of three-tuples, representing the
        task insert ID, element index, and iteration index, respectively.
        """
        objs = []
        for iter_idx in indices:
            iter_i = (
                self.tasks.get(insert_ID=iter_idx.task_insert_ID)
                .elements[iter_idx.element_idx]
                .iterations[iter_idx.iteration_idx]
            )
            objs.append(iter_i)
        return objs

    def get_elements_from_IDs(self, indices: List[ElementID]) -> List[Element]:
        """Return element objects from a list of two-tuples, representing the task insert
        ID, and element index, respectively."""
        return [
            self.tasks.get(insert_ID=idx.task_insert_ID).elements[idx.element_idx]
            for idx in indices
        ]

    def set_EAR_start(
        self,
        task_insert_ID,
        element_iteration_idx,
        action_idx,
        run_idx,
    ) -> None:
        """Set the start time on an EAR."""
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EAR_start(
                    task_insert_ID,
                    element_iteration_idx,
                    action_idx,
                    run_idx,
                )

    def set_EAR_end(
        self,
        task_insert_ID,
        element_iteration_idx,
        action_idx,
        run_idx,
    ) -> None:
        """Set the end time on an EAR."""
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EAR_end(
                    task_insert_ID,
                    element_iteration_idx,
                    action_idx,
                    run_idx,
                )

    def resolve_jobscripts(self):

        submission_jobscripts = []
        for task in self.tasks:
            res, res_hash, res_map, EAR_map = generate_EAR_resource_map(task)
            jobscripts, _ = group_resource_map_into_jobscripts(res_map)

            for js_dat in jobscripts:

                js_i = {
                    "task_insert_ID": task.insert_ID,
                    "loop_idx": None,  # TODO
                    "EARs": {},
                    "elements": {},
                    "resources": res[js_dat["resources"]],
                    "resource_hash": res_hash[js_dat["resources"]],
                }
                dep_elem_map = {}
                js_deps = {}
                js_array_dep = True
                for elem_idx, act_indices in js_dat["elements"].items():
                    js_i["elements"][elem_idx] = []
                    all_EAR_IDs = []
                    for act_idx in act_indices:
                        EAR_idx, run_idx, iter_idx = EAR_map[act_idx, elem_idx]
                        # construct EAR_ID object so we can retrieve the EAR objects and
                        # so their dependencies:
                        EAR_id = EAR_ID(
                            task_insert_ID=task.insert_ID,
                            element_idx=elem_idx,
                            iteration_idx=iter_idx,
                            action_idx=act_idx,
                            run_idx=run_idx,
                            EAR_idx=EAR_idx,
                        )
                        all_EAR_IDs.append(EAR_id)
                        js_i["EARs"][EAR_idx] = (
                            task.insert_ID,
                            iter_idx,
                            act_idx,
                            run_idx,
                        )
                        js_i["elements"][elem_idx].append(EAR_idx)

                    # get indices of EARs that this element depends on:
                    EAR_objs = self.get_EARs_from_IDs(all_EAR_IDs)
                    EAR_deps = [i.get_EAR_dependencies() for i in EAR_objs]
                    EAR_deps_flat = [j for i in EAR_deps for j in i]
                    EAR_deps_EAR_idx = [
                        (i.task_insert_ID, i.element_idx, i.EAR_idx)
                        for i in EAR_deps_flat
                    ]

                    # find jobscript dependencies:
                    for dep_task_ID, dep_elem_idx, dep_EAR_idx in EAR_deps_EAR_idx:

                        # loop over jobscripts added so far:
                        for js_j_idx, js_j in enumerate(submission_jobscripts):
                            if (
                                dep_task_ID == js_j["task_insert_ID"]
                                and dep_EAR_idx in js_j["EARs"]
                            ):
                                if js_j_idx not in dep_elem_map:
                                    dep_elem_map[js_j_idx] = {}

                                if js_j_idx not in js_deps:
                                    js_deps[js_j_idx] = {
                                        "is_array": None,
                                        "element_map": None,
                                    }

                                if not js_array_dep:
                                    break

                                if elem_idx in dep_elem_map[js_j_idx]:
                                    # the element of the new jobscript depends on more
                                    # than one element of the previous jobscript
                                    # (js_j_idx), so cannot be an array dependency:
                                    js_array_dep = False
                                    break

                                dep_elem_map[js_j_idx][elem_idx] = dep_elem_idx

                                js_deps[js_j_idx]["is_array"] = js_array_dep
                                js_deps[js_j_idx]["element_map"] = dep_elem_map[js_j_idx]

                # For array dependency, all elements of the new jobscript must be
                # specified in the element dependency map keys, and all elements in the
                # dependency jobscript must be specified in the element dependency map
                # values. Together with the previous check, this ensures a one-to-one
                # mapping.
                for js_dep_idx, dep_info in js_deps.items():
                    if dep_info["is_array"]:
                        if set(js_dat["elements"].keys()) != set(
                            dep_info["element_map"].keys()
                        ):
                            dep_info["is_array"] = False
                        if set(
                            submission_jobscripts[js_dep_idx]["elements"].keys()
                        ) != set(dep_info["element_map"].values()):
                            dep_info["is_array"] = False

                js_i["dependencies"] = js_deps
                submission_jobscripts.append(js_i)

        return submission_jobscripts


@dataclass
class WorkflowBlueprint:
    """Pre-built workflow templates that are simpler to parametrise (e.g. fitting workflows)."""

    workflow_template: WorkflowTemplate
