from __future__ import annotations

import copy
from typing import Dict, List, Optional, Tuple, Union

from hpcflow.sdk import app
from hpcflow.sdk.core.json_like import JSONLike
from hpcflow.sdk.core.parameters import InputSourceType
from hpcflow.sdk.core.task import WorkflowTask
from hpcflow.sdk.core.utils import check_valid_py_identifier

# from .parameters import Parameter

# from valida.conditions import ConditionLike


# @dataclass
# class StoppingCriterion:
#     parameter: Parameter
#     condition: ConditionLike


# @dataclass
# class Loop:
#     parameter: Parameter
#     stopping_criteria: StoppingCriterion  # TODO: should be a logical combination of these (maybe provide a superclass in valida to re-use some logic there?)
#     maximum_iterations: int


class Loop(JSONLike):
    _app_attr = "app"

    def __init__(
        self,
        tasks: List[Union[int, app.WorkflowTask]],
        num_iterations: int,
        name: Optional[str] = None,
        non_iterable_parameters: Optional[List[str]] = None,
    ) -> None:
        """

        Parameters
        ----------
        name
            Loop name, optional
        tasks
            List of task insert IDs or WorkflowTask objects
        non_iterable_parameters
            Specify input parameters that should not iterate.

        """

        _task_insert_IDs = []
        for task in tasks:
            if isinstance(task, WorkflowTask):
                _task_insert_IDs.append(task.insert_ID)
            elif isinstance(task, int):
                _task_insert_IDs.append(task)
            else:
                raise TypeError(
                    f"`tasks` must be a list whose elements are either task insert IDs "
                    f"or WorkflowTask objects, but received the following: {tasks!r}."
                )

        self._task_insert_IDs = _task_insert_IDs
        self._num_iterations = num_iterations
        self._name = check_valid_py_identifier(name) if name else name
        self._non_iterable_parameters = non_iterable_parameters or []

        self._workflow_template = None  # assigned by parent WorkflowTemplate

    def to_dict(self):
        out = super().to_dict()
        return {k.lstrip("_"): v for k, v in out.items()}

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        if "task_insert_IDs" in json_like:
            insert_IDs = json_like.pop("task_insert_IDs")
        else:
            insert_IDs = json_like.pop("tasks")
        obj = cls(tasks=insert_IDs, **json_like)
        return obj

    @property
    def task_insert_IDs(self) -> Tuple[int]:
        """Get the list of task insert_IDs that define the extent of the loop."""
        return tuple(self._task_insert_IDs)

    @property
    def name(self):
        return self._name

    @property
    def num_iterations(self):
        return self._num_iterations

    @property
    def non_iterable_parameters(self):
        return self._non_iterable_parameters

    @property
    def workflow_template(self):
        return self._workflow_template

    @workflow_template.setter
    def workflow_template(self, template: app.WorkflowTemplate):
        self._workflow_template = template
        self._validate_against_template()

    @property
    def task_objects(self) -> Tuple[app.WorkflowTask]:
        if not self.workflow_template:
            raise RuntimeError(
                "Workflow template must be assigned to retrieve task objects of the loop."
            )
        return tuple(
            self.workflow_template.workflow.tasks.get(insert_ID=i)
            for i in self.task_insert_IDs
        )

    def _validate_against_template(self):
        """Validate the loop parameters against the associated workflow."""

        # insert IDs must exist:
        for insert_ID in self.task_insert_IDs:
            try:
                self.workflow_template.workflow.tasks.get(insert_ID=insert_ID)
            except ValueError:
                raise ValueError(
                    f"Loop {self.name!r} has an invalid task insert ID {insert_ID!r}. "
                    f"Such as task does not exist in the associated workflow."
                )

    def __repr__(self):
        num_iterations_str = ""
        if self.num_iterations is not None:
            num_iterations_str = f", num_iterations={self.num_iterations!r}"

        name_str = ""
        if self.name:
            name_str = f", name={self.name!r}"

        return (
            f"{self.__class__.__name__}("
            f"task_insert_IDs={self.task_insert_IDs!r}{num_iterations_str}{name_str}"
            f")"
        )

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        kwargs["tasks"] = kwargs.pop("task_insert_IDs")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._workflow_template = self._workflow_template
        return obj


class WorkflowLoop:
    """Class to represent a Loop that is bound to a Workflow."""

    _app_attr = "app"

    def __init__(
        self,
        index: int,
        workflow: app.Workflow,
        template: app.Loop,
        num_added_iterations: int,
        iterable_parameters: Dict[int : List[int, List[int]]],
    ):
        self._index = index
        self._workflow = workflow
        self._template = template
        self._num_added_iterations = num_added_iterations
        self._iterable_parameters = iterable_parameters

        # incremented when a new loop iteration is added, reset on dump to disk:
        self._pending_num_added_iterations = 0

        self._validate()

    def _validate(self):
        # task subset must be a contiguous range of task indices:
        task_indices = self.task_indices
        task_min, task_max = task_indices[0], task_indices[-1]
        if task_indices != tuple(range(task_min, task_max + 1)):
            raise ValueError(f"Loop task subset must be a contiguous range")

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(template={self.template!r}, "
            f"num_added_iterations={self.num_added_iterations!r})"
        )

    def _reset_pending_num_added_iters(self):
        self._pending_num_added_iterations = 0

    def _accept_pending_num_added_iters(self):
        self._num_added_iterations = self.num_added_iterations
        self._reset_pending_num_added_iters()

    @property
    def index(self):
        return self._index

    @property
    def task_insert_IDs(self):
        return self.template.task_insert_IDs

    @property
    def task_objects(self):
        return self.template.task_objects

    @property
    def task_indices(self) -> Tuple[int]:
        """Get the list of task indices that define the extent of the loop."""
        return tuple(i.index for i in self.task_objects)

    @property
    def workflow(self):
        return self._workflow

    @property
    def template(self):
        return self._template

    @property
    def name(self):
        return self.template.name

    @property
    def iterable_parameters(self):
        return self._iterable_parameters

    @property
    def num_iterations(self):
        return self.template.num_iterations

    @property
    def num_added_iterations(self):
        return self._num_added_iterations + self._pending_num_added_iterations

    @staticmethod
    def _find_iterable_parameters(loop_template: app.Loop):
        all_inputs_first_idx = {}
        all_outputs_idx = {}
        for task in loop_template.task_objects:
            for typ in task.template.all_schema_input_types:
                if typ not in all_inputs_first_idx:
                    all_inputs_first_idx[typ] = task.insert_ID
            for typ in task.template.all_schema_output_types:
                if typ not in all_outputs_idx:
                    all_outputs_idx[typ] = []
                all_outputs_idx[typ].append(task.insert_ID)

        all_inputs_first_idx, all_outputs_idx

        iterable_params = {}
        for typ, first_idx in all_inputs_first_idx.items():
            if typ in all_outputs_idx and first_idx <= all_outputs_idx[typ][0]:
                iterable_params[typ] = {
                    "input_task": first_idx,
                    "output_tasks": all_outputs_idx[typ],
                }

        for non_iter in loop_template.non_iterable_parameters:
            if non_iter in iterable_params:
                del iterable_params[non_iter]

        return iterable_params

    @classmethod
    def new_empty_loop(cls, index: int, workflow: app.Workflow, template: app.Loop):
        obj = cls(
            index=index,
            workflow=workflow,
            template=template,
            num_added_iterations=1,
            iterable_parameters=cls._find_iterable_parameters(template),
        )
        return obj

    def get_parent_loops(self) -> List[app.WorkflowLoop]:
        """Get loops whose task subset is a superset of this loop's task subset. If two
        loops have identical task subsets, the first loop in the workflow loop index is
        considered the parent."""
        parents = []
        passed_self = False
        self_tasks = set(self.task_insert_IDs)
        for loop_i in self.workflow.loops:
            if loop_i.index == self.index:
                passed_self = True
                continue
            other_tasks = set(loop_i.task_insert_IDs)
            if self_tasks.issubset(other_tasks):
                if (self_tasks == other_tasks) and passed_self:
                    continue
                parents.append(loop_i)
        return parents

    def get_child_loops(self) -> List[app.WorkflowLoop]:
        """Get loops whose task subset is a subset of this loop's task subset. If two
        loops have identical task subsets, the first loop in the workflow loop index is
        considered the parent."""
        children = []
        passed_self = False
        self_tasks = set(self.task_insert_IDs)
        for loop_i in self.workflow.loops:
            if loop_i.index == self.index:
                passed_self = True
                continue
            other_tasks = set(loop_i.task_insert_IDs)
            if self_tasks.issuperset(other_tasks):
                if (self_tasks == other_tasks) and not passed_self:
                    continue
                children.append(loop_i)
        return children

    def add_iteration(self, parent_loop_indices=None):
        parent_loop_indices = parent_loop_indices or {}
        cur_loop_idx = self.num_added_iterations - 1
        parent_loops = self.get_parent_loops()
        child_loops = self.get_child_loops()

        for parent_loop in parent_loops:
            if parent_loop.name not in parent_loop_indices:
                raise ValueError(
                    f"Parent loop {parent_loop.name!r} must be specified in "
                    f"`parent_loop_indices`."
                )
        all_new_data_idx = {}  # keys are (task.insert_ID and element.index)

        for task in self.task_objects:
            for elem_idx in range(task.num_elements):
                # element needs to take into account changes made in this code
                element = task.elements[elem_idx]

                new_data_idx = {}

                # copy resources from zeroth iteration:
                for key, val in element.iterations[0].get_data_idx().items():
                    if key.startswith("resources."):
                        new_data_idx[key] = val

                for inp in task.template.all_schema_inputs:
                    is_inp_task = False
                    iter_dat = self.iterable_parameters.get(inp.typ)
                    if iter_dat:
                        is_inp_task = task.insert_ID == iter_dat["input_task"]

                    if is_inp_task:
                        # source from final output task of previous iteration, with all parent
                        # loop indices the same as previous iteration, and all child loop indices
                        # maximised:

                        # identify element(s) from which this iterable input should be
                        # parametrised:
                        if task.insert_ID == iter_dat["output_tasks"][-1]:
                            src_elem = element
                        else:
                            src_elems = element.get_dependent_elements_recursively(
                                task_insert_ID=iter_dat["output_tasks"][-1]
                            )
                            if len(src_elems) > 1:
                                raise NotImplementedError(
                                    f"Multiple elements found in the iterable parameter {inp!r}'s"
                                    f" latest output task (insert ID: "
                                    f"{iter_dat['output_tasks'][-1]}) that can be used to "
                                    f"parametrise the next iteration."
                                )
                            elif not src_elems:
                                # TODO: maybe OK?
                                raise NotImplementedError(
                                    f"No elements found in the iterable parameter {inp!r}'s"
                                    f" latest output task (insert ID: "
                                    f"{iter_dat['output_tasks'][-1]}) that can be used to "
                                    f"parametrise the next iteration."
                                )
                            src_elem = src_elems[0]

                        child_loop_max_iters = {
                            i.name: i.num_added_iterations - 1 for i in child_loops
                        }
                        parent_loop_same_iters = {
                            i.name: parent_loop_indices[i.name] for i in parent_loops
                        }
                        source_iter_loop_idx = {
                            **child_loop_max_iters,
                            **parent_loop_same_iters,
                            self.name: cur_loop_idx,
                        }

                        # identify the ElementIteration from which this input should be
                        # parametrised:
                        source_iter = None
                        for iter_i in src_elem.iterations:
                            if iter_i.loop_idx == source_iter_loop_idx:
                                source_iter = iter_i
                                break

                        inp_dat_idx = source_iter.get_data_idx()[f"outputs.{inp.typ}"]
                        new_data_idx[f"inputs.{inp.typ}"] = inp_dat_idx

                    else:
                        inp_key = f"inputs.{inp.typ}"

                        orig_inp_src = element.input_sources[inp_key]
                        inp_dat_idx = None

                        if orig_inp_src.source_type is InputSourceType.LOCAL:
                            # keep locally defined inputs from original element
                            inp_dat_idx = element.iterations[0].get_data_idx()[inp_key]

                        elif orig_inp_src.source_type is InputSourceType.TASK:
                            # same task/element, but update iteration to the just-added
                            # iteration:
                            for (tiID, e_idx), prev_dat_idx in all_new_data_idx.items():
                                if tiID == orig_inp_src.task_ref:
                                    # find which element in that task `element` depends
                                    # on:
                                    task_i = self.workflow.tasks.get(insert_ID=tiID)
                                    elem_i = task_i.elements[e_idx]
                                    src_elems_i = (
                                        elem_i.get_dependent_elements_recursively(
                                            task_insert_ID=task.insert_ID
                                        )
                                    )
                                    if (
                                        len(src_elems_i) == 1
                                        and src_elems_i[0].id_ == element.id_
                                    ):
                                        inp_dat_idx = prev_dat_idx[
                                            f"{orig_inp_src.task_source_type.name.lower()}s.{inp.typ}"
                                        ]

                        if inp_dat_idx is None:
                            raise RuntimeError(
                                f"Could not find a source for parameter {inp.typ} "
                                f"when adding a new iteration for task {task!r}."
                            )

                        new_data_idx[inp_key] = inp_dat_idx

                for out in task.template.all_schema_outputs:
                    path_i = f"outputs.{out.typ}"
                    p_src = {
                        "type": "EAR_output",
                        # "task_insert_ID": task.insert_ID,
                        # "element_idx": element.index,
                        # "run_idx": 0,
                    }
                    new_data_idx[path_i] = self.workflow._add_unset_parameter_data(p_src)

                schema_params = set(
                    i for i in new_data_idx.keys() if len(i.split(".")) == 2
                )
                all_new_data_idx[(task.insert_ID, element.index)] = new_data_idx
                iter_ID_i = self.workflow._store.add_element_iteration(
                    element_ID=element.id_,
                    data_idx=new_data_idx,
                    schema_parameters=list(schema_params),
                    loop_idx={**parent_loop_indices, self.name: cur_loop_idx + 1},
                )

                task.initialise_EARs()

        self._pending_num_added_iterations += 1
        self.workflow._store.update_loop_num_iters(
            index=self.index,
            num_iters=self.num_added_iterations,
        )
