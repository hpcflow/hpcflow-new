from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

from hpcflow.sdk.core.json_like import JSONLike

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
        tasks: List[Union[int, WorkflowTask]],
        num_iterations: int,
        name: Optional[str] = None,
    ) -> None:
        """

        Parameters
        ----------
        name
            Loop name, optional
        tasks
            List of task insert IDs or WorkflowTask objects

        """

        _task_insert_IDs = []
        for task in tasks:
            if isinstance(task, self.app.WorkflowTask):
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
        self._name = name

        self._workflow_template = None  # assigned by parent WorkflowTemplate

    def to_dict(self):
        out = super().to_dict()
        return {k.lstrip("_"): v for k, v in out.items()}

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        insert_IDs = json_like.pop("task_insert_IDs")
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
    def workflow_template(self):
        return self._workflow_template

    @workflow_template.setter
    def workflow_template(self, template: WorkflowTemplate):
        self._workflow_template = template
        self._validate_against_template()

    @property
    def task_objects(self) -> Tuple[WorkflowTask]:
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
            f"tasks={self.tasks!r}{num_iterations_str}{name_str}"
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
        workflow: Workflow,
        template: Loop,
        num_added_iterations: int,
        iterable_parameters: Dict[int : List[int, List[int]]],
    ):
        self._index = index
        self._workflow = workflow
        self._template = template
        self._num_added_iterations = num_added_iterations
        self._iterable_parameters = iterable_parameters

        self._validate()

    def _validate(self):
        # task subset must be a contiguous range of task indices:
        task_indices = self.task_indices
        task_min, task_max = task_indices[0], task_indices[-1]
        if task_indices != tuple(range(task_min, task_max + 1)):
            raise ValueError(f"Loop task subset must be a contiguous range")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

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
        return self._num_added_iterations

    @staticmethod
    def _find_iterable_parameters(loop_template: Loop):
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

        return iterable_params

    @classmethod
    def new_empty_loop(cls, index: int, workflow: Workflow, template: Loop):
        obj = cls(
            index=index,
            workflow=workflow,
            template=template,
            num_added_iterations=1,
            iterable_parameters=cls._find_iterable_parameters(template),
        )
        return obj

    def get_parent_loops(self) -> List[WorkflowLoop]:
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

    def get_child_loops(self):
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
