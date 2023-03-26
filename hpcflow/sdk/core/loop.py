from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import List, Optional

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
    """
    Parameters
    ----------
    name
        Loop name, optional
    tasks
        List of task insert IDs. TODO: accept WorkflowTask objects

    """

    def __init__(
        self,
        tasks: List[int],
        num_iterations: int,
        name: Optional[str] = None,
    ) -> None:

        self.tasks = tasks
        self.num_iterations = num_iterations
        self.name = name

        self.workflow_template = None  # assigned by parent WorkflowTemplate

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
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj.workflow_template = self.workflow_template
        return obj


class WorkflowLoop:
    """Class to represent a Loop that is bound to a Workflow."""

    _app_attr = "app"

    def __init__(
        self,
        workflow: Workflow,
        template: Loop,
    ):
        self._workflow = workflow
        self._template = template

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"

    @property
    def workflow(self):
        return self._workflow

    @property
    def template(self):
        return self._template

    @property
    def name(self):
        return self.template.name
