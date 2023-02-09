from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional

from valida.conditions import ConditionLike
from valida.datapath import DataPath
from valida.rules import Rule

from hpcflow.sdk.core.zarr_io import zarr_decode
from hpcflow.sdk.core.parameters import InputValue
from hpcflow.sdk.core.utils import (
    check_valid_py_identifier,
    get_in_container,
    get_relative_path,
    set_in_container,
)


@dataclass
class ElementInputs:

    _app_attr = "_app"

    element: Element

    def __repr__(self):
        return f"{self.__class__.__name__}(" f"{', '.join(self._get_input_names())}" f")"

    def _get_input_names(self):
        return sorted(self.element.task.template.all_schema_input_types)

    def __getattr__(self, name):
        if name not in self._get_input_names():
            raise ValueError(f"No input named {name!r}.")
        return self.element.get(f"inputs.{name}")

    def __dir__(self):
        return super().__dir__() + self._get_input_names()


@dataclass
class ElementOutputs:

    _app_attr = "_app"

    element: Element

    def __repr__(self):
        return f"{self.__class__.__name__}(" f"{', '.join(self._get_output_names())}" f")"

    def _get_output_names(self):
        return list(self.element.task.template.all_schema_output_types)

    def __getattr__(self, name):
        if name not in self._get_output_names():
            raise ValueError(f"No output named {name!r}.")
        return self.element.get(f"outputs.{name}")

    def __dir__(self):
        return super().__dir__() + self._get_output_names()


@dataclass
class Element:

    _app_attr = "app"

    task: WorkflowTask
    data_index: Dict
    global_index: int

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"task={self.task!r}, global_index={self.global_index!r}"
            f")"
        )

    @property
    def workflow(self):
        return self.task.workflow

    @property
    def inputs(self):
        return self.app.ElementInputs(self)

    @property
    def outputs(self):
        return self.app.ElementOutputs(self)

    @property
    def resources(self):
        return self.app.ResourceList.from_json_like(self.get("resources"))

    @property
    def index(self):
        """Get the index of the element within the task.

        Note: the `global_index` attribute returns the index of the element within the
        workflow, across all tasks."""

        return self.task.elements.index(self)

    @property
    def input_sources(self):
        return {k: v[self.index] for k, v in self.task.element_input_sources.items()}

    @property
    def sequence_value_indices(self):
        return {k: v[self.index] for k, v in self.task.element_sequence_indices.items()}

    @property
    def element_set(self):
        return self.task.template.element_sets[self.task.element_set_indices[self.index]]

    @property
    def task_dependencies(self):
        """Get tasks that this element depends on."""

        dependencies = []
        for elem_idx in self.element_dependencies:
            task_i = self.workflow.elements[elem_idx].task
            if task_i not in dependencies:
                dependencies.append(task_i)

        return dependencies

    @property
    def dependent_tasks(self):
        """Get tasks that depend on this element."""

        dependents = []
        for elem_idx in self.dependent_elements:
            task_i = self.workflow.elements[elem_idx].task
            if task_i not in dependents:
                dependents.append(task_i)

        return dependents

    @property
    def element_dependencies(self):
        """Get indices of elements that this element depends on."""

        dependencies = []

        # get direct dependencies:
        for src in self.input_sources.values():
            if src.startswith("element"):
                elem_idx = int(src.split(".")[1])
                if elem_idx not in dependencies:
                    dependencies.append(elem_idx)

        # get indirect dependencies:
        for elem_idx in dependencies:
            for elem_dep_i in self.workflow.elements[elem_idx].element_dependencies:
                if elem_dep_i not in dependencies:
                    dependencies.append(elem_dep_i)

        return dependencies

    @property
    def dependent_elements(self):
        """Get indices of elements that depend on this element."""
        dependents = []

        # get direct dependents:
        for task in self.task.dependent_tasks:
            for element in task.elements:
                for src in element.input_sources.values():
                    if src.startswith("element"):
                        elem_idx = int(src.split(".")[1])
                        if elem_idx == self.global_index:
                            if element.global_index not in dependents:
                                dependents.append(element.global_index)

        # get indirect dependents:
        for elem_idx in dependents:
            for elem_dep_i in self.workflow.elements[elem_idx].dependent_elements:
                if elem_dep_i not in dependents:
                    dependents.append(elem_dep_i)

        return dependents

    def get_sequence_value(self, sequence_path):
        seq = self.element_set.get_sequence_from_path(sequence_path)
        if not seq:
            raise ValueError(
                f"No sequence with path {sequence_path!r} in this element's originating "
                f"element set."
            )
        val_idx = self.sequence_value_indices[sequence_path]
        val = seq.values[val_idx]

        return val

    @property
    def dir_name(self):
        return str(self.index)

    @property
    def dir_path(self):
        return self.task.dir_path / self.dir_name

    def _path_to_parameter(self, path):
        if len(path) != 2 or path[0] == "resources":
            return

        if path[0] == "inputs":
            for i in self.task.template.schemas:
                for j in i.inputs:
                    if j.parameter.typ == path[1]:
                        return j.parameter

        elif path[0] == "outputs":
            for i in self.task.template.schemas:
                for j in i.outputs:
                    if j.parameter.typ == path[1]:
                        return j.parameter

    def get(self, path: str = None, raise_on_missing=False):
        """Get element data from the persistent store."""

        path = [] if not path else path.split(".")
        parameter = self._path_to_parameter(path)
        current_value = None
        is_cur_val_set = False
        for path_i, data_idx_i in self.data_index.items():

            path_i = path_i.split(".")
            is_parent = False
            is_update = False
            try:
                rel_path = get_relative_path(path, path_i)
                is_parent = True
            except ValueError:
                try:
                    update_path = get_relative_path(path_i, path)
                    is_update = True

                except ValueError:
                    # no intersection between paths
                    continue

            zarr_group = self.workflow.get_zarr_parameter_group(data_idx_i)
            data = zarr_decode(zarr_group)

            if is_parent:
                # replace current value:
                try:
                    current_value = get_in_container(data, rel_path, cast_indices=True)
                    is_cur_val_set = True
                except (KeyError, IndexError, ValueError):
                    continue

            elif is_update:
                # update sub-part of current value
                current_value = current_value or {}
                set_in_container(current_value, update_path, data, ensure_path=True)
                is_cur_val_set = True

        if raise_on_missing and not is_cur_val_set:
            raise ValueError(f"Path {path} does not exist in the element data.")

        if parameter and parameter._value_class:
            current_value = parameter._value_class(**current_value)

        return current_value

    def to_element_set_data(self):
        """Generate lists of workflow-bound InputValues and ResourceList."""
        inputs = []
        resources = []
        for k, v in self.data_index.items():

            k_s = k.split(".")

            if k_s[0] == "inputs":
                inp_val = self.app.InputValue(
                    parameter=k_s[1],
                    path=k_s[2:] or None,
                    value=None,
                )
                inp_val._value_group_idx = v
                inp_val._workflow = self.workflow
                inputs.append(inp_val)

            elif k_s[0] == "resources":
                scope = self.app.ActionScope.from_json_like(k_s[1])
                res = self.app.ResourceSpec(scope=scope)
                res._value_group_idx = v
                res._workflow = self.workflow
                resources.append(res)

        return inputs, resources

    def resolve_actions(self):
        """Return a list of `ElementAction`s given the associated schema(s) and particular
        parametrisation of this element."""
        element_actions = []
        for schema in self.task.template.schemas:
            # TODO: add a TaskSchema.resolve_actions method?
            for action in schema.actions:
                element_actions.extend(action.resolve_element_actions(element=self))
        return tuple(element_actions)

    def test_rule(self, rule: Rule) -> bool:
        """Test a rule on this element data."""

        param_path = ".".join(
            i.condition.callable.kwargs["value"] for i in rule.path.parts[:2]
        )
        elem_dat = self.get(param_path)

        data_path = DataPath(*rule.path.parts[2:])
        rule = Rule(path=data_path, condition=rule.condition, cast=rule.cast)

        return rule.test(elem_dat).is_valid


@dataclass
class ElementFilter:

    parameter_path: ParameterPath
    condition: ConditionLike


@dataclass
class ElementGroup:

    name: str
    where: Optional[ElementFilter] = None
    group_by_distinct: Optional[ParameterPath] = None

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


@dataclass
class ElementRepeats:

    number: int
    where: Optional[ElementFilter] = None
