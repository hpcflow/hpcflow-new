from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional

from valida.conditions import ConditionLike
from hpcflow.sdk.core.zarr_io import zarr_decode
from hpcflow.sdk.core.utils import (
    check_valid_py_identifier,
    get_in_container,
    get_relative_path,
    set_in_container,
)


@dataclass
class ElementInputs:

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

    task: Task
    data_index: Dict

    @property
    def workflow(self):
        return self.task.workflow

    @property
    def inputs(self):
        return ElementInputs(self)

    @property
    def outputs(self):
        return ElementOutputs(self)

    @property
    def resources(self):
        return self.app.ResourceList.from_json_like(self.get("resources"))

    @property
    def index(self):
        return self.task.elements.index(self)

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

    def get(self, path: str = None):
        """Get element data from the persistent store."""

        path = [] if not path else path.split(".")
        parameter = self._path_to_parameter(path)
        current_value = None
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
                    continue

            zarr_group = self.workflow.get_zarr_parameter_group(data_idx_i)
            data = zarr_decode(zarr_group)

            if is_parent:
                # replace current value:
                try:
                    current_value = get_in_container(data, rel_path)
                except (KeyError, IndexError):
                    continue

            elif is_update:
                # update sub-part of current value
                current_value = current_value or {}
                set_in_container(current_value, update_path, data, ensure_path=True)

        if parameter and parameter._value_class:
            current_value = parameter._value_class(**current_value)

        return current_value

    def resolve_actions(self):
        """Return a list of `ElementAction`s given the associated schema(s) and particular
        parametrisation of this element."""
        element_actions = []
        for schema in self.task.template.schemas:
            # TODO: add a TaskSchema.resolve_actions method?
            for action in schema.actions:
                element_actions.extend(action.resolve_element_actions(element=self))
        return tuple(element_actions)


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
