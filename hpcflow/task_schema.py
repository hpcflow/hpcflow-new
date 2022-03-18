from dataclasses import dataclass, field
from typing import List, Optional, Union

from hpcflow.actions import Action
from hpcflow.errors import MissingActionsError
from hpcflow.parameters import (
    Parameter,
    ParameterPropagationMode,
    SchemaInput,
    SchemaOutput,
    SchemaParameter,
)

from hpcflow.utils import check_valid_py_identifier


@dataclass
class TaskObjective:
    name: str

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


@dataclass
class TaskSchema:

    # TODO: build comprehensive test suite for TaskSchema
    # - decide how schema inputs and outputs are linked to action inputs and outputs?
    # - should the appearance of parameters in the actions determined schema inputs/outputs
    #   - still need to define e.g. defaults, so makes sense to keep inputs/outputs as schema
    #     parameters, and then verify that all action parameters are taken from schema parameters.
    # - should command files be listed as part of the schema? probably, yes.

    objective: Union[TaskObjective, str]
    actions: List[Action]
    method: Optional[str] = None
    implementation: Optional[str] = None
    inputs: Optional[List[Union[Parameter, SchemaInput]]] = field(
        default_factory=lambda: []
    )
    outputs: Optional[List[Union[Parameter, SchemaOutput]]] = field(
        default_factory=lambda: []
    )

    def __post_init__(self):

        self._validate()

    def _validate(self):

        if isinstance(self.objective, str):
            self.objective = TaskObjective(self.objective)

        if self.method:
            self.method = check_valid_py_identifier(self.method)
        if self.implementation:
            self.implementation = check_valid_py_identifier(self.implementation)

        # coerce Parameters to SchemaInputs
        for idx, i in enumerate(self.inputs):
            if isinstance(i, Parameter):
                self.inputs[idx] = SchemaInput(i)

        # coerce Parameters to SchemaOutputs
        for idx, i in enumerate(self.outputs):
            if isinstance(i, Parameter):
                self.outputs[idx] = SchemaOutput(i)

        if not self.actions:
            raise MissingActionsError("A task schema must define at least one Action.")

    @property
    def input_types(self):
        return tuple(i.typ for i in self.inputs)

    @property
    def output_types(self):
        return tuple(i.typ for i in self.outputs)

    @property
    def provides_parameters(self):
        return tuple(
            i
            for i in self.inputs + self.outputs
            if i.propagation_mode != ParameterPropagationMode.NEVER
        )

    @classmethod
    def from_spec(cls, spec, all_parameters, environments, cmd_files):
        spec["inputs"] = [
            SchemaInput.from_spec(i, all_parameters) for i in spec.get("inputs", [])
        ]
        spec["outputs"] = [
            SchemaOutput.from_spec(i, all_parameters) for i in spec.get("outputs", [])
        ]
        spec["actions"] = [
            Action.from_spec(i, environments, all_parameters, cmd_files)
            for i in spec.get("actions")
        ]
        return cls(**spec)

    def get_parameter_dependence(self, parameter: SchemaParameter):
        """Find if/where a given parameter is used by the schema's actions."""
        out = {"input_file_writers": [], "commands": []}
        for act_idx, action in enumerate(self.actions):
            deps = action.get_parameter_dependence(parameter)
            for key in out:
                out[key].extend((act_idx, i) for i in deps[key])
        return out
