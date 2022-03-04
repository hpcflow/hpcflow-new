from dataclasses import dataclass, field
import enum
from typing import Any, List, Optional, Sequence, Union
import numpy as np
from hpcflow.element import ElementFilter
from hpcflow.errors import InputSourceValidationError

from hpcflow.typing_stubs import (
    SubParameter,
    InputValue,
    TaskTemplate,
    TaskSchema,
    WorkflowTemplate,
)
from hpcflow.utils import check_valid_py_identifier


Address = List[Union[int, float, str]]
Numeric = Union[int, float, np.number]


class ParameterPropagationMode(enum.Enum):

    IMPLICIT = 0
    EXPLICIT = 1
    NEVER = 2


@dataclass
class ParameterPath:

    path: Sequence[Union[str, int, float]]
    task: Optional[Union[TaskTemplate, TaskSchema]] = None  # default is "current" task


@dataclass
class Parameter:
    typ: str
    is_file: bool = False
    sub_parameters: List[SubParameter] = field(default_factory=lambda: [])

    def __post_init__(self):
        self.typ = check_valid_py_identifier(self.typ)

    @classmethod
    def from_spec(cls, spec):
        spec["typ"] = spec.pop("type")
        return cls(**spec)


@dataclass
class SubParameter:
    address: Address
    parameter: Parameter


@dataclass
class SchemaParameter:
    @classmethod
    def from_spec(cls, spec, parameters):
        spec["parameter"] = parameters[spec["parameter"]]
        prop_mode = spec.get("propagation_mode")
        if prop_mode:
            spec["propagation_mode"] = getattr(
                ParameterPropagationMode, prop_mode.upper()
            )
        return cls(**spec)

    @property
    def name(self):
        return self.parameter.name

    @property
    def typ(self):
        return self.parameter.typ


@dataclass
class SchemaInput(SchemaParameter):
    """A Parameter as used within a particular schema, for which a default value may be
    applied."""

    parameter: Parameter
    default_value: Optional[InputValue] = None
    propagation_mode: ParameterPropagationMode = ParameterPropagationMode.IMPLICIT

    # can we define elements groups on local inputs as well, or should these be just for
    # elements from other tasks?
    group: Optional[str] = None
    where: Optional[ElementFilter] = None

    def __post_init__(self):
        self._validate()

    def _validate(self):
        if self.default_value is not None:
            if self.default_value.parameter != self.parameter:
                raise ValueError(
                    f"{self.__class__.__name__} `default_value` must be an `InputValue` for "
                    f"parameter: {self.parameter!r}, but specified `InputValue` parameter "
                    f"is: {self.default_value.parameter!r}."
                )

    @property
    def input_or_output(self):
        return "input"

    @classmethod
    def from_spec(cls, spec, parameters):
        if "default_value" in spec:
            spec["default_value"] = InputValue(
                parameter=parameters[spec["parameter"]], value=spec["default_value"]
            )
        return super().from_spec(spec, parameters)


@dataclass
class SchemaOutput(SchemaParameter):
    """A Parameter as outputted from particular task."""

    parameter: Parameter
    propagation_mode: ParameterPropagationMode = ParameterPropagationMode.IMPLICIT

    @property
    def input_or_output(self):
        return "output"


@dataclass
class BuiltinSchemaParameter:
    # builtin inputs (resources,parameter_perturbations,method,implementation
    # builtin outputs (time, memory use, node/hostname etc)
    # - builtin parameters do not propagate to other tasks (since all tasks define the same
    #   builtin parameters).
    # - however, builtin parameters can be accessed if a downstream task schema specifically
    #   asks for them (e.g. for calculating/plotting a convergence test)
    pass


@dataclass
class ValueSequence:
    path: Sequence[Union[str, int, float]]
    values: List[Any]
    nesting_order: int

    def __post_init__(self):
        self.path = tuple(self.path)

    def check_address_exists(self, value):
        """Check a given nested dict/list "address" resolves to somewhere within
        `value`."""
        if self.address:
            sub_val = value
            for i in self.address:
                try:
                    sub_val = sub_val[i]
                except (IndexError, KeyError, TypeError):
                    msg = (
                        f"Address {self.address} does not exist in the base "
                        f"value: {value}"
                    )
                    raise ValueError(msg)

    @classmethod
    def from_spec(cls, spec):
        return cls(**spec)

    @classmethod
    def from_linear_space(cls, start, stop, num=50, address=None, **kwargs):
        values = list(np.linspace(start, stop, num=num, **kwargs))
        return cls(values, address=address)

    @classmethod
    def from_range(cls, start, stop, step=1, address=None):
        if isinstance(step, int):
            return cls(values=list(np.arange(start, stop, step)), address=address)
        else:
            # Use linspace for non-integer step, as recommended by Numpy:
            return cls.from_linear_space(
                start,
                stop,
                num=int((stop - start) / step),
                address=address,
                endpoint=False,
            )


@dataclass
class AbstractInputValue:
    """Class to represent all sequence-able inputs to a task."""


@dataclass
class ResourceSpec(AbstractInputValue):
    pass


@dataclass
class ValuePerturbation(AbstractInputValue):
    name: str
    path: Optional[Sequence[Union[str, int, float]]] = None
    multiplicative_factor: Optional[Numeric] = 1
    additive_factor: Optional[Numeric] = 0

    @classmethod
    def from_spec(cls, spec):
        return cls(**spec)


@dataclass
class InputValue(AbstractInputValue):
    parameter: Union[Parameter, SchemaInput]
    path: Optional[Sequence[Union[str, int, float]]] = None
    value: Optional[Any] = None

    def __post_init__(self):
        self._validate()

    @property
    def is_sub_value(self):
        """True if the value is for a sub part of the parameter (i.e. if `path` is set).
        Sub-values are not added to the base parameter data, but are interpreted as
        single-value sequences."""
        return True if self.path else False

    def _validate(self):
        pass
        # if self.sequences:
        #     seen_addresses = []
        #     for i in self.sequences:
        #         if i.address not in seen_addresses:
        #             seen_addresses.append(i.address)
        #         else:
        #             raise InputValueDuplicateSequenceAddress(
        #                 f"`{i.__class__.__name__}` defined with address {i.address!r} multiple "
        #                 f"times."
        #             )
        #         i.check_address_exists(self.value)

    @classmethod
    def from_spec(cls, spec, all_parameters):
        spec["parameter"] = all_parameters[spec.pop("parameter")]
        return cls(**spec)


@dataclass
class InputSource:
    source: str
    where: Optional[ElementFilter] = None

    # TODO: validate and split?

    def __post_init__(self):
        self._validate()

    def _validate(self):

        parts = self.source.lower().split(".")
        source_type = parts[0]

        allowed = ["imports", "tasks", "local", "default"]
        if source_type not in allowed:
            raise ValueError(
                f"InputSource `source` specified as {source_type!r}, but must be one "
                f"of: {allowed!r}."
            )

        if source_type == "tasks":
            task_ref = parts[1]
            task_source_type = parts[2]
            allowed_task_types = ["inputs", "outputs"]
            if task_source_type not in allowed_task_types:
                raise ValueError(
                    f"InputSource `source` with source type 'tasks' must be in the format: "
                    f"'tasks.[task_reference].inputs' or 'tasks.[task_reference].outputs', "
                    f"where [task_reference] is the unique name of a workflow task, specified "
                    f"source was: {self.source!r}."
                )
            self._task_ref = task_ref
            self._task_source_type = task_source_type

        elif source_type == "imports":
            self._imports_ref = parts[1]

        if (source_type == "local" and len(parts) > 1) or (
            source_type == "imports" and len(parts) > 2
        ):
            raise ValueError(f"InputSource source not understood: {self.source!r}.")

        if source_type in ["local", "default"] and self.where is not None:
            raise ValueError(
                f"Element filter via the `where` argument is not supported for InputSource "
                f"with source type {source_type!r}."
            )

        self._source_type = source_type

    @property
    def source_type(self):
        return self._source_type

    @property
    def task_ref(self):
        return self._task_ref

    @property
    def task_source_type(self):
        return self._task_source_type

    @property
    def imports_ref(self):
        return self._imports_ref

    @classmethod
    def from_spec(cls, spec):
        if "where" in spec:
            spec["where"] = ElementFilter.from_spec(spec["where"])
        return cls(**spec)

    def validate(
        self,
        schema_input: SchemaInput,
        task_template: TaskTemplate,
        workflow_like: WorkflowTemplate,
    ):
        """Check a supplied source is valid for a given workflow (template)."""

        if self.source_type == "tasks":

            # check referenced task exists:
            try:
                ref_task = getattr(workflow_like.tasks, self.task_ref)
            except AttributeError:
                raise InputSourceValidationError(
                    f"InputSource {self.source!r} cannot be resolved within the workflow."
                )

            # check the parameter is provided by the referenced task:
            if self.task_source_type == "inputs":
                if schema_input.typ not in ref_task.template.all_schema_input_types:
                    raise InputSourceValidationError(
                        f"Input parameter {schema_input.typ!r} cannot be sourced from task "
                        f"{self.task_ref!r}, since the task schemas do not provide this "
                        f"parameter as an input. Available inputs from this task are: "
                        f"{ref_task.template.all_schema_input_types!r}."
                    )
            elif self.task_source_type == "outputs":
                if schema_input.typ not in ref_task.template.all_schema_output_types:
                    raise InputSourceValidationError(
                        f"Input parameter {schema_input.typ!r} cannot be sourced from task "
                        f"{self.task_ref!r}, since the task schemas do not provide this "
                        f"parameter as an output. Available outputs from this task are: "
                        f"{ref_task.template.all_schema_output_types!r}."
                    )

        elif self.source_type == "imports":
            raise NotImplementedError("ain't dunit yet")

        elif self.source_type == "default":
            # check a default value exists:
            if schema_input.default_value is None:
                raise InputSourceValidationError(
                    f"Input parameter {schema_input.typ!r} cannot be sourced from the "
                    f"default value, because no default value is specified."
                )

        elif self.source_type == "local":
            # check local value is supplied by schema:
            if schema_input.typ not in task_template.all_schema_input_types:
                raise InputSourceValidationError(
                    f"Input parameter {schema_input.typ!r} cannot be sourced from the "
                    f"local inputs, because the schema does not provide such an input. "
                    f"Available inputs defined by the schema are: "
                    f"{task_template.all_schema_inputs!r}."
                )

            # check local value exists:
            if schema_input.typ not in task_template.defined_input_types:
                raise InputSourceValidationError(
                    f"Input parameter {schema_input.typ!r} cannot be sourced from the "
                    f"local inputs, because no local value is defined."
                )
