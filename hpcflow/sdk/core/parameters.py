from __future__ import annotations
from dataclasses import dataclass, field
import enum
from typing import Any, Dict, List, Optional, Sequence, Union

import numpy as np

from hpcflow.sdk.core.errors import ValuesAlreadyPersistentError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import check_valid_py_identifier
from hpcflow.sdk.core.zarr_io import ZarrEncodable, zarr_decode


Address = List[Union[int, float, str]]
Numeric = Union[int, float, np.number]


class ParameterPropagationMode(enum.Enum):

    IMPLICIT = 0
    EXPLICIT = 1
    NEVER = 2


@dataclass
class ParameterPath(JSONLike):

    path: Sequence[Union[str, int, float]]
    task: Optional[Union[TaskTemplate, TaskSchema]] = None  # default is "current" task


@dataclass
class Parameter(JSONLike):

    _validation_schema = "parameters_spec_schema.yaml"
    _child_objects = (
        ChildObjectSpec(
            name="typ",
            json_like_name="type",
        ),
    )

    typ: str
    is_file: bool = False
    sub_parameters: List[SubParameter] = field(default_factory=lambda: [])
    _value_class: Any = None
    _hash_value: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.typ = check_valid_py_identifier(self.typ)
        for i in ZarrEncodable.__subclasses__():
            if i._typ == self.typ:
                self._value_class = i

    def to_dict(self):
        dct = super().to_dict()
        del dct["_value_class"]
        return dct


@dataclass
class SubParameter:
    address: Address
    parameter: Parameter


@dataclass
class SchemaParameter(JSONLike):

    _child_objects = (
        ChildObjectSpec(
            name="parameter",
            class_name="SchemaInput",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
    )

    def __post_init__(self):
        self._validate()

    def _validate(self):
        if isinstance(self.parameter, str):
            self.parameter = Parameter(self.parameter)

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

    _child_objects = (
        ChildObjectSpec(
            name="parameter",
            class_name="SchemaInput",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
        ChildObjectSpec(
            name="default_value",
            class_name="InputValue",
        ),
        ChildObjectSpec(
            name="propagation_mode",
            class_name="ParameterPropagationMode",
            is_enum=True,
        ),
    )

    parameter: Parameter
    default_value: Optional[InputValue] = None
    propagation_mode: ParameterPropagationMode = ParameterPropagationMode.IMPLICIT

    # can we define elements groups on local inputs as well, or should these be just for
    # elements from other tasks?
    group: Optional[str] = None
    where: Optional[ElementFilter] = None

    def _validate(self):
        super()._validate()
        if self.default_value is not None:
            if not isinstance(self.default_value, self.app.InputValue):
                self.default_value = self.app.InputValue(
                    parameter=self.parameter,
                    value=self.default_value,
                )
            if self.default_value.parameter != self.parameter:
                raise ValueError(
                    f"{self.__class__.__name__} `default_value` must be an `InputValue` for "
                    f"parameter: {self.parameter!r}, but specified `InputValue` parameter "
                    f"is: {self.default_value.parameter!r}."
                )

    @property
    def input_or_output(self):
        return "input"

    # @classmethod
    # def from_json_like(cls, json_like):
    #     kwargs = cls.prepare_from_json_like(json_like)
    #     cls.app.logger.warn(f"SchemaInput.from_json_like: kwargs: {kwargs}")
    #     return super().from_json_like(kwargs)

    # @classmethod
    # @required_keys(1, "parameter")
    # @allowed_keys(1, "parameter", "default_value", "propagation_mode")
    # @check_in_object_list(spec_name="parameter")
    # def from_spec(cls, spec, parameters):
    #     cls.app.logger.debug("SchemaInput.from_spec")
    #     if "default_value" in spec:
    #         spec["default_value"] = InputValue(
    #             parameter=parameters[spec["parameter"]], value=spec["default_value"]
    #         )
    #     return super().from_spec(spec, parameters)


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


class ValueSequence(JSONLike):
    def __init__(
        self,
        path: str,
        nesting_order: int,
        values: List[Any],
    ):
        self.path = path
        self.nesting_order = nesting_order
        self._values = values

        self._values_group_idx = None
        self._workflow = None

    def __repr__(self):
        vals_grp_idx = (
            f"values_group_idx={self._values_group_idx}, "
            if self._values_group_idx
            else ""
        )
        return (
            f"{self.__class__.__name__}("
            f"path={self.path!r}, "
            f"nesting_order={self.nesting_order}, "
            f"{vals_grp_idx}"
            f"values={self.values}"
            f")"
        )

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _values_group_idx = json_like.pop("_values_group_idx", None)
        if "_values" in json_like:
            json_like["values"] = json_like.pop("_values")

        obj = cls(**json_like)
        obj._values_group_idx = _values_group_idx
        return obj

    def to_dict(self):
        out = super().to_dict()
        if "_workflow" in out:
            del out["_workflow"]
        return out

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

    def _get_param_path(self):
        return self.path  # TODO: maybe not needed?

    def make_persistent(self, workflow):
        """Save value to a persistent workflow."""

        # TODO: test raise
        if self._values_group_idx is not None:
            raise ValuesAlreadyPersistentError(
                f"{self.__class__.__name__} is already persistent."
            )

        param_group_idx = []
        for i in self._values:
            pg_idx_i = workflow._add_parameter_group(i, is_set=True)
            param_group_idx.append(pg_idx_i)

        self._values_group_idx = param_group_idx
        self._workflow = workflow
        self._values = None
        return {self._get_param_path(): param_group_idx}

    @property
    def workflow(self):
        return self._workflow

    @property
    def values(self):
        if self._values_group_idx is not None:
            vals = []
            for pg_idx_i in self._values_group_idx:
                grp = self._workflow.get_zarr_parameter_group(pg_idx_i)
                vals.append(zarr_decode(grp))
            return vals
        else:
            return self._values

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
class AbstractInputValue(JSONLike):
    """Class to represent all sequence-able inputs to a task."""

    _workflow = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"_value_group_idx={self._value_group_idx}, "
            f"value={self.value}"
            f")"
        )

    def to_dict(self):
        out = super().to_dict()
        if "_workflow" in out:
            del out["_workflow"]
        return out

    def make_persistent(self, workflow) -> Dict:
        """Save value to a persistent workflow.

        Parameters
        ----------
        workflow : Workflow

        Returns
        -------
        dict of tuple : int
            Single-item dict whose key is the data path for this task input and whose
            value is the integer index of the parameter data Zarr group where the data is
            stored.
        """

        param_group_idx = workflow._add_parameter_group(self._value, is_set=True)
        self._value_group_idx = param_group_idx
        self._workflow = workflow
        self._value = None
        return {self._get_param_path(): [param_group_idx]}

    @property
    def workflow(self):
        if self._workflow:
            return self._workflow
        elif self._task:
            return self._task.workflow_template.workflow

    @property
    def value(self):
        if self._value_group_idx is not None:
            grp = self.workflow.get_zarr_parameter_group(self._value_group_idx)
            val = zarr_decode(grp)
            if self.parameter._value_class:
                val = self.parameter._value_class(**val)
        else:
            val = self._value

        return val


@dataclass
class ValuePerturbation(AbstractInputValue):
    name: str
    path: Optional[Sequence[Union[str, int, float]]] = None
    multiplicative_factor: Optional[Numeric] = 1
    additive_factor: Optional[Numeric] = 0

    @classmethod
    def from_spec(cls, spec):
        return cls(**spec)


class InputValue(AbstractInputValue):

    _child_objects = (
        ChildObjectSpec(
            name="parameter",
            class_name="SchemaInput",
            shared_data_primary_key="typ",
            shared_data_name="parameters",
        ),
    )

    def __init__(
        self,
        parameter: Union[Parameter, SchemaInput, str],
        value: Optional[Any] = None,
        path: Optional[str] = None,
    ):
        if isinstance(parameter, str):
            parameter = self.app.parameters.get(parameter)

        self.parameter = parameter
        self.path = (path.strip(".") if path else None) or None
        self._value = value

        self._value_group_idx = None  # assigned by method make_persistent
        self._task = None  # assigned by parent Task

    def __repr__(self):

        val_grp_idx = ""
        if self._value_group_idx is not None:
            val_grp_idx = f", value_group_idx={self._value_group_idx}"

        path_str = ""
        if self.path is not None:
            path_str = f", path={self.path!r}"

        return (
            f"{self.__class__.__name__}("
            f"parameter={self.parameter.typ!r}, "
            f"value={self.value}"
            f"{path_str}"
            f"{val_grp_idx}"
            f")"
        )

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _value_group_idx = json_like.pop("_value_group_idx", None)
        if "_value" in json_like:
            json_like["value"] = json_like.pop("_value")

        obj = cls(**json_like)
        obj._value_group_idx = _value_group_idx

        return obj

    def _get_param_path(self):
        return f"inputs.{self.parameter.typ}" f"{f'.{self.path}' if self.path else ''}"

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):

        if "path" not in json_like:
            param_spec = json_like["parameter"].split(".")
            json_like["parameter"] = param_spec[0]
            json_like["path"] = ".".join(param_spec[1:])

        obj = super().from_json_like(json_like, shared_data)

        return obj

    @property
    def is_sub_value(self):
        """True if the value is for a sub part of the parameter (i.e. if `path` is set).
        Sub-values are not added to the base parameter data, but are interpreted as
        single-value sequences."""
        return True if self.path else False


class ResourceSpec(JSONLike):

    _resource_list = None

    _child_objects = (
        ChildObjectSpec(
            name="scope",
            class_name="ActionScope",
        ),
    )

    def __init__(self, scope=None, scratch=None, num_cores=None):
        self.scope = scope or self.app.ActionScope.any()
        self.scratch = scratch
        self.num_cores = num_cores

    def __repr__(self):
        scratch_str = ""
        if self.scratch is not None:
            scratch_str = f", scratch={self.scratch!r}"
        num_cores_str = ""
        if self.num_cores is not None:
            num_cores_str = f", num_cores={self.num_cores!r}"

        return (
            f"{self.__class__.__name__}("
            f"scope={self.scope}"
            f"{scratch_str}"
            f"{num_cores_str}"
            f")"
        )

    def _get_param_path(self):
        scope_str = ""
        if self.scope.typ.name != self.app.ActionScopeType.ANY.name:
            scope_str = f".{self.scope.to_string()}"
        return f"resources{scope_str}"

    @property
    def task(self):
        return self._resource_list.task


class InputSourceType(enum.Enum):

    IMPORT = 0
    LOCAL = 1
    DEFAULT = 2
    TASK = 3


class TaskSourceType(enum.Enum):
    INPUT = 0
    OUTPUT = 1
    ANY = 2


class InputSourceMode(enum.Enum):
    """Set to MANUAL if a task has input source(s) specified on creation (or modification)
    otherwise set to AUTO, in which case input sources will be set by hpcflow, and input
    sources may be appended to if new tasks/imports are added to the workflow."""

    AUTO = 0
    MANUAL = 1


class InputSource(JSONLike):

    _child_objects = (
        ChildObjectSpec(
            name="source_type",
            json_like_name="type",
            class_name="InputSourceType",
            is_enum=True,
        ),
    )

    def __init__(
        self,
        source_type,
        import_ref=None,
        task_ref=None,
        task_source_type=None,
        where=None,
    ):

        self.source_type = self._validate_source_type(source_type)
        self.import_ref = import_ref
        self.task_ref = task_ref
        self.task_source_type = self._validate_task_source_type(task_source_type)
        self.where = where

        if self.source_type is InputSourceType.TASK:
            if self.task_ref is None:
                raise ValueError(f"Must specify `task_ref` if `source_type` is TASK.")
            if self.task_source_type is None:
                self.task_source_type = TaskSourceType.OUTPUT

        if self.source_type is InputSourceType.IMPORT and self.import_ref is None:
            raise ValueError(f"Must specify `import_ref` if `source_type` is IMPORT.")

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        elif (
            self.source_type == other.source_type
            and self.import_ref == other.import_ref
            and self.task_ref == other.task_ref
            and self.task_source_type == other.task_source_type
            and self.where == other.where
        ):
            return True
        else:
            return False

    def __repr__(self) -> str:
        members = {k: v for k, v in self.__dict__.items() if v is not None}
        return (
            f"{self.__class__.__name__}("
            f'{", ".join([f"{k}={repr(v)}" for k, v in members.items()])}'
            f")"
        )

    def to_string(self):
        out = [self.source_type.name.lower()]
        if self.source_type is InputSourceType.TASK:
            out += [str(self.task_ref), self.task_source_type.name.lower()]
        elif self.source_type is InputSourceType.IMPORT:
            out += [str(self.import_ref)]
        return ".".join(out)

    @staticmethod
    def _validate_source_type(src_type):
        if src_type is None:
            return None
        if isinstance(src_type, InputSourceType):
            return src_type
        try:
            src_type = getattr(InputSourceType, src_type.upper())
        except AttributeError:
            raise ValueError(
                f"InputSource `source_type` specified as {src_type!r}, but "
                f"must be one of: {[i.name for i in InputSourceType]!r}."
            )
        return src_type

    @staticmethod
    def _validate_task_source_type(task_src_type):
        if task_src_type is None:
            return None
        if isinstance(task_src_type, TaskSourceType):
            return task_src_type
        try:
            task_source_type = getattr(TaskSourceType, task_src_type.upper())
        except AttributeError:
            raise ValueError(
                f"InputSource `task_source_type` specified as {task_src_type!r}, but "
                f"must be one of: {[i.name for i in TaskSourceType]!r}."
            )
        return task_source_type

    @classmethod
    def from_string(cls, str_defn):
        return cls(**cls._parse_from_string(str_defn))

    @classmethod
    def _parse_from_string(cls, str_defn):
        """Parse a dot-delimited string definition of an InputSource.

        Examples:
            - task.[task_ref].input
            - task.[task_ref].output
            - local
            - default
            - import.[import_ref]

        """
        parts = str_defn.lower().split(".")
        source_type = cls._validate_source_type(parts[0])
        task_ref = None
        task_source_type = None
        import_ref = None
        if (
            (
                source_type in (InputSourceType.LOCAL, InputSourceType.DEFAULT)
                and len(parts) > 1
            )
            or (source_type is InputSourceType.TASK and len(parts) > 3)
            or (source_type is InputSourceType.IMPORT and len(parts) > 2)
        ):
            raise ValueError(f"InputSource string not understood: {str_defn!r}.")

        if source_type is InputSourceType.TASK:
            task_ref = parts[1]
            try:
                task_ref = int(task_ref)
            except ValueError:
                pass
            try:
                task_source_type_str = parts[2]
            except IndexError:
                task_source_type_str = TaskSourceType.OUTPUT
            task_source_type = cls._validate_task_source_type(task_source_type_str)
        elif source_type is InputSourceType.IMPORT:
            import_ref = parts[1]
            try:
                import_ref = int(import_ref)
            except ValueError:
                pass

        return {
            "source_type": source_type,
            "import_ref": import_ref,
            "task_ref": task_ref,
            "task_source_type": task_source_type,
        }

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if isinstance(json_like, str):
            json_like = cls._parse_from_string(json_like)
        return super().from_json_like(json_like, shared_data)

    @classmethod
    def import_(cls, import_ref):
        return cls(source_type=InputSourceType.IMPORT, import_ref=import_ref)

    @classmethod
    def local(cls):
        return cls(source_type=InputSourceType.LOCAL)

    @classmethod
    def default(cls):
        return cls(source_type=InputSourceType.DEFAULT)

    @classmethod
    def task(cls, task_ref, task_source_type=None):
        if not task_source_type:
            task_source_type = TaskSourceType.OUTPUT
        return cls(
            source_type=InputSourceType.TASK,
            task_ref=task_ref,
            task_source_type=cls._validate_task_source_type(task_source_type),
        )


@dataclass
class ParameterValue:
    @property
    def value(self):
        pass


@dataclass
class AvailableInputSources:
    """Container for listing the available sources for a given input within a task."""

    # TODO: should this be a list of `InputSource`?

    has_local: bool
    has_default: bool
    tasks: Dict
    imports: Dict
