from __future__ import annotations
from collections.abc import Sequence
import copy
from dataclasses import dataclass, field
from datetime import timedelta
import enum
from pathlib import Path
import re
from typing import TypedDict, TypeVar, cast, TYPE_CHECKING

import numpy as np
from valida import Schema as ValidaSchema  # type: ignore

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.element import ElementFilter
from hpcflow.sdk.core.errors import (
    MalformedParameterPathError,
    UnknownResourceSpecItemError,
    WorkflowParameterMissingError,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.parallel import ParallelMode
from hpcflow.sdk.core.rule import Rule
from hpcflow.sdk.core.utils import (
    check_valid_py_identifier,
    get_enum_by_name_or_val,
    linspace_rect,
    process_string_nodes,
    split_param_label,
)
from hpcflow.sdk.submission.submission import timedelta_format

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any, ClassVar, Literal
    from typing_extensions import NotRequired, Self, TypeAlias
    from ..app import BaseApp
    from ..typing import ParamSource
    from .actions import ActionScope
    from .object_list import ResourceList
    from .rule import RuleArgs
    from .task import ElementSet, TaskSchema, TaskTemplate, WorkflowTask
    from .workflow import Workflow, WorkflowTemplate
    from .validation import Schema


Address: TypeAlias = "list[int | float | str]"
Numeric: TypeAlias = "int | float | np.number"
T = TypeVar("T")


def _process_demo_data_strings(app: BaseApp, value: T) -> T:
    def string_processor(str_in: str) -> str:
        demo_pattern = r"\<\<demo_data_file:(.*)\>\>"
        str_out = re.sub(
            pattern=demo_pattern,
            repl=lambda x: str(app.get_demo_data_file_path(x.group(1))),
            string=str_in,
        )
        return str_out

    return process_string_nodes(value, string_processor)


@dataclass
@hydrate
class ParameterValue:
    _typ: ClassVar[str | None] = None
    _sub_parameters: ClassVar[dict[str, str]] = {}

    def to_dict(self):
        if hasattr(self, "__dict__"):
            return dict(self.__dict__)
        elif hasattr(self, "__slots__"):
            return {k: getattr(self, k) for k in self.__slots__}
        else:
            raise NotImplementedError

    def prepare_JSON_dump(self) -> dict[str, Any]:
        raise NotImplementedError

    def dump_to_HDF5_group(self, group):
        raise NotImplementedError

    @classmethod
    def save_from_HDF5_group(cls, group, param_id: int, workflow):
        raise NotImplementedError

    @classmethod
    def save_from_JSON(cls, data, param_id: int | list[int], workflow):
        raise NotImplementedError


class ParameterPropagationMode(enum.Enum):
    IMPLICIT = 0
    EXPLICIT = 1
    NEVER = 2


@dataclass
class ParameterPath(JSONLike):
    # TODO: unused?
    path: Sequence[str | int | float]
    task: TaskTemplate | TaskSchema | None = None  # default is "current" task


@dataclass
@hydrate
class Parameter(JSONLike):
    _validation_schema: ClassVar[str] = "parameters_spec_schema.yaml"
    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="typ",
            json_like_name="type",
        ),
        ChildObjectSpec(
            name="_validation",
            class_obj=ValidaSchema,
        ),
    )

    typ: str
    is_file: bool = False
    sub_parameters: list[SubParameter] = field(default_factory=list)
    name: str | None = None
    _value_class: type[ParameterValue] | None = None
    _hash_value: str | None = field(default=None, repr=False)
    _validation: Schema | None = None

    def __repr__(self) -> str:
        is_file_str = ""
        if self.is_file:
            is_file_str = f", is_file={self.is_file!r}"

        sub_parameters_str = ""
        if self.sub_parameters:
            sub_parameters_str = f", sub_parameters={self.sub_parameters!r}"

        _value_class_str = ""
        if self._value_class is not None:
            _value_class_str = f", _value_class={self._value_class!r}"

        return (
            f"{self.__class__.__name__}("
            f"typ={self.typ!r}{is_file_str}{sub_parameters_str}{_value_class_str}"
            f")"
        )

    def __post_init__(self) -> None:
        self.typ = check_valid_py_identifier(self.typ)
        self._set_value_class()

    def _set_value_class(self) -> None:
        # custom parameter classes must inherit from `ParameterValue` not the app
        # subclass:
        if self._value_class is None:
            for i in ParameterValue.__subclasses__():
                if i._typ == self.typ:
                    self._value_class = i
                    break

    def __eq__(self, other) -> bool:
        return isinstance(other, Parameter) and self.typ == other.typ

    def __lt__(self, other):
        return self.typ < other.typ

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        _validation = kwargs.pop("_validation")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._validation = _validation
        return obj

    def to_dict(self):
        dct = super().to_dict()
        del dct["_value_class"]
        if dct.get("name") is None:
            dct.pop("name", None)
        dct.pop("_task_schema", None)  # TODO: how do we have a _task_schema ref?
        return dct

    @property
    def url_slug(self) -> str:
        return self.typ.lower().replace("_", "-")

    def _instantiate_value(self, source: ParamSource, val: dict) -> Any:
        """
        Convert the serialized form of this parameter to its "real" form,
        if that is valid to do at all.
        """
        if self._value_class is None:
            return val
        method_name = source.get("value_class_method")
        if method_name is not None:
            method = getattr(self._value_class, method_name)
        else:
            method = self._value_class
        return method(**val)

    def _force_value_class(self) -> type[ParameterValue] | None:
        param_cls = self._value_class
        if param_cls is None:
            self._set_value_class()
            param_cls = self._value_class
        return param_cls


@dataclass
class SubParameter:
    address: Address
    parameter: Parameter


@dataclass
class SchemaParameter(JSONLike):
    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="parameter",
            class_name="Parameter",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
    )

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if isinstance(self.parameter, str):
            self.parameter: Parameter = self._app.Parameter(typ=self.parameter)

    @property
    def name(self) -> str:
        return self.parameter.name or ""

    @property
    def typ(self) -> str:
        return self.parameter.typ


class NullDefault(enum.Enum):
    NULL = 0


class LabelInfo(TypedDict):
    propagation_mode: NotRequired[ParameterPropagationMode]
    group: NotRequired[str]
    default_value: NotRequired[InputValue]


class SchemaInput(SchemaParameter):
    """A Parameter as used within a particular schema, for which a default value may be
    applied.

    Parameters
    ----------
    parameter
        The parameter (i.e. type) of this schema input.
    multiple
        If True, expect one or more of these parameters defined in the workflow,
        distinguished by a string label in square brackets. For example `p1[0]` for a
        parameter `p1`.
    labels
        Dict whose keys represent the string labels that distinguish multiple parameters
        if `multiple` is `True`. Use the key "*" to mean all labels not matching
        other label keys. If `multiple` is `False`, this will default to a
        single-item dict with an empty string key: `{{"": {{}}}}`. If `multiple` is
        `True`, this will default to a single-item dict with the catch-all key:
        `{{"*": {{}}}}`. On initialisation, remaining keyword-arguments are treated as default
        values for the dict values of `labels`.
    default_value
        The default value for this input parameter. This is itself a default value that
        will be applied to all `labels` values if a "default_value" key does not exist.
    propagation_mode
        Determines how this input should propagate through the workflow. This is a default
        value that will be applied to all `labels` values if a "propagation_mode" key does
        not exist. By default, the input is allowed to be used in downstream tasks simply
        because it has a compatible type (this is the "implicit" propagation mode). Other
        options are "explicit", meaning that the parameter must be explicitly specified in
        the downstream task `input_sources` for it to be used, and "never", meaning that
        the parameter must not be used in downstream tasks and will be inaccessible to
        those tasks.
    group
        Determines the name of the element group from which this input should be sourced.
        This is a default value that will be applied to all `labels` if a "group" key
        does not exist.
    """

    _task_schema: TaskSchema | None = None  # assigned by parent TaskSchema

    _child_objects = (
        ChildObjectSpec(
            name="parameter",
            class_name="Parameter",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
    )

    def __init__(
        self,
        parameter: Parameter | str,
        multiple: bool = False,
        labels: dict[str, LabelInfo] | None = None,
        default_value: InputValue | Any | NullDefault = NullDefault.NULL,
        propagation_mode: ParameterPropagationMode = ParameterPropagationMode.IMPLICIT,
        group: str | None = None,
    ):
        # TODO: can we define elements groups on local inputs as well, or should these be
        # just for elements from other tasks?

        # TODO: test we allow unlabelled with accepts-multiple True.
        # TODO: test we allow a single labelled with accepts-multiple False.

        if isinstance(parameter, str):
            try:
                self.parameter = self._app.parameters.get(parameter)
            except ValueError:
                self.parameter = self._app.Parameter(parameter)
        else:
            self.parameter = parameter

        self.multiple = multiple

        self.labels: dict[str, LabelInfo]
        if labels is None:
            if self.multiple:
                self.labels = {"*": {}}
            else:
                self.labels = {"": {}}
        else:
            self.labels = labels
            if not self.multiple:
                # check single-item:
                if len(self.labels) > 1:
                    raise ValueError(
                        f"If `{self.__class__.__name__}.multiple` is `False`, "
                        f"then `labels` must be a single-item `dict` if specified, but "
                        f"`labels` is: {self.labels!r}."
                    )

        labels_defaults: LabelInfo = {}
        if propagation_mode is not None:
            labels_defaults["propagation_mode"] = propagation_mode
        if group is not None:
            labels_defaults["group"] = group

        # apply defaults:
        for k, v in self.labels.items():
            labels_defaults_i = copy.deepcopy(labels_defaults)
            if default_value is not NullDefault.NULL:
                if isinstance(default_value, InputValue):
                    labels_defaults_i["default_value"] = default_value
                else:
                    labels_defaults_i["default_value"] = self._app.InputValue(
                        parameter=self.parameter,
                        value=default_value,
                        label=k,
                    )
            label_i: LabelInfo = {**labels_defaults_i, **v}
            if "propagation_mode" in label_i:
                label_i["propagation_mode"] = get_enum_by_name_or_val(
                    ParameterPropagationMode, label_i["propagation_mode"]
                )
            if "default_value" in label_i:
                label_i["default_value"]._schema_input = self
            self.labels[k] = label_i

        self._set_parent_refs()
        self._validate()

    def __repr__(self) -> str:
        default_str = ""
        group_str = ""
        labels_str = ""
        if not self.multiple and self.labels:
            label = next(iter(self.labels.keys()))  # the single key

            default_str = ""
            if "default_value" in self.labels[label]:
                default_str = (
                    f", default_value={self.labels[label]['default_value'].value!r}"
                )

            group = self.labels[label].get("group")
            if group is not None:
                group_str = f", group={group!r}"

        else:
            labels_str = f", labels={str(self.labels)!r}"

        return (
            f"{self.__class__.__name__}("
            f"parameter={self.parameter.__class__.__name__}({self.parameter.typ!r}), "
            f"multiple={self.multiple!r}"
            f"{default_str}{group_str}{labels_str}"
            f")"
        )

    def to_dict(self):
        dct = super().to_dict()
        for k, v in dct["labels"].items():
            prop_mode = v.get("parameter_propagation_mode")
            if prop_mode:
                dct["labels"][k]["parameter_propagation_mode"] = prop_mode.name
        return dct

    def to_json_like(self, dct=None, shared_data=None, exclude=None, path=None):
        out, shared = super().to_json_like(dct, shared_data, exclude, path)
        for k, v in out["labels"].items():
            if "default_value" in v:
                out["labels"][k]["default_value_is_input_value"] = True
        return out, shared

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        for k, v in json_like.get("labels", {}).items():
            if "default_value" in v:
                if "default_value_is_input_value" in v:
                    inp_val_kwargs = v["default_value"]
                else:
                    inp_val_kwargs = {
                        "parameter": json_like["parameter"],
                        "value": v["default_value"],
                        "label": k,
                    }
                json_like["labels"][k][
                    "default_value"
                ] = cls._app.InputValue.from_json_like(
                    json_like=inp_val_kwargs,
                    shared_data=shared_data,
                )

        obj = super().from_json_like(json_like, shared_data)
        return obj

    def __deepcopy__(self, memo):
        kwargs = {
            "parameter": self.parameter,
            "multiple": self.multiple,
            "labels": self.labels,
        }
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._task_schema = self._task_schema
        return obj

    @property
    def default_value(self) -> InputValue | Literal[NullDefault.NULL] | None:
        single_data = self.single_labelled_data
        if single_data:
            if "default_value" in single_data:
                return single_data["default_value"]
            else:
                return NullDefault.NULL
        return None

    @property
    def task_schema(self) -> TaskSchema:
        assert self._task_schema is not None
        return self._task_schema

    @property
    def all_labelled_types(self) -> list[str]:
        return list(f"{self.typ}{f'[{i}]' if i else ''}" for i in self.labels)

    @property
    def single_label(self) -> str | None:
        if not self.multiple:
            return next(iter(self.labels))
        return None

    @property
    def single_labelled_type(self) -> str | None:
        if not self.multiple:
            return next(iter(self.labelled_info()))["labelled_type"]
        return None

    @property
    def single_labelled_data(self) -> LabelInfo | None:
        label = self.single_label
        if label is not None:
            return self.labels[label]
        return None

    def labelled_info(self) -> Iterator[LabellingDescriptor]:
        for k, v in self.labels.items():
            label = f"[{k}]" if k else ""
            dct: LabellingDescriptor = {
                "labelled_type": self.parameter.typ + label,
                "propagation_mode": v["propagation_mode"],
                "group": cast(str, v.get("group")),
            }
            if "default_value" in v:
                dct["default_value"] = v["default_value"]
            yield dct

    def _validate(self) -> None:
        super()._validate()
        for k, v in self.labels.items():
            if "default_value" in v:
                if not isinstance(v["default_value"], InputValue):
                    def_val = self._app.InputValue(
                        parameter=self.parameter,
                        value=v["default_value"],
                        label=k,
                    )
                    v["default_value"] = def_val
                else:
                    def_val = v["default_value"]
                if def_val.parameter != self.parameter or def_val.label != k:
                    raise ValueError(
                        f"{self.__class__.__name__} `default_value` for label {k!r} must "
                        f"be an `InputValue` for parameter: {self.parameter!r} with the "
                        f"same label, but specified `InputValue` is: "
                        f"{v['default_value']!r}."
                    )

    @property
    def input_or_output(self) -> str:
        return "input"


class LabellingDescriptor(TypedDict):
    labelled_type: str
    propagation_mode: ParameterPropagationMode
    group: str
    default_value: NotRequired[InputValue]


@dataclass(init=False)
@hydrate
class SchemaOutput(SchemaParameter):
    """A Parameter as outputted from particular task."""

    parameter: Parameter
    propagation_mode: ParameterPropagationMode

    def __init__(
        self,
        parameter: Parameter | str,
        propagation_mode: ParameterPropagationMode = ParameterPropagationMode.IMPLICIT,
    ):
        if isinstance(parameter, str):
            self.parameter: Parameter = self._app.Parameter(typ=parameter)
        else:
            self.parameter = parameter
        self.propagation_mode = propagation_mode

    @property
    def input_or_output(self) -> str:
        return "output"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"parameter={self.parameter.__class__.__name__}({self.parameter.typ!r}), "
            f"propagation_mode={self.propagation_mode.name!r}"
            f")"
        )


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
        values: list[Any] | None,
        nesting_order: int | float | None = None,
        label: str | int | None = None,
        value_class_method: str | None = None,
    ):
        self.path, self.label = self._validate_parameter_path(path, label)
        self.nesting_order = float(nesting_order) if nesting_order is not None else None
        self.value_class_method = value_class_method

        if values is not None:
            self._values: list[Any] | None = [
                _process_demo_data_strings(self._app, i) for i in values
            ]
        else:
            self._values = None

        self._values_group_idx: list[int] | None = None
        self._values_are_objs: list[
            bool
        ] | None = None  # assigned initially on `make_persistent`

        self._workflow: Workflow | None = None
        self._element_set: ElementSet | None = None  # assigned by parent ElementSet

        # assigned if this is an "inputs" sequence in `WorkflowTask._add_element_set`:
        self._parameter: Parameter | None = None

        self._path_split: list[str] | None = None  # assigned by property `path_split`

        self._values_method: str | None = None
        self._values_method_args: dict | None = None

    def __repr__(self):
        label_str = ""
        if self.label:
            label_str = f"label={self.label!r}, "
        vals_grp_idx = (
            f"values_group_idx={self._values_group_idx}, "
            if self._values_group_idx
            else ""
        )
        return (
            f"{self.__class__.__name__}("
            f"path={self.path!r}, "
            f"{label_str}"
            f"nesting_order={self.nesting_order}, "
            f"{vals_grp_idx}"
            f"values={self.values}"
            f")"
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.to_dict() == other.to_dict():
            return True
        return False

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        kwargs["values"] = kwargs.pop("_values")

        _values_group_idx = kwargs.pop("_values_group_idx")
        _values_are_objs = kwargs.pop("_values_are_objs")
        _values_method = kwargs.pop("_values_method", None)
        _values_method_args = kwargs.pop("_values_method_args", None)

        obj = self.__class__(**copy.deepcopy(kwargs, memo))

        obj._values_group_idx = _values_group_idx
        obj._values_are_objs = _values_are_objs
        obj._values_method = _values_method
        obj._values_method_args = _values_method_args

        obj._workflow = self._workflow
        obj._element_set = self._element_set
        obj._path_split = self._path_split
        obj._parameter = self._parameter

        return obj

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if "::" in json_like["path"]:
            path, cls_method = json_like["path"].split("::")
            json_like["path"] = path
            json_like["value_class_method"] = cls_method

        val_key = None
        for i in json_like:
            if "values" in i:
                val_key = i
        if "::" in val_key:
            # class method (e.g. `from_range`, `from_file` etc):
            _, method = val_key.split("::")
            _values_method_args = json_like.pop(val_key)
            _values_method = f"_values_{method}"
            _values_method_args = _process_demo_data_strings(
                cls._app, _values_method_args
            )
            json_like["values"] = getattr(cls, _values_method)(**_values_method_args)

        obj = super().from_json_like(json_like, shared_data)
        if "::" in val_key:
            obj._values_method = method
            obj._values_method_args = _values_method_args

        return obj

    @property
    def parameter(self) -> Parameter | None:
        return self._parameter

    @property
    def path_split(self) -> list[str]:
        if self._path_split is None:
            self._path_split = self.path.split(".")
        return self._path_split

    @property
    def path_type(self) -> str:
        return self.path_split[0]

    @property
    def input_type(self) -> str | None:
        if self.path_type == "inputs":
            return self.path_split[1].replace(self._label_fmt, "")
        return None

    @property
    def input_path(self) -> str | None:
        if self.path_type == "inputs":
            return ".".join(self.path_split[2:])
        return None

    @property
    def resource_scope(self) -> str | None:
        if self.path_type == "resources":
            return self.path_split[1]
        return None

    @property
    def is_sub_value(self) -> bool:
        """True if the values are for a sub part of the parameter."""
        return True if self.input_path else False

    @property
    def _label_fmt(self) -> str:
        return f"[{self.label}]" if self.label else ""

    @property
    def labelled_type(self) -> str | None:
        if self.input_type:
            return f"{self.input_type}{self._label_fmt}"
        return None

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _values_group_idx = json_like.pop("_values_group_idx", None)
        _values_are_objs = json_like.pop("_values_are_objs", None)
        _values_method = json_like.pop("_values_method", None)
        _values_method_args = json_like.pop("_values_method_args", None)
        if "_values" in json_like:
            json_like["values"] = json_like.pop("_values")

        obj = cls(**json_like)
        obj._values_group_idx = _values_group_idx
        obj._values_are_objs = _values_are_objs
        obj._values_method = _values_method
        obj._values_method_args = _values_method_args
        return obj

    def _validate_parameter_path(
        self, path: str, label: str | int | None
    ) -> tuple[str, str | int | None]:
        """Parse the supplied path and perform basic checks on it.

        This method also adds the specified `SchemaInput` label to the path and checks for
        consistency if a label is already present.

        """
        label_arg = label

        if not isinstance(path, str):
            raise MalformedParameterPathError(
                f"`path` must be a string, but given path has type {type(path)} with value "
                f"{path!r}."
            )
        path_l = path.lower()
        path_split = path_l.split(".")
        allowed_path_start = ("inputs", "resources", "environments", "env_preset")
        if not path_split[0] in allowed_path_start:
            raise MalformedParameterPathError(
                f"`path` must start with one of: "
                f'{", ".join(f"{i!r}" for i in allowed_path_start)}, but given path '
                f"is: {path!r}."
            )

        _, label_from_path = split_param_label(path_l)

        if path_split[0] == "inputs":
            if label_arg is not None and label_arg != "":
                if label_from_path is None:
                    # add label to path without lower casing any parts:
                    path_split_orig = path.split(".")
                    path_split_orig[1] += f"[{label_arg}]"
                    path = ".".join(path_split_orig)
                elif str(label_arg) != label_from_path:
                    raise ValueError(
                        f"{self.__class__.__name__} `label` argument is specified as "
                        f"{label_arg!r}, but a distinct label is implied by the sequence "
                        f"path: {path!r}."
                    )
            elif label_from_path:
                label = label_from_path

        elif path_split[0] == "resources":
            if label_from_path or label_arg:
                raise ValueError(
                    f"{self.__class__.__name__} `label` argument ({label_arg!r}) and/or "
                    f"label specification via `path` ({path!r}) is not supported for "
                    f"`resource` sequences."
                )
            try:
                self._app.ActionScope.from_json_like(path_split[1])
            except Exception as err:
                raise MalformedParameterPathError(
                    f"Cannot parse a resource action scope from the second component of the "
                    f"path: {path!r}. Exception was: {err}."
                ) from None

            if len(path_split) > 2:
                if path_split[2] not in ResourceSpec.ALLOWED_PARAMETERS:
                    allowed_keys_str = ", ".join(
                        f'"{i}"' for i in ResourceSpec.ALLOWED_PARAMETERS
                    )
                    raise UnknownResourceSpecItemError(
                        f"Resource item name {path_split[2]!r} is unknown. Allowed "
                        f"resource item names are: {allowed_keys_str}."
                    )
            label = ""

        elif path_split[0] == "environments":
            # rewrite as a resources path:
            path = f"resources.any.{path}"
            label = str(label) if label is not None else ""
        else:
            pass
            # note: `env_preset` paths also need to be transformed into `resources`
            # paths, but we cannot do that until the sequence is part of a task, since
            # the available environment presets are defined in the task schema.

        return path, label

    def to_dict(self):
        out = super().to_dict()
        del out["_parameter"]
        del out["_path_split"]
        if "_workflow" in out:
            del out["_workflow"]
        return out

    @property
    def normalised_path(self) -> str:
        return self.path

    @property
    def normalised_inputs_path(self) -> str | None:
        """Return the normalised path without the "inputs" prefix, if the sequence is an
        inputs sequence, else return None."""

        if self.input_type:
            if self.input_path:
                return f"{self.labelled_type}.{self.input_path}"
            else:
                return self.labelled_type
        return None

    def make_persistent(
        self, workflow: Workflow, source: ParamSource
    ) -> tuple[str, list[int], bool]:
        """Save value to a persistent workflow."""

        if self._values_group_idx is not None:
            if not all(workflow.check_parameters_exist(self._values_group_idx)):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a parameter group index "
                    f"({self._values_group_idx}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
            return self.normalised_path, self._values_group_idx, False

        data_ref: list[int] = []
        source = copy.deepcopy(source)
        if self.value_class_method:
            source["value_class_method"] = self.value_class_method
        are_objs: list[bool] = []
        assert self._values is not None
        for idx, i in enumerate(self._values):
            # record if ParameterValue sub-classes are passed for values, which allows
            # us to re-init the objects on access to `.value`:
            are_objs.append(isinstance(i, ParameterValue))
            source = copy.deepcopy(source)
            source["sequence_idx"] = idx
            pg_idx_i = workflow._add_parameter_data(i, source=source)
            data_ref.append(pg_idx_i)

        self._values_group_idx = data_ref
        self._workflow = workflow
        self._values = None
        self._values_are_objs = are_objs
        return self.normalised_path, data_ref, True

    @property
    def workflow(self) -> Workflow | None:
        if self._workflow:
            return self._workflow
        elif self._element_set:
            tmpl = self._element_set.task_template.workflow_template
            if tmpl:
                return tmpl.workflow
        return None

    @property
    def values(self) -> list[Any] | None:
        if self._values_group_idx is not None:
            vals: list[Any] = []
            for idx, pg_idx_i in enumerate(self._values_group_idx):
                w = self.workflow
                if not w:
                    continue
                param_i = w.get_parameter(pg_idx_i)
                if param_i.data is not None:
                    val_i = param_i.data
                else:
                    val_i = param_i.file

                # `val_i` might already be a `_value_class` object if the store has not
                # yet been committed to disk:
                if (
                    self.parameter
                    and self._values_are_objs
                    and self._values_are_objs[idx]
                    and isinstance(val_i, dict)
                ):
                    val_i = self.parameter._instantiate_value(param_i.source, val_i)

                vals.append(val_i)
            return vals
        else:
            return self._values

    @classmethod
    def _values_from_linear_space(
        cls, start: float, stop: float, num: int, **kwargs
    ) -> list[float]:
        return np.linspace(start, stop, num=num, **kwargs).tolist()

    @classmethod
    def _values_from_geometric_space(
        cls, start: float, stop: float, num: int, **kwargs
    ) -> list[float]:
        return np.geomspace(start, stop, num=num, **kwargs).tolist()

    @classmethod
    def _values_from_log_space(
        cls, start: float, stop: float, num: int, base: float = 10.0, **kwargs
    ) -> list[float]:
        return np.logspace(start, stop, num=num, base=base, **kwargs).tolist()

    @classmethod
    def _values_from_range(
        cls, start: int | float, stop: int | float, step: int | float, **kwargs
    ) -> list[float]:
        return np.arange(start, stop, step, **kwargs).tolist()

    @classmethod
    def _values_from_file(cls, file_path: str | Path) -> list[str]:
        with Path(file_path).open("rt") as fh:
            vals = [i.strip() for i in fh.readlines()]
        return vals

    @classmethod
    def _values_from_rectangle(
        cls,
        start: Sequence[float],
        stop: Sequence[float],
        num: Sequence[int],
        coord: int | tuple[int, int] | None = None,
        include: Sequence[str] | None = None,
        **kwargs,
    ) -> list[float]:
        vals = linspace_rect(start=start, stop=stop, num=num, include=include, **kwargs)
        if coord is not None:
            return vals[coord].tolist()
        else:
            return (vals.T).tolist()

    @classmethod
    def _values_from_random_uniform(
        cls,
        num: int,
        low: float = 0.0,
        high: float = 1.0,
        seed: int | list[int] | None = None,
    ) -> list[float]:
        rng = np.random.default_rng(seed)
        return rng.uniform(low=low, high=high, size=num).tolist()

    @classmethod
    def from_linear_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        # TODO: save persistently as an array?
        args = {"start": start, "stop": stop, "num": num, **kwargs}
        values = cls._values_from_linear_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_linear_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_geometric_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        endpoint=True,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        args = {"start": start, "stop": stop, "num": num, "endpoint": endpoint, **kwargs}
        values = cls._values_from_geometric_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_geometric_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_log_space(
        cls,
        path: str,
        start: float,
        stop: float,
        num: int,
        nesting_order: float = 0,
        base=10.0,
        endpoint=True,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        args = {
            "start": start,
            "stop": stop,
            "num": num,
            "endpoint": endpoint,
            "base": base,
            **kwargs,
        }
        values = cls._values_from_log_space(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_log_space"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_range(
        cls,
        path: str,
        start: float,
        stop: float,
        nesting_order: float = 0,
        step: int | float = 1,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        # TODO: save persistently as an array?
        args = {"start": start, "stop": stop, "step": step, **kwargs}
        if isinstance(step, int):
            values = cls._values_from_range(**args)
        else:
            # Use linspace for non-integer step, as recommended by Numpy:
            values = cls._values_from_linear_space(
                start=start,
                stop=stop,
                num=int((stop - start) / step),
                endpoint=False,
                **kwargs,
            )
        obj = cls(
            values=values,
            path=path,
            nesting_order=nesting_order,
            label=label,
        )
        obj._values_method = "from_range"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_file(
        cls,
        path: str,
        file_path: str | Path,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        args = {"file_path": file_path, **kwargs}
        values = cls._values_from_file(**args)
        obj = cls(
            values=values,
            path=path,
            nesting_order=nesting_order,
            label=label,
        )

        obj._values_method = "from_file"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_rectangle(
        cls,
        path: str,
        start: Sequence[float],
        stop: Sequence[float],
        num: Sequence[int],
        coord: int | None = None,
        include: list[str] | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        """
        Parameters
        ----------
        coord:
            Which coordinate to use. Either 0, 1, or `None`, meaning each value will be
            both coordinates.
        include
            If specified, include only the specified edges. Choose from "top", "right",
            "bottom", "left".
        """
        args = {
            "start": start,
            "stop": stop,
            "num": num,
            "coord": coord,
            "include": include,
            **kwargs,
        }
        values = cls._values_from_rectangle(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_rectangle"
        obj._values_method_args = args
        return obj

    @classmethod
    def from_random_uniform(
        cls,
        path,
        num: int,
        low: float = 0.0,
        high: float = 1.0,
        seed: int | list[int] | None = None,
        nesting_order: float = 0,
        label: str | int | None = None,
        **kwargs,
    ) -> Self:
        args = {"low": low, "high": high, "num": num, "seed": seed, **kwargs}
        values = cls._values_from_random_uniform(**args)
        obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
        obj._values_method = "from_random_uniform"
        obj._values_method_args = args
        return obj


@dataclass
class AbstractInputValue(JSONLike):
    """Class to represent all sequence-able inputs to a task."""

    _workflow: Workflow | None = None
    _element_set: ElementSet | None = None
    _schema_input: SchemaInput | None = None
    _value: Any | None = None
    _value_group_idx: int | list[int] | None = None

    def __repr__(self) -> str:
        try:
            value_str = f", value={self.value}"
        except WorkflowParameterMissingError:
            value_str = ""

        return (
            f"{self.__class__.__name__}("
            f"_value_group_idx={self._value_group_idx}"
            f"{value_str}"
            f")"
        )

    def to_dict(self) -> dict[str, Any]:
        out = super().to_dict()
        if "_workflow" in out:
            del out["_workflow"]
        if "_schema_input" in out:
            del out["_schema_input"]
        return out

    def make_persistent(
        self, workflow: Workflow, source: ParamSource
    ) -> tuple[str, list[int | list[int]], bool]:
        """Save value to a persistent workflow.

        Returns
        -------
        String is the data path for this task input and single item integer list
        contains the index of the parameter data Zarr group where the data is
        stored.

        """

        if self._value_group_idx is not None:
            data_ref = self._value_group_idx
            is_new = False
            if not workflow.check_parameters_exist(data_ref):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a data reference "
                    f"({data_ref}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
        else:
            data_ref = workflow._add_parameter_data(self._value, source=source)
            self._value_group_idx = data_ref
            is_new = True
            self._value = None

        return (self.normalised_path, [data_ref], is_new)

    @property
    def normalised_path(self) -> str:
        raise NotImplementedError

    @property
    def workflow(self) -> Workflow | None:
        if self._workflow:
            return self._workflow
        elif self._element_set:
            w_tmpl = self._element_set.task_template.workflow_template
            if w_tmpl:
                return w_tmpl.workflow
        if self._schema_input:
            t_tmpl = self._schema_input.task_schema.task_template
            if t_tmpl:
                w_tmpl = t_tmpl.workflow_template
                if w_tmpl:
                    return w_tmpl.workflow
        return None

    @property
    def value(self) -> Any:
        return self._value


@dataclass
class ValuePerturbation(AbstractInputValue):
    name: str = ""
    path: Sequence[str | int | float] | None = None
    multiplicative_factor: Numeric | None = 1
    additive_factor: Numeric | None = 0

    def __post_init__(self):
        assert self.name

    @classmethod
    def from_spec(cls, spec):
        return cls(**spec)


@hydrate
class InputValue(AbstractInputValue):
    """
    Parameters
    ----------
    parameter
        Parameter whose value is to be specified
    label
        Optional identifier to be used where the associated `SchemaInput` accepts multiple
        parameters of the specified type. This will be cast to a string.
    value
        The input parameter value.
    value_class_method
        A class method that can be invoked with the `value` attribute as keyword
        arguments.
    path
        Dot-delimited path within the parameter's nested data structure for which `value`
        should be set.

    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="parameter",
            class_name="Parameter",
            shared_data_primary_key="typ",
            shared_data_name="parameters",
        ),
    )

    def __init__(
        self,
        parameter: Parameter | SchemaInput | str,
        value: Any | None = None,
        label: str | Any = None,
        value_class_method: str | None = None,
        path: str | None = None,
        _check_obj: bool = True,
    ):
        super().__init__()
        if isinstance(parameter, str):
            try:
                self.parameter = self._app.parameters.get(parameter)
            except ValueError:
                self.parameter = self._app.Parameter(parameter)
        elif isinstance(parameter, SchemaInput):
            self.parameter = parameter.parameter
        else:
            self.parameter = parameter

        self.label = str(label) if label is not None else ""
        self.path = (path.strip(".") if path else None) or None
        self.value_class_method = value_class_method
        self._value = _process_demo_data_strings(self._app, value)

        # record if a ParameterValue sub-class is passed for value, which allows us
        # to re-init the object on `.value`:
        self._value_is_obj = isinstance(value, ParameterValue)
        if _check_obj:
            self._check_dict_value_if_object()

    def _check_dict_value_if_object(self):
        """For non-persistent input values, check that, if a matching `ParameterValue`
        class exists and the specified value is not of that type, then the specified
        value is a dict, which can later be passed to the ParameterValue sub-class
        to initialise the object.
        """
        if (
            self._value_group_idx is None
            and not self.path
            and not self._value_is_obj
            and self.parameter._value_class
            and self._value is not None
            and not isinstance(self._value, dict)
        ):
            raise ValueError(
                f"{self.__class__.__name__} with specified value {self._value!r} is "
                f"associated with a ParameterValue subclass "
                f"({self.parameter._value_class!r}), but the value data type is not a "
                f"dict."
            )

    def __deepcopy__(self, memo) -> Self:
        kwargs = self.to_dict()
        _value = kwargs.pop("_value")
        kwargs.pop("_schema_input", None)
        _value_group_idx = kwargs.pop("_value_group_idx")
        _value_is_obj = kwargs.pop("_value_is_obj")
        obj = self.__class__(**copy.deepcopy(kwargs, memo), _check_obj=False)
        obj._value = _value
        obj._value_group_idx = _value_group_idx
        obj._value_is_obj = _value_is_obj
        obj._element_set = self._element_set
        obj._schema_input = self._schema_input
        return obj

    def __repr__(self) -> str:
        val_grp_idx = ""
        if self._value_group_idx is not None:
            val_grp_idx = f", value_group_idx={self._value_group_idx}"

        path_str = ""
        if self.path is not None:
            path_str = f", path={self.path!r}"

        label_str = ""
        if self.label is not None:
            label_str = f", label={self.label!r}"

        try:
            value_str = f", value={self.value!r}"
        except WorkflowParameterMissingError:
            value_str = ""

        return (
            f"{self.__class__.__name__}("
            f"parameter={self.parameter.typ!r}{label_str}"
            f"{value_str}"
            f"{path_str}"
            f"{val_grp_idx}"
            f")"
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.to_dict() == other.to_dict()

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _value_group_idx = json_like.pop("_value_group_idx", None)
        _value_is_obj = json_like.pop("_value_is_obj", None)
        if "_value" in json_like:
            json_like["value"] = json_like.pop("_value")

        obj = cls(**json_like, _check_obj=False)
        obj._value_group_idx = _value_group_idx
        obj._value_is_obj = _value_is_obj
        obj._check_dict_value_if_object()
        return obj

    @property
    def labelled_type(self) -> str:
        label = f"[{self.label}]" if self.label else ""
        return f"{self.parameter.typ}{label}"

    @property
    def normalised_inputs_path(self) -> str:
        return f"{self.labelled_type}{f'.{self.path}' if self.path else ''}"

    @property
    def normalised_path(self) -> str:
        return f"inputs.{self.normalised_inputs_path}"

    def make_persistent(
        self, workflow: Workflow, source: ParamSource
    ) -> tuple[str, list[int | list[int]], bool]:
        source = copy.deepcopy(source)
        if self.value_class_method is not None:
            source["value_class_method"] = self.value_class_method
        return super().make_persistent(workflow, source)

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if "[" in json_like["parameter"]:
            # extract out the parameter label:
            param, label = split_param_label(json_like["parameter"])
            json_like["parameter"] = param
            json_like["label"] = label

        if "::" in json_like["parameter"]:
            param, cls_method = json_like["parameter"].split("::")
            json_like["parameter"] = param
            json_like["value_class_method"] = cls_method

        if "path" not in json_like:
            param_spec = json_like["parameter"].split(".")
            json_like["parameter"] = param_spec[0]
            json_like["path"] = ".".join(param_spec[1:])

        return super().from_json_like(json_like, shared_data)

    @property
    def is_sub_value(self) -> bool:
        """True if the value is for a sub part of the parameter (i.e. if `path` is set).
        Sub-values are not added to the base parameter data, but are interpreted as
        single-value sequences."""
        return True if self.path else False

    @property
    def value(self) -> Any:
        if self._value_group_idx is not None and self.workflow:
            val = self.workflow.get_parameter_data(cast(int, self._value_group_idx))
            if self._value_is_obj and self.parameter._value_class:
                return self.parameter._value_class(**val)
            return val
        else:
            return self._value


class ResourceSpecArgs(TypedDict):
    """
    Supported keyword arguments for a ResourceSpec.
    """

    scope: NotRequired[ActionScope | str]
    scratch: NotRequired[str]
    parallel_mode: NotRequired[str | ParallelMode]
    num_cores: NotRequired[int]
    num_cores_per_node: NotRequired[int]
    num_threads: NotRequired[int]
    num_nodes: NotRequired[int]
    scheduler: NotRequired[str]
    shell: NotRequired[str]
    use_job_array: NotRequired[bool]
    max_array_items: NotRequired[int]
    time_limit: NotRequired[str | timedelta]
    scheduler_args: NotRequired[dict[str, Any]]
    shell_args: NotRequired[dict[str, Any]]
    os_name: NotRequired[str]
    environments: NotRequired[dict[str, dict[str, Any]]]
    SGE_parallel_env: NotRequired[str]
    SLURM_partition: NotRequired[str]
    SLURM_num_tasks: NotRequired[str]
    SLURM_num_tasks_per_node: NotRequired[str]
    SLURM_num_nodes: NotRequired[str]
    SLURM_num_cpus_per_task: NotRequired[str]


class ResourceSpec(JSONLike):
    """Class to represent specification of resource requirements for a (set of) actions.

    Notes
    -----
    `os_name` is used for retrieving a default shell name and for retrieving the correct
    `Shell` class; when using WSL, it should still be `nt` (i.e. Windows).

    """

    ALLOWED_PARAMETERS: ClassVar[set[str]] = {
        "scratch",
        "parallel_mode",
        "num_cores",
        "num_cores_per_node",
        "num_threads",
        "num_nodes",
        "scheduler",
        "shell",
        "use_job_array",
        "max_array_items",
        "time_limit",
        "scheduler_args",
        "shell_args",
        "os_name",
        "environments",
        "SGE_parallel_env",
        "SLURM_partition",
        "SLURM_num_tasks",
        "SLURM_num_tasks_per_node",
        "SLURM_num_nodes",
        "SLURM_num_cpus_per_task",
    }

    _resource_list: ResourceList | None = None

    _child_objects = (
        ChildObjectSpec(
            name="scope",
            class_name="ActionScope",
        ),
    )

    @staticmethod
    def __parse_thing(
        typ: type[ActionScope], val: ActionScope | str | None
    ) -> ActionScope | None:
        if isinstance(val, typ):
            return val
        elif val is None:
            return typ.any()
        else:
            return typ.from_json_like(cast(str, val))

    def __init__(
        self,
        scope: ActionScope | str | None = None,
        scratch: str | None = None,
        parallel_mode: str | ParallelMode | None = None,
        num_cores: int | None = None,
        num_cores_per_node: int | None = None,
        num_threads: int | None = None,
        num_nodes: int | None = None,
        scheduler: str | None = None,
        shell: str | None = None,
        use_job_array: bool | None = None,
        max_array_items: int | None = None,
        time_limit: str | timedelta | None = None,
        scheduler_args: dict[str, Any] | None = None,
        shell_args: dict[str, Any] | None = None,
        os_name: str | None = None,
        environments: dict[str, dict[str, Any]] | None = None,
        SGE_parallel_env: str | None = None,
        SLURM_partition: str | None = None,
        SLURM_num_tasks: str | None = None,
        SLURM_num_tasks_per_node: str | None = None,
        SLURM_num_nodes: str | None = None,
        SLURM_num_cpus_per_task: str | None = None,
    ):
        self.scope = self.__parse_thing(self._app.ActionScope, scope)

        if isinstance(time_limit, timedelta):
            time_limit = timedelta_format(time_limit)

        # assigned by `make_persistent`
        self._workflow: Workflow | None = None
        self._value_group_idx: int | list[int] | None = None

        # user-specified resource parameters:
        self._scratch = scratch
        self._parallel_mode = get_enum_by_name_or_val(ParallelMode, parallel_mode)
        self._num_cores = num_cores
        self._num_threads = num_threads
        self._num_nodes = num_nodes
        self._num_cores_per_node = num_cores_per_node
        self._scheduler = self._process_string(scheduler)
        self._shell = self._process_string(shell)
        self._os_name = self._process_string(os_name)
        self._environments = environments
        self._use_job_array = use_job_array
        self._max_array_items = max_array_items
        self._time_limit = time_limit
        self._scheduler_args = scheduler_args
        self._shell_args = shell_args

        # user-specified SGE-specific parameters:
        self._SGE_parallel_env = SGE_parallel_env

        # user-specified SLURM-specific parameters:
        self._SLURM_partition = SLURM_partition
        self._SLURM_num_tasks = SLURM_num_tasks
        self._SLURM_num_tasks_per_node = SLURM_num_tasks_per_node
        self._SLURM_num_nodes = SLURM_num_nodes
        self._SLURM_num_cpus_per_task = SLURM_num_cpus_per_task

    def __deepcopy__(self, memo):
        kwargs = copy.deepcopy(self.to_dict(), memo)
        _value_group_idx = kwargs.pop("value_group_idx", None)
        obj = self.__class__(**kwargs)
        obj._value_group_idx = _value_group_idx
        obj._resource_list = self._resource_list
        return obj

    def __repr__(self):
        param_strs = ""
        for i in self.ALLOWED_PARAMETERS:
            i_str = ""
            try:
                i_val = getattr(self, i)
            except WorkflowParameterMissingError:
                pass
            else:
                if i_val is not None:
                    i_str = f", {i}={i_val!r}"

            param_strs += i_str

        return f"{self.__class__.__name__}(scope={self.scope}{param_strs})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.to_dict() == other.to_dict():
            return True
        return False

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _value_group_idx = json_like.pop("value_group_idx", None)
        try:
            obj = cls(**json_like)
        except TypeError:
            given_keys = set(k for k in json_like.keys() if k != "scope")
            bad_keys = given_keys - cls.ALLOWED_PARAMETERS
            bad_keys_str = ", ".join(f'"{i}"' for i in bad_keys)
            allowed_keys_str = ", ".join(f'"{i}"' for i in cls.ALLOWED_PARAMETERS)
            raise UnknownResourceSpecItemError(
                f"The following resource item names are unknown: {bad_keys_str}. Allowed "
                f"resource item names are: {allowed_keys_str}."
            )
        obj._value_group_idx = _value_group_idx

        return obj

    @property
    def normalised_resources_path(self) -> str:
        scope = self.scope
        assert scope is not None
        return scope.to_string()

    @property
    def normalised_path(self) -> str:
        return f"resources.{self.normalised_resources_path}"

    def to_dict(self):
        out = super().to_dict()
        if "_workflow" in out:
            del out["_workflow"]

        if self._value_group_idx is not None:
            # only store pointer to persistent data:
            out = {k: v for k, v in out.items() if k in ["_value_group_idx", "scope"]}
        else:
            out = {k: v for k, v in out.items() if v is not None}

        out = {k.lstrip("_"): v for k, v in out.items()}
        return out

    def _get_members(self):
        out = self.to_dict()
        out.pop("scope")
        out.pop("value_group_idx", None)
        out = {k: v for k, v in out.items() if v is not None}
        return out

    def make_persistent(
        self, workflow: Workflow, source: ParamSource
    ) -> tuple[str, list[int | list[int]], bool]:
        """Save to a persistent workflow.

        Returns
        -------
        String is the data path for this task input and integer list
        contains the indices of the parameter data Zarr groups where the data is
        stored.

        """

        if self._value_group_idx is not None:
            data_ref = self._value_group_idx
            is_new = False
            if not workflow.check_parameters_exist(data_ref):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a parameter group index "
                    f"({data_ref}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
        else:
            data_ref = workflow._add_parameter_data(self._get_members(), source=source)
            is_new = True
            self._value_group_idx = data_ref
            self._workflow = workflow

            self._num_cores = None
            self._scratch = None
            self._scheduler = None
            self._shell = None
            self._use_job_array = None
            self._max_array_items = None
            self._time_limit = None
            self._scheduler_args = None
            self._shell_args = None
            self._os_name = None
            self._environments = None

        return (self.normalised_path, [data_ref], is_new)

    def copy_non_persistent(self):
        """Make a non-persistent copy."""
        kwargs = {"scope": self.scope}
        for name in self.ALLOWED_PARAMETERS:
            kwargs[name] = getattr(self, name)
        return self.__class__(**kwargs)

    def _get_value(self, value_name: str | None = None):
        if self._value_group_idx is not None and self.workflow:
            val = self.workflow.get_parameter_data(cast(int, self._value_group_idx))
        else:
            val = self._get_members()
        if value_name is not None and val is not None:
            return val.get(value_name)

        return val

    @staticmethod
    def _process_string(value: str | None):
        return value.lower().strip() if value else value

    def _setter_persistent_check(self):
        if self._value_group_idx:
            raise ValueError(
                f"Cannot set attribute of a persistent {self.__class__.__name__!r}."
            )

    @property
    def scratch(self) -> str | None:
        # TODO: currently unused, except in tests
        return self._get_value("scratch")

    @property
    def parallel_mode(self) -> ParallelMode | None:
        return self._get_value("parallel_mode")

    @property
    def num_cores(self) -> int | None:
        return self._get_value("num_cores")

    @property
    def num_cores_per_node(self) -> int | None:
        return self._get_value("num_cores_per_node")

    @property
    def num_nodes(self) -> int | None:
        return self._get_value("num_nodes")

    @property
    def num_threads(self) -> int | None:
        return self._get_value("num_threads")

    @property
    def scheduler(self) -> str | None:
        return self._get_value("scheduler")

    @scheduler.setter
    def scheduler(self, value: str | None):
        self._setter_persistent_check()
        self._scheduler = self._process_string(value)

    @property
    def shell(self) -> str | None:
        return self._get_value("shell")

    @shell.setter
    def shell(self, value: str | None):
        self._setter_persistent_check()
        self._shell = self._process_string(value)

    @property
    def use_job_array(self) -> bool:
        return self._get_value("use_job_array")

    @property
    def max_array_items(self) -> int | None:
        return self._get_value("max_array_items")

    @property
    def time_limit(self) -> str | None:
        return self._get_value("time_limit")

    @property
    def scheduler_args(self) -> dict:
        return self._get_value("scheduler_args")

    @property
    def shell_args(self) -> dict | None:
        return self._get_value("shell_args")

    @property
    def os_name(self) -> str:
        return self._get_value("os_name")

    @os_name.setter
    def os_name(self, value: str):
        self._setter_persistent_check()
        self._os_name = self._process_string(value)

    @property
    def environments(self) -> dict | None:
        return self._get_value("environments")

    @property
    def SGE_parallel_env(self) -> str | None:
        return self._get_value("SGE_parallel_env")

    @property
    def SLURM_partition(self) -> str | None:
        return self._get_value("SLURM_partition")

    @property
    def SLURM_num_tasks(self) -> int | None:
        return self._get_value("SLURM_num_tasks")

    @property
    def SLURM_num_tasks_per_node(self) -> int | None:
        return self._get_value("SLURM_num_tasks_per_node")

    @property
    def SLURM_num_nodes(self) -> int | None:
        return self._get_value("SLURM_num_nodes")

    @property
    def SLURM_num_cpus_per_task(self) -> int | None:
        return self._get_value("SLURM_num_cpus_per_task")

    @property
    def workflow(self) -> Workflow | None:
        if self._workflow:
            return self._workflow

        elif self.element_set:
            # element-set-level resources
            wt = self.element_set.task_template.workflow_template
            return wt.workflow if wt else None

        elif self.workflow_template:
            # template-level resources
            return self.workflow_template.workflow

        elif self._value_group_idx is not None:
            raise RuntimeError(
                f"`{self.__class__.__name__}._value_group_idx` is set but the `workflow` "
                f"attribute is not. This might be because we are in the process of "
                f"creating the workflow object."
            )

        return None

    @property
    def element_set(self) -> ElementSet | None:
        if not self._resource_list:
            return None
        return self._resource_list.element_set

    @property
    def workflow_template(self) -> WorkflowTemplate | None:
        if not self._resource_list:
            return None
        return self._resource_list.workflow_template


class InputSourceType(enum.Enum):
    IMPORT = 0
    LOCAL = 1
    DEFAULT = 2
    TASK = 3


class TaskSourceType(enum.Enum):
    INPUT = 0
    OUTPUT = 1
    ANY = 2


_Where: TypeAlias = "RuleArgs | Rule | Sequence[RuleArgs | Rule] | ElementFilter"


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
        source_type: InputSourceType | str,
        import_ref: int | None = None,
        task_ref: int | None = None,
        task_source_type: TaskSourceType | str | None = None,
        element_iters: list[int] | None = None,
        path: str | None = None,
        where: _Where | None = None,
    ):
        if where is None or isinstance(where, ElementFilter):
            self.where: ElementFilter | None = where
        else:
            self.where = self._app.ElementFilter(
                rules=[
                    rule if isinstance(rule, Rule) else Rule(**rule)
                    for rule in (where if isinstance(where, Sequence) else [where])
                ]
            )

        self.source_type = get_enum_by_name_or_val(InputSourceType, source_type)
        self.import_ref = import_ref
        self.task_ref = task_ref
        self.task_source_type = get_enum_by_name_or_val(TaskSourceType, task_source_type)
        self.element_iters = element_iters
        self.path = path

        if self.source_type is InputSourceType.TASK:
            if self.task_ref is None:
                raise ValueError("Must specify `task_ref` if `source_type` is TASK.")
            if self.task_source_type is None:
                self.task_source_type = TaskSourceType.OUTPUT

        if self.source_type is InputSourceType.IMPORT and self.import_ref is None:
            raise ValueError("Must specify `import_ref` if `source_type` is IMPORT.")

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        elif (
            self.source_type == other.source_type
            and self.import_ref == other.import_ref
            and self.task_ref == other.task_ref
            and self.task_source_type == other.task_source_type
            and self.element_iters == other.element_iters
            and self.where == other.where
            and self.path == other.path
        ):
            return True
        else:
            return False

    def __repr__(self) -> str:
        assert self.source_type
        cls_method_name = self.source_type.name.lower()

        args_lst = []

        if self.source_type is InputSourceType.IMPORT:
            cls_method_name += "_"
            args_lst.append(f"import_ref={self.import_ref}")

        elif self.source_type is InputSourceType.TASK:
            assert self.task_source_type
            args_lst += (
                f"task_ref={self.task_ref}",
                f"task_source_type={self.task_source_type.name.lower()!r}",
            )

        if self.element_iters is not None:
            args_lst.append(f"element_iters={self.element_iters}")

        if self.where is not None:
            args_lst.append(f"where={self.where!r}")

        args = ", ".join(args_lst)
        out = f"{self.__class__.__name__}.{cls_method_name}({args})"

        return out

    def get_task(self, workflow: Workflow) -> WorkflowTask | None:
        """If source_type is task, then return the referenced task from the given
        workflow."""
        if self.source_type is InputSourceType.TASK:
            for task in workflow.tasks:
                if task.insert_ID == self.task_ref:
                    return task
        return None

    def is_in(self, other_input_sources: list[InputSource]) -> int | None:
        """Check if this input source is in a list of other input sources, without
        considering the `element_iters` and `where` attributes."""

        for idx, other in enumerate(other_input_sources):
            if (
                self.source_type == other.source_type
                and self.import_ref == other.import_ref
                and self.task_ref == other.task_ref
                and self.task_source_type == other.task_source_type
                and self.path == other.path
            ):
                return idx
        return None

    def to_string(self) -> str:
        assert self.source_type
        out = [self.source_type.name.lower()]
        if self.source_type is InputSourceType.TASK:
            assert self.task_source_type
            out += [str(self.task_ref), self.task_source_type.name.lower()]
            if self.element_iters is not None:
                out += ["[" + ",".join(f"{i}" for i in self.element_iters) + "]"]
        elif self.source_type is InputSourceType.IMPORT:
            out += [str(self.import_ref)]
        return ".".join(out)

    @classmethod
    def from_string(cls, str_defn: str) -> Self:
        return cls(**cls._parse_from_string(str_defn))

    @classmethod
    def _parse_from_string(cls, str_defn: str):
        """Parse a dot-delimited string definition of an InputSource.

        Examples:
            - task.[task_ref].input
            - task.[task_ref].output
            - local
            - default
            - import.[import_ref]

        """
        parts = str_defn.split(".")
        source_type = get_enum_by_name_or_val(InputSourceType, parts[0])
        task_ref: int | None = None
        task_source_type: TaskSourceType | None = None
        import_ref: int | None = None
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
            # TODO: does this include element_iters?
            try:
                task_ref = int(parts[1])
            except ValueError:
                pass
            try:
                task_source_type = get_enum_by_name_or_val(TaskSourceType, parts[2])
            except IndexError:
                task_source_type = TaskSourceType.OUTPUT
        elif source_type is InputSourceType.IMPORT:
            try:
                import_ref = int(parts[1])
            except ValueError:
                pass

        return {
            "source_type": source_type,
            "import_ref": import_ref,
            "task_ref": task_ref,
            "task_source_type": task_source_type,
        }

    @classmethod
    def import_(
        cls,
        import_ref: int,
        element_iters: list[int] | None = None,
        where: _Where | None = None,
    ) -> Self:
        return cls(
            source_type=InputSourceType.IMPORT,
            import_ref=import_ref,
            element_iters=element_iters,
            where=where,
        )

    @classmethod
    def local(cls) -> Self:
        return cls(source_type=InputSourceType.LOCAL)

    @classmethod
    def default(cls) -> Self:
        return cls(source_type=InputSourceType.DEFAULT)

    @classmethod
    def task(
        cls,
        task_ref: int,
        task_source_type: TaskSourceType | str | None = None,
        element_iters: list[int] | None = None,
        where: _Where | None = None,
    ) -> Self:
        return cls(
            source_type=InputSourceType.TASK,
            task_ref=task_ref,
            task_source_type=get_enum_by_name_or_val(
                TaskSourceType, task_source_type or TaskSourceType.OUTPUT
            ),
            where=where,
            element_iters=element_iters,
        )
