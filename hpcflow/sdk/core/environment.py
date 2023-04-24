from dataclasses import dataclass, field
from typing import List, Any, Optional, Sequence

from textwrap import dedent

from hpcflow.sdk.core.errors import DuplicateExecutableError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import check_valid_py_identifier, get_duplicate_items


@dataclass
class NumCores(JSONLike):
    start: int
    stop: int
    step: int = None

    def __post_init__(self):
        if self.step is None:
            self.step = 1

    def __contains__(self, x):
        if x in range(self.start, self.stop + 1, self.step):
            return True
        else:
            return False

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.start == other.start
            and self.stop == other.stop
            and self.step == other.step
        ):
            return True
        return False


@dataclass
class ExecutableInstance(JSONLike):
    parallel_mode: str
    num_cores: Any
    command: str

    def __post_init__(self):
        if not isinstance(self.num_cores, dict):
            self.num_cores = {"start": self.num_cores, "stop": self.num_cores}
        if not isinstance(self.num_cores, self.app.NumCores):
            self.num_cores = self.app.NumCores(**self.num_cores)

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.parallel_mode == other.parallel_mode
            and self.num_cores == other.num_cores
            and self.command == other.command
        ):
            return True
        return False

    @classmethod
    def from_spec(cls, spec):
        return cls(**spec)


class Executable(JSONLike):

    _child_objects = (
        ChildObjectSpec(
            name="instances",
            class_name="ExecutableInstance",
            is_multiple=True,
        ),
    )

    def __init__(self, label: str, instances: List[ExecutableInstance]):

        self.label = check_valid_py_identifier(label)
        self.instances = instances

        self._executables_list = None  # assigned by parent

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"label={self.label}, "
            f"instances={self.instances!r}"
            f")"
        )

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.label == other.label
            and self.instances == other.instances
            and self.environment.name == other.environment.name
        ):
            return True
        return False

    @property
    def environment(self):
        return self._executables_list.environment

    def filter_instances(self, parallel_mode=None, num_cores=None):
        out = []
        for i in self.instances:
            if parallel_mode is None or i.parallel_mode == parallel_mode:
                if num_cores is None or num_cores in i.num_cores:
                    out.append(i)
        return out


class Environment(JSONLike):

    _hash_value = None
    _validation_schema = "environments_spec_schema.yaml"
    _child_objects = (
        ChildObjectSpec(
            name="executables",
            class_name="ExecutablesList",
            parent_ref="environment",
        ),
    )

    def __init__(
        self, name, setup=None, specifiers=None, executables=None, _hash_value=None
    ):
        self.name = name
        self.setup = setup
        self.specifiers = specifiers or {}
        self.executables = (
            executables
            if isinstance(executables, self.app.ExecutablesList)
            else self.app.ExecutablesList(executables or [])
        )
        self._hash_value = _hash_value
        if self.setup:
            if isinstance(self.setup, str):
                self.setup = tuple(
                    i.strip() for i in dedent(self.setup).strip().split("\n")
                )
            elif not isinstance(self.setup, tuple):
                self.setup = tuple(self.setup)
        self._set_parent_refs()
        self._validate()

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.setup == other.setup
            and self.executables == other.executables
            and self.specifiers == other.specifiers
        ):
            return True
        return False

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r})"

    def _validate(self):
        dup_labels = get_duplicate_items(i.label for i in self.executables)
        if dup_labels:
            raise DuplicateExecutableError(
                f"Executables must have unique `label`s within each environment, but "
                f"found label(s) multiple times: {dup_labels!r}"
            )
