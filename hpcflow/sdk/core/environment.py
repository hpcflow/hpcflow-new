from dataclasses import dataclass, field
from typing import List, Any, Optional, Sequence

from textwrap import dedent

from hpcflow.sdk.core.errors import DuplicateExecutableError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import check_valid_py_identifier, get_duplicate_items
from hpcflow.sdk.core.object_list import ExecutablesList


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
        if not isinstance(self.num_cores, NumCores):
            self.num_cores = NumCores(**self.num_cores)

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


@dataclass
class Executable(JSONLike):

    _child_objects = (
        ChildObjectSpec(
            name="instances",
            class_name="ExecutableInstance",
            is_multiple=True,
        ),
    )

    label: str
    instances: List[ExecutableInstance] = field(repr=False, default_factory=lambda: [])
    environment: Any = field(default=None, repr=False)

    def __post_init__(self):
        self.label = check_valid_py_identifier(self.label)

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.label == other.label
            and self.instances == other.instances
            and self.environment.name == other.environment.name
        ):
            return True
        return False

    def filter_instances(self, parallel_mode=None, num_cores=None):
        out = []
        for i in self.instances:
            if parallel_mode is None or i.parallel_mode == parallel_mode:
                if num_cores is None or num_cores in i.num_cores:
                    out.append(i)
        return out


@dataclass
class Environment(JSONLike):

    app = None
    _validation_schema = "environments_spec_schema.yaml"
    _child_objects = (
        ChildObjectSpec(
            name="executables",
            class_name="Executable",
            is_multiple=True,
        ),
    )

    name: str
    setup: Optional[Sequence] = None
    specifiers: Optional[dict] = None
    executables: Optional[List[Executable]] = field(default_factory=lambda: [])
    _hash_value: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        for i in self.executables:
            i.environment = self

        if self.setup:
            if isinstance(self.setup, str):
                self.setup = tuple(
                    i.strip() for i in dedent(self.setup).strip().split("\n")
                )
            elif not isinstance(self.setup, tuple):
                self.setup = tuple(self.setup)

        self.executables = ExecutablesList(self.executables)

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

    def _validate(self):
        dup_labels = get_duplicate_items(i.label for i in self.executables)
        if dup_labels:
            raise DuplicateExecutableError(
                f"Executables must have unique `label`s within each environment, but "
                f"found label(s) multiple times: {dup_labels!r}"
            )
