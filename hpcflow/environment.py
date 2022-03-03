from dataclasses import dataclass, field
import re
from typing import List, Any, Dict, Optional, Sequence, Union

from textwrap import dedent

from hpcflow.errors import DuplicateExecutableError
from hpcflow.utils import check_valid_py_identifier, get_duplicate_items
from hpcflow.object_list import ExecutablesList


@dataclass
class NumCores:
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
class ExecutableInstance:
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
class Executable:
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

    @classmethod
    def from_spec(cls, spec):
        spec["instances"] = [
            ExecutableInstance.from_spec(i) for i in spec.get("instances", [])
        ]
        return cls(**spec)


@dataclass
class Environment:
    name: str
    setup: Optional[Sequence] = None
    executables: Optional[List[Executable]] = field(default_factory=lambda: [])

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

        self.executables = ExecutablesList(*self.executables)

        self._validate()

    def __eq__(self, other):
        if (
            type(self) == type(other)
            and self.setup == other.setup
            and self.executables == other.executables
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

    @classmethod
    def from_spec(cls, spec):
        spec["executables"] = [
            Executable.from_spec(i) for i in spec.get("executables", [])
        ]
        return cls(**spec)
