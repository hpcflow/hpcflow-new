from __future__ import annotations

from dataclasses import dataclass
from typing import List, Any
from itertools import zip_longest
from textwrap import dedent
import re

from hpcflow.sdk import app
from hpcflow.sdk.core.errors import DuplicateExecutableError, SemanticVersionSpecError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.object_list import ExecutablesList
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
        if not isinstance(self.num_cores, NumCores):
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

    def __init__(self, label: str, instances: List[app.ExecutableInstance]):
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
            if isinstance(executables, ExecutablesList)
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


class SortableVersionSpec:
    def __init__(self, value) -> None:
        self.value = value

    def __eq__(self, __value: object) -> bool:
        return self.value == __value.value

    def __lt__(self, other) -> bool:
        return self.value < other.value


class SemanticVersionSpec(SortableVersionSpec):

    # used to indicate a given environment definition version specifier should use this
    # version spec class:
    id_ = "semantic"

    # from https://semver.org/
    RE_PATTERN = (
        r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
        r"(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
        r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
        r"(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
    )

    def __init__(self, value) -> None:
        super().__init__(value)
        self._parts = self._get_parts()

    @property
    def parts(self):
        return self._parts

    def _get_parts(self):
        match = re.match(self.RE_PATTERN, self.value)
        if not match:
            raise SemanticVersionSpecError(
                f"Version {self.value!r} does not seem to conform to the semantic "
                f"versioning specification as defined at https://semver.org/."
            )
        dct = match.groupdict()
        dct["major"] = int(dct["major"])
        dct["minor"] = int(dct["minor"])
        dct["patch"] = int(dct["patch"])
        if dct["prerelease"]:
            # split on dots, and try to cast to integers:
            dct["prerelease"] = dct["prerelease"].split(".")
            for idx, i in enumerate(dct["prerelease"]):
                try:
                    i_int = int(i)
                except ValueError:
                    pass
                else:
                    dct["prerelease"][idx] = i_int

        return dct

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, SemanticVersionSpec):
            __value = SemanticVersionSpec(__value)
        parts = {k: v for k, v in self.parts.items() if k != "buildmetadata"}
        parts_other = {k: v for k, v in __value.parts.items() if k != "buildmetadata"}
        return parts == parts_other

    def __gt__(self, other) -> bool:
        if not isinstance(other, SemanticVersionSpec):
            other = SemanticVersionSpec(other)
        return other <= self

    def __ge__(self, other) -> bool:
        if not isinstance(other, SemanticVersionSpec):
            other = SemanticVersionSpec(other)
        return self == other or self > other

    def __le__(self, other) -> bool:
        if not isinstance(other, SemanticVersionSpec):
            other = SemanticVersionSpec(other)
        return self == other or self < other

    def __lt__(self, other) -> bool:
        if not isinstance(other, SemanticVersionSpec):
            other = SemanticVersionSpec(other)
        parts = self.parts
        parts_o = other.parts
        if parts["major"] < parts_o["major"]:
            return True
        elif parts["major"] > parts_o["major"]:
            return False
        elif parts["minor"] < parts_o["minor"]:
            return True
        elif parts["minor"] > parts_o["minor"]:
            return False
        elif parts["patch"] < parts_o["patch"]:
            return True
        elif parts["patch"] > parts_o["patch"]:
            return False
        else:
            # same (major, minor, patch), look at prerelease (buildmetadata not
            # considered)

            # prerelease has lower precedence than normal release:
            if parts["prerelease"] and parts_o["prerelease"] is None:
                return True
            elif parts["prerelease"] is None and parts_o["prerelease"]:
                return False
            else:
                # both have some prerelease defined
                for i, j in zip_longest(parts["prerelease"], parts_o["prerelease"]):
                    if i is None:
                        return True
                    elif j is None:
                        return False
                    try:
                        if i < j:
                            return True
                        elif j < i:
                            return False
                        else:
                            continue
                    except TypeError:
                        # numeric identifiers have lower precedence:
                        if isinstance(i, int):
                            return True
                        elif isinstance(j, int):
                            return False
