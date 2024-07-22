from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from textwrap import dedent

from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.errors import DuplicateExecutableError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.object_list import ExecutablesList
from hpcflow.sdk.core.utils import check_valid_py_identifier, get_duplicate_items

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import ClassVar
    from ..app import BaseApp


@dataclass
class NumCores(JSONLike):
    start: int
    stop: int
    step: int = 1

    def __contains__(self, x) -> bool:
        return x in range(self.start, self.stop + 1, self.step)


@dataclass
@hydrate
class ExecutableInstance(JSONLike):
    app: ClassVar[BaseApp]
    parallel_mode: str | None
    num_cores: NumCores
    command: str

    def __init__(
        self, parallel_mode: str | None, num_cores: NumCores | int | dict, command: str
    ):
        self.parallel_mode = parallel_mode
        self.command = command
        if isinstance(num_cores, NumCores):
            self.num_cores = num_cores
        elif isinstance(num_cores, int):
            self.num_cores = NumCores(num_cores, num_cores)
        else:
            self.num_cores = NumCores(**num_cores)

    @classmethod
    def from_spec(cls, spec) -> ExecutableInstance:
        return cls(**spec)


class Executable(JSONLike):
    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="instances",
            class_name="ExecutableInstance",
            is_multiple=True,
        ),
    )

    def __init__(self, label: str, instances: list[ExecutableInstance]):
        self.label = check_valid_py_identifier(label)
        self.instances = instances

        self._executables_list: ExecutablesList | None = None  # assigned by parent

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"label={self.label}, "
            f"instances={self.instances!r}"
            f")"
        )

    def __eq__(self, other):
        return (
            isinstance(other, Executable)
            and self.label == other.label
            and self.instances == other.instances
            and self.environment.name == other.environment.name
        )

    @property
    def environment(self):
        return self._executables_list.environment

    def filter_instances(
        self, parallel_mode: str | None = None, num_cores: int | None = None
    ) -> list[ExecutableInstance]:
        out: list[ExecutableInstance] = []
        for i in self.instances:
            if parallel_mode is None or i.parallel_mode == parallel_mode:
                if num_cores is None or num_cores in i.num_cores:
                    out.append(i)
        return out


class Environment(JSONLike):
    app: ClassVar[BaseApp]
    _validation_schema: ClassVar[str] = "environments_spec_schema.yaml"
    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="executables",
            class_name="ExecutablesList",
            parent_ref="environment",
        ),
    )

    def __init__(
        self,
        name: str,
        setup: Sequence[str] | None = None,
        specifiers: dict | None = None,
        executables: ExecutablesList | Sequence[Executable] | None = None,
        _hash_value: str | None = None,
    ):
        self.name = name
        self.specifiers = specifiers or {}
        self.executables = (
            executables
            if isinstance(executables, ExecutablesList)
            else self.app.ExecutablesList(executables or [])
        )
        self._hash_value = _hash_value
        self.setup: tuple[str, ...] | None
        if setup:
            if isinstance(setup, str):
                self.setup = tuple(i.strip() for i in dedent(setup).strip().split("\n"))
            else:
                self.setup = tuple(setup)
        else:
            self.setup = None
        self._set_parent_refs()
        self._validate()

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, Environment)
            and self.setup == other.setup
            and self.executables == other.executables
            and self.specifiers == other.specifiers
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name!r})"

    def _validate(self):
        dup_labels = get_duplicate_items(i.label for i in self.executables)
        if dup_labels:
            raise DuplicateExecutableError(
                f"Executables must have unique `label`s within each environment, but "
                f"found label(s) multiple times: {dup_labels!r}"
            )
