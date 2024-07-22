from __future__ import annotations
from collections.abc import Sequence
from importlib import resources
from typing import Any, Generic, Protocol, TypeVar
from valida import Schema as VSchema  # type: ignore

T = TypeVar("T")


class ValidatedData(Protocol, Generic[T]):
    @property
    def is_valid(self) -> bool:
        ...

    def get_failures_string(self) -> str:
        ...

    cast_data: T


class PreparedConditionCallable(Protocol):
    @property
    def name(self) -> str:
        ...

    @property
    def args(self) -> tuple[str, ...]:
        ...


class Condition(Protocol):
    @property
    def callable(self) -> PreparedConditionCallable:
        ...


class Rule(Protocol):
    @property
    def condition(self) -> Condition:
        ...

    @property
    def path(self) -> object:
        ...


class Schema(Protocol):
    def validate(self, data: T) -> ValidatedData[T]:
        ...

    @property
    def rules(self) -> Sequence[Rule]:
        ...

    def add_schema(self, schema: Schema, root_path: Any = None) -> None:
        ...


def get_schema(filename) -> Schema:
    """Get a valida `Schema` object from the embedded data directory."""
    package = "hpcflow.sdk.data"
    try:
        fh = resources.files(package).joinpath(filename).open("r")
    except AttributeError:
        # < python 3.9; `resource.open_text` deprecated since 3.11
        fh = resources.open_text(package, filename)
    schema_dat = fh.read()
    fh.close()
    schema = VSchema.from_yaml(schema_dat)
    return schema
