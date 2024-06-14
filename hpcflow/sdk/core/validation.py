from importlib import resources
from typing import Any, Generic, Protocol, TypeVar
from valida import Schema as VSchema  # type: ignore
from valida.rules import Rule as VRule  # type: ignore
from valida.conditions import (
    ConditionLike as _Condition,
    PreparedConditionCallable as Callable)

T = TypeVar('T')


class ValidatedData(Protocol, Generic[T]):
    @property
    def is_valid(self) -> bool: ...
    def get_failures_string(self) -> str: ...
    @property
    def cast_data(self) -> T: ...


class PreparedConditionCallable(Protocol, Callable):
    @property
    def name(self) -> str: ...
    @property
    def args(self) -> tuple[str, ...]: ...


class Condition(Protocol, _Condition):
    @property
    def callable(self) -> PreparedConditionCallable: ...


class Rule(Protocol, VRule):
    @property
    def condition(self) -> Condition: ...


class Schema(Protocol, VSchema):
    def validate(self, data: T) -> ValidatedData[T]: ...
    @property
    def rules(self) -> list[Rule]: ...


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
