from importlib import resources
from typing import Any, Protocol
from valida import Schema as VSchema  # type: ignore


class ValidatedData(Protocol):
    @property
    def is_valid(self) -> bool:
        ...
    def get_failures_string(self) -> str:
        ...


class Schema(Protocol, VSchema):
    def validate(self, data: Any) -> ValidatedData:
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
    schema = Schema.from_yaml(schema_dat)
    return schema
