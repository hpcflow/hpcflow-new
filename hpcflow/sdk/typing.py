from __future__ import annotations
from dataclasses import InitVar
from typing import ClassVar, TypedDict, TypeVar, cast
from typing_extensions import NotRequired, TypeAlias
from pathlib import Path
import re

PathLike: TypeAlias = "str | Path | None"
"""
Things we can convert into a proper path.
"""


class ParamSource(TypedDict):
    type: NotRequired[str]
    EAR_ID: NotRequired[int]
    task_insert_ID: NotRequired[int]
    action_idx: NotRequired[int]
    element_idx: NotRequired[int]
    element_set_idx: NotRequired[int]
    run_idx: NotRequired[int]
    sequence_idx: NotRequired[int]
    task_idx: NotRequired[int]
    value_class_method: NotRequired[str]


# EAR: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
E_idx_type: TypeAlias = "tuple[int, int]"
EI_idx_type: TypeAlias = "tuple[int, int, int]"
EAR_idx_type: TypeAlias = "tuple[int, int, int, int, int]"

DataIndex: TypeAlias = "dict[str, int | list[int]]"
"""
The type of indices to data. These are *normally* dictionaries of integers,
but can have leaves being lists of integers when dealing with iterations.
"""

_T = TypeVar("_T")

_CLASS_VAR_RE = re.compile(r"ClassVar\[(.*)\]")
_INIT_VAR_RE = re.compile(r"InitVar\[(.*)\]")


def hydrate(cls: type[_T]) -> type[_T]:
    """
    Partially hydrates the annotations on fields in a class, so that a @dataclass
    annotation can recognise that ClassVar-annotated fields are class variables.
    """
    anns = {}
    for f, a in cls.__annotations__.items():
        if isinstance(a, str):
            m = _CLASS_VAR_RE.match(a)
            if m:
                anns[f] = ClassVar[m.group(1)]
                continue
            m = _INIT_VAR_RE.match(a)
            if m:
                anns[f] = InitVar(cast(type, m.group(1)))
                continue
        anns[f] = a
    cls.__annotations__ = anns
    return cls
