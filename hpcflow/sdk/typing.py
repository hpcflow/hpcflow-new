"""
Common type aliases.
"""
from __future__ import annotations
from dataclasses import InitVar
from typing import ClassVar, TypedDict, TypeVar, cast
from typing_extensions import NotRequired, TypeAlias
from pathlib import Path
import re

#: Type of a value that can be treated as a path.
PathLike: TypeAlias = "str | Path | None"


class ParamSource(TypedDict):
    """
    A parameter source descriptor.
    """
    #: Parameter type name.
    type: NotRequired[str]
    #: EAR ID.
    EAR_ID: NotRequired[int]
    #: Task insertion ID.
    task_insert_ID: NotRequired[int]
    #: Action index.
    action_idx: NotRequired[int]
    #: Element index.
    element_idx: NotRequired[int]
    #: Element set index.
    element_set_idx: NotRequired[int]
    #: Element action run index.
    run_idx: NotRequired[int]
    #: Sequence index.
    sequence_idx: NotRequired[int]
    #: Task index.
    task_idx: NotRequired[int]
    #: Name of method used to create the parameter's value(s).
    value_class_method: NotRequired[str]


# EAR: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
#: Type of an element index:
#: (task_insert_ID, element_idx)
E_idx_type: TypeAlias = "tuple[int, int]"
#: Type of an element iteration index:
#: (task_insert_ID, element_idx, iteration_idx)
EI_idx_type: TypeAlias = "tuple[int, int, int]"
#: Type of an element action run index:
#: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
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
