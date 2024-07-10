from dataclasses import InitVar
from typing import ClassVar, Union, TypeAlias, TypeVar, cast
from pathlib import Path
import re

PathLike: TypeAlias = Union[str, Path, None]

# EAR: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
E_idx_type: TypeAlias = tuple[int, int]
EI_idx_type: TypeAlias = tuple[int, int, int]
EAR_idx_type: TypeAlias = tuple[int, int, int, int, int]

ParamSource: TypeAlias = dict[str, str | int]
DataIndex: TypeAlias = dict[str, int | list[int]]

_T = TypeVar('_T')

def hydrate(cls: type[_T]) -> type[_T]:
    """
    Partially hydrates the annotations on fields in a class, so that a @dataclass
    annotation can recognise that ClassVar-annotated fields are class variables.
    """
    anns = {}
    cvre = re.compile(r"ClassVar\[(.*)\]")
    ivre = re.compile(r"InitVar\[(.*)\]")
    for f, a in cls.__annotations__.items():
        if isinstance(a, str):
            m = cvre.match(a)
            if m:
                anns[f] = ClassVar[m.group(1)]
                continue
            m = ivre.match(a)
            if m:
                anns[f] = InitVar(cast(type, m.group(1)))
                continue
        anns[f] = a
    cls.__annotations__ = anns
    return cls
