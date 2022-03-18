from dataclasses import dataclass
from typing import List, Optional

from valida.conditions import ConditionLike

from hpcflow.typing_stubs import Parameter, ParameterPath, Task
from hpcflow.utils import check_valid_py_identifier


@dataclass
class Element:
    task: Task
    inputs: List[Parameter]
    outputs: List[Parameter]


@dataclass
class ElementFilter:

    parameter_path: ParameterPath
    condition: ConditionLike

    @classmethod
    def from_spec(cls, spec):
        raise NotImplementedError("ain't dun it yet")


@dataclass
class ElementGroup:

    name: str
    where: Optional[ElementFilter] = None
    group_by_distinct: Optional[ParameterPath] = None

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


@dataclass
class ElementRepeats:

    number: int
    where: Optional[ElementFilter] = None
