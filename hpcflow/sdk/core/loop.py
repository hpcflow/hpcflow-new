from dataclasses import dataclass
from .parameters import Parameter

from valida.conditions import ConditionLike


@dataclass
class StoppingCriterion:
    parameter: Parameter
    condition: ConditionLike


@dataclass
class Loop:
    parameter: Parameter
    stopping_criteria: StoppingCriterion  # TODO: should be a logical combination of these (maybe provide a superclass in valida to re-use some logic there?)
    maximum_iterations: int
