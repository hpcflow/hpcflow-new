from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class _JES:
    """
    Model of the state of a JobscriptElementState
    """
    _value: int
    symbol: str
    colour: str
    __doc__: str = ""


class JobscriptElementState(_JES, Enum):
    """Enumeration to convey a particular jobscript element state as reported by the
    scheduler."""

    pending = (
        0,
        "○",
        "yellow",
        "Waiting for resource allocation.",
    )
    waiting = (
        1,
        "◊",
        "grey46",
        "Waiting for one or more dependencies to finish.",
    )
    running = (
        2,
        "●",
        "dodger_blue1",
        "Executing now.",
    )
    finished = (
        3,
        "■",
        "grey46",
        "Previously submitted but is no longer active.",
    )
    cancelled = (
        4,
        "C",
        "red3",
        "Cancelled by the user.",
    )
    errored = (
        5,
        "E",
        "red3",
        "The scheduler reports an error state.",
    )

    @property
    def value(self) -> int:
        return self._value

    @property
    def rich_repr(self) -> str:
        return f"[{self.colour}]{self.symbol}[/{self.colour}]"
