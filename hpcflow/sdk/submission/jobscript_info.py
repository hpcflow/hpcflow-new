"""
Jobscript state enumeration.
"""

import enum


class JobscriptElementState(enum.Enum):
    """Enumeration to convey a particular jobscript element state as reported by the
    scheduler."""

    def __new__(cls, value, symbol, colour, doc=None):
        member = object.__new__(cls)
        member._value_ = value
        member.symbol = symbol
        member.colour = colour
        member.__doc__ = doc
        return member

    #: Waiting for resource allocation.
    pending = (
        0,
        "○",
        "yellow",
        "Waiting for resource allocation.",
    )
    #: Waiting for one or more dependencies to finish.
    waiting = (
        1,
        "◊",
        "grey46",
        "Waiting for one or more dependencies to finish.",
    )
    #: Executing now.
    running = (
        2,
        "●",
        "dodger_blue1",
        "Executing now.",
    )
    #: Previously submitted but is no longer active.
    finished = (
        3,
        "■",
        "grey46",
        "Previously submitted but is no longer active.",
    )
    #: Cancelled by the user.
    cancelled = (
        4,
        "C",
        "red3",
        "Cancelled by the user.",
    )
    #: The scheduler reports an error state.
    errored = (
        5,
        "E",
        "red3",
        "The scheduler reports an error state.",
    )

    @property
    def rich_repr(self):
        """
        Rich representation of this enumeration element.
        """
        return f"[{self.colour}]{self.symbol}[/{self.colour}]"
