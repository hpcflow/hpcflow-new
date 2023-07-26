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

    pending = (
        0,
        "■",
        "yellow",
        "Waiting for resource allocation.",
    )
    waiting = (
        1,
        "■",
        "grey46",
        "Waiting for one or more dependencies to finish.",
    )
    running = (
        2,
        "■",
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
        "■",
        "red3",
        "Cancelled by the user.",
    )
    errored = (
        5,
        "■",
        "red3",
        "The scheduler reports an error state.",
    )

    @property
    def rich_repr(self):
        return f"[{self.colour}]{self.symbol}[/{self.colour}]"
