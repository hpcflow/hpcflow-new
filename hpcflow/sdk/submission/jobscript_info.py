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
        "The jobscript element is waiting for resource allocation.",
    )
    waiting = (
        1,
        "■",
        "grey46",
        "The jobscript element is waiting for one or more dependencies to finish.",
    )
    running = (
        2,
        "■",
        "dodger_blue1",
        "The jobscript element is running.",
    )
    finished = (
        3,
        "■",
        "grey46",
        "The jobscript element was previously submitted but is no longer active.",
    )
    cancelled = (
        4,
        "■",
        "red3",
        "The jobscript was cancelled by the user.",
    )
    errored = (
        5,
        "■",
        "red3",
        "The scheduler reports an error state for the jobscript element.",
    )
