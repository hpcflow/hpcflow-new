"""
Parallel modes.
"""

import enum


class ParallelMode(enum.Enum):
    """
    Potential parallel modes.
    """

    # TODO: Document what these really mean. This is totally a guess!

    #: Spread resources so work is not allocated together.
    DISTRIBUTED = 0
    #: Share resources so work is allocated together.
    SHARED = 1
    #: Spread and share resources; scheduler may choose.
    HYBRID = 2
