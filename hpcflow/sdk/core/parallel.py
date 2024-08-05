"""
Parallel modes.
"""

import enum


class ParallelMode(enum.Enum):
    """
    Potential parallel modes.
    """

    # TODO: Document what these really mean.
    DISTRIBUTED = 0
    SHARED = 1
    HYBRID = 2
