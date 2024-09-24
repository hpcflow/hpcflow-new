"""
Parallel modes.
"""

import enum


class ParallelMode(enum.Enum):
    """
    Potential parallel modes.

    Note: this is not yet implemented in any meaningful way!

    """

    #: Use distributed-memory parallelism (e.g. MPI).
    DISTRIBUTED = 0
    #: Use shared-memory parallelism (e.g. OpenMP).
    SHARED = 1
    #: Use both distributed- and shared-memory parallelism.
    HYBRID = 2
