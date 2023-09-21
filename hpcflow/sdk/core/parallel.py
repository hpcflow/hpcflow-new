import enum


class ParallelMode(enum.Enum):
    DISTRIBUTED = 0
    SHARED = 1
    HYBRID = 2
