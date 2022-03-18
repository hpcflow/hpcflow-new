from dataclasses import dataclass
from typing import List, Any


@dataclass
class Command:

    command: str
    arguments: List[Any] = None
    stdout: str = None
    stderr: str = None
    stdin: str = None

    @classmethod
    def from_spec(cls, spec):
        return cls(**spec)


@dataclass
class CommandArgument:
    """
    Attributes
    ----------
    parts : list of any of str, File, Parameter

    """

    parts: List[Any]
