from dataclasses import dataclass
from typing import List, Any

from hpcflow.sdk.core.json_like import JSONLike


@dataclass
class Command(JSONLike):

    command: str
    arguments: List[Any] = None
    stdout: str = None
    stderr: str = None
    stdin: str = None


@dataclass
class CommandArgument:
    """
    Attributes
    ----------
    parts : list of any of str, File, Parameter

    """

    parts: List[Any]
