from dataclasses import dataclass
from typing import List, Any

from hpcflow.sdk.core.json_like import JSONLike


@dataclass
class Command(JSONLike):

    _app_attr = "app"

    command: str
    arguments: List[Any] = None
    stdout: str = None
    stderr: str = None
    stdin: str = None

    def __repr__(self) -> str:
        out = []
        if self.command:
            out.append(f"command={self.command!r}")
        if self.arguments:
            out.append(f"arguments={self.arguments!r}")
        if self.stdout:
            out.append(f"stdout={self.stdout!r}")
        if self.stderr:
            out.append(f"stderr={self.stderr!r}")
        if self.stdin:
            out.append(f"stdin={self.stdin!r}")

        return f"{self.__class__.__name__}({', '.join(out)})"


@dataclass
class CommandArgument:
    """
    Attributes
    ----------
    parts : list of any of str, File, Parameter

    """

    parts: List[Any]
