from dataclasses import dataclass
import re
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

    def get_output_types(self):
        # note: we use "parameter" rather than "output", because it could be a schema
        # output or schema input.
        vars_regex = r"\<\<parameter:(.*?)\>\>"
        out = {"stdout": None, "stderr": None}
        for i, label in zip((self.stdout, self.stderr), ("stdout", "stderr")):
            if i:
                match = re.search(vars_regex, i)
                if match:
                    param_typ = match.group(1)
                    if match.span(0) != (0, len(i)):
                        raise ValueError(
                            f"If specified as a parameter, `{label}` must not include"
                            f" any characters other than the parameter "
                            f"specification, but this was given: {i!r}."
                        )
                    out[label] = param_typ
        return out
