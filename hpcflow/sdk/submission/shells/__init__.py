import os
from typing import Dict, Optional

from hpcflow.sdk.core.errors import UnsupportedShellError

from .base import Shell
from .bash import Bash, WSLBash
from .powershell import WindowsPowerShell

ALL_SHELLS = {
    "bash": {"posix": Bash},
    "powershell": {"nt": WindowsPowerShell},
    "wsl+bash": {"nt": WSLBash},
    "wsl": {"nt": WSLBash},  # TODO: cast this to wsl+bash in ResourceSpec?
}

# used to set the default shell in the default config:
DEFAULT_SHELL_NAMES = {
    "posix": "bash",
    "nt": "powershell",
}


def get_supported_shells(os_name: Optional[str] = None) -> Dict[str, Shell]:
    os_name = os_name or os.name
    return {k: v.get(os_name) for k, v in ALL_SHELLS.items() if v.get(os_name)}


def get_shell(shell_name, os_name: Optional[str] = None, **kwargs) -> Shell:
    # TODO: apply config default shell args?

    os_name = os_name or os.name
    shell_name = shell_name.lower()

    supported = get_supported_shells(os_name.lower())
    shell_cls = supported.get(shell_name)
    if not shell_cls:
        raise UnsupportedShellError(shell=shell_name, supported=supported)

    shell_obj = shell_cls(**kwargs)

    return shell_obj
