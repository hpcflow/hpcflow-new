"""
Adapters for various shells.
"""
from __future__ import annotations
import os

from hpcflow.sdk.core.errors import UnsupportedShellError

from .base import Shell
from .bash import Bash, WSLBash
from .powershell import WindowsPowerShell

#: All supported shells.
ALL_SHELLS: dict[str, dict[str, type[Shell]]] = {
    "bash": {"posix": Bash},
    "powershell": {"nt": WindowsPowerShell},
    "wsl+bash": {"nt": WSLBash},
    "wsl": {"nt": WSLBash},  # TODO: cast this to wsl+bash in ResourceSpec?
}

#: The default shell in the default config.
DEFAULT_SHELL_NAMES = {
    "posix": "bash",
    "nt": "powershell",
}


def get_supported_shells(os_name: str | None = None) -> dict[str, type[Shell]]:
    """
    Get shells supported on the current or given OS.
    """
    os_name_ = os_name or os.name
    return {k: v[os_name_] for k, v in ALL_SHELLS.items() if v.get(os_name_)}


def get_shell(shell_name, os_name: str | None = None, **kwargs) -> Shell:
    """
    Get a shell interface with the given name for a given OS (or the current one).
    """
    # TODO: apply config default shell args?

    os_name = os_name or os.name
    shell_name = shell_name.lower()

    supported = get_supported_shells(os_name.lower())
    shell_cls = supported.get(shell_name)
    if not shell_cls:
        raise UnsupportedShellError(shell=shell_name, supported=supported)

    shell_obj = shell_cls(**kwargs)

    return shell_obj
