import logging
import os
import platform
import sys
from pathlib import Path
import warnings

import sentry_sdk

from hpcflow.sdk.core.utils import PrettyPrinter


class RunTimeInfo(PrettyPrinter):
    """Get useful run-time information, including the executable name used to
    invoke the CLI, in the case a PyInstaller-built executable was used.

    Attributes
    ----------
    sys_prefix : str
        From `sys.prefix`. If running in a virtual environment, this will point to the
        environment directory. If not running in a virtual environment, this will point to
        the Python installation root.
    sys_base_prefix : str
        From `sys.base_prefix`. This will be equal to `sys_prefix` (`sys.prefix`) if not
        running within a virtual environment. However, if running within a virtual
        environment, this will be the Python installation directory, and `sys_prefix` will
        be equal to the virtual environment directory.
    """

    def __init__(self, name, version, logger):

        is_frozen = getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")
        bundle_dir = (
            sys._MEIPASS if is_frozen else os.path.dirname(os.path.abspath(__file__))
        )

        self.name = name.split(".")[0]  # if name is given as __name__
        self.version = version
        self.is_frozen = is_frozen
        self.working_dir = os.getcwd()
        self.logger = logger

        path_exec = Path(sys.executable)
        path_argv = Path(sys.argv[0])

        if self.is_frozen:
            self.bundle_dir = Path(bundle_dir)
            self.executable_path = path_argv
            self.resolved_executable_path = path_exec
            self.executable_name = self.executable_path.name
            self.resolved_executable_name = self.resolved_executable_path.name
        else:
            self.script_path = path_argv
            self.resolved_script_path = path_argv.absolute()
            self.python_executable_path = path_exec

        self.python_version = platform.python_version()
        self.is_venv = hasattr(sys, "real_prefix") or sys.base_prefix != sys.prefix
        self.is_conda_venv = "CONDA_PREFIX" in os.environ

        self.sys_prefix = getattr(sys, "prefix", None)
        self.sys_base_prefix = getattr(sys, "base_prefix", None)
        self.sys_real_prefix = getattr(sys, "real_prefix", None)
        self.conda_prefix = os.environ.get("CONDA_PREFIX")

        self.invocation_command = self.get_invocation_command()

        try:
            self.venv_path = self._set_venv_path()
        except ValueError:
            self.venv_path = None

        self.logger.debug(
            f"is_frozen: {self.is_frozen!r}"
            f"{f' ({self.executable_name!r})' if self.is_frozen else ''}"
        )
        self.logger.debug(
            f"is_venv: {self.is_venv!r}"
            f"{f' ({self.sys_prefix!r})' if self.is_venv else ''}"
        )
        self.logger.debug(
            f"is_conda_venv: {self.is_conda_venv!r}"
            f"{f' ({self.conda_prefix!r})' if self.is_conda_venv else ''}"
        )
        if self.is_venv and self.is_conda_venv:
            msg = (
                "Running in a nested virtual environment (conda and non-conda). "
                "Environments may not be re-activate in the same order in associated, "
                "subsequent invocations of hpcflow."
            )
            warnings.warn(msg)

        for k, v in self._get_members().items():
            if k in (
                "is_frozen",
                "is_venv",
                "is_conda_venv",
                "executable_name",
                "python_version",
            ):
                sentry_sdk.set_tag(f"rti.{k}", v)

    def _get_members(self):
        out = {"is_frozen": self.is_frozen, "python_version": self.python_version}
        if self.is_frozen:
            out.update(
                {
                    "executable_name": self.executable_name,
                    "resolved_executable_name": self.resolved_executable_name,
                    "executable_path": self.executable_path,
                    "resolved_executable_path": self.resolved_executable_path,
                }
            )
        else:
            out.update(
                {
                    "script_path": self.script_path,
                    "resolved_script_path": self.resolved_script_path,
                    "python_executable_path": self.python_executable_path,
                    "is_venv": self.is_venv,
                    "is_conda_venv": self.is_conda_venv,
                    "sys_prefix": self.sys_prefix,
                    "sys_base_prefix": self.sys_base_prefix,
                    "sys_real_prefix": self.sys_real_prefix,
                    "conda_prefix": self.conda_prefix,
                    "venv_path": self.venv_path,
                }
            )
        out.update({"working_dir": self.working_dir})
        out.update({"invocation_command": self.get_invocation_command()})
        return out

    def __repr__(self):
        out = f"{self.__class__.__name__}("
        for k, v in self._get_members().items():
            out += f"{k}={v!r}"
        return out

    def _set_venv_path(self):
        out = []
        if self.is_venv:
            out.append(self.sys_prefix)
        elif self.is_conda_venv:
            out.append(self.conda_prefix)
        if not out:
            raise ValueError("Not running in a virtual environment!")
        if len(out) == 1:
            return out[0]
        else:
            return out

    def get_activate_env_command(self):
        pass

    def get_deactivate_env_command(self):
        pass

    def get_invocation_command(self):
        """Get the command that was used to invoke this instance of the app."""
        if self.is_frozen:
            return [str(self.resolved_executable_path)]
        else:
            return [str(self.python_executable_path), str(self.resolved_script_path)]
