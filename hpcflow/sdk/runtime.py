from importlib import import_module
import logging
import os
import platform
import socket
import sys
from pathlib import Path
import warnings

import sentry_sdk
from rich.table import Table
from rich.console import Console


class RunTimeInfo:
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

    def __init__(self, name, package_name, version, logger):
        is_frozen = getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")
        bundle_dir = (
            sys._MEIPASS if is_frozen else os.path.dirname(os.path.abspath(__file__))
        )

        self.name = name.split(".")[0]  # if name is given as __name__ # TODO: what?
        self.package_name = package_name
        self.version = version
        self.is_frozen = is_frozen
        self.working_dir = os.getcwd()
        self.logger = logger
        self.hostname = socket.gethostname()

        self.in_ipython = False
        self.is_interactive = False
        self.in_pytest = False  # set in `conftest.py`
        self.from_CLI = False  # set in CLI

        if self.is_frozen:
            self.bundle_dir = Path(bundle_dir)
        else:
            self.python_executable_path = Path(sys.executable)

            try:
                get_ipython
                self.in_ipython = True
            except NameError:
                pass

            if hasattr(sys, "ps1"):
                self.is_interactive = True

        self.python_version = platform.python_version()
        self.is_venv = hasattr(sys, "real_prefix") or sys.base_prefix != sys.prefix
        self.is_conda_venv = "CONDA_PREFIX" in os.environ

        self.sys_prefix = getattr(sys, "prefix", None)
        self.sys_base_prefix = getattr(sys, "base_prefix", None)
        self.sys_real_prefix = getattr(sys, "real_prefix", None)
        self.conda_prefix = os.environ.get("CONDA_PREFIX")

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
        # TODO: investigate
        # if self.is_venv and self.is_conda_venv:
        #     msg = (
        #         "Running in a nested virtual environment (conda and non-conda). "
        #         "Environments may not be re-activate in the same order in associated, "
        #         "subsequent invocations of hpcflow."
        #     )
        #     warnings.warn(msg)

        for k, v in self.to_dict().items():
            if k in (
                "is_frozen",
                "is_venv",
                "is_conda_venv",
                "executable_name",
                "python_version",
                "in_ipython",
            ):
                sentry_sdk.set_tag(f"rti.{k}", v)

    def to_dict(self):
        out = {
            "name": self.name,
            "package_name": self.package_name,
            "version": self.version,
            "is_frozen": self.is_frozen,
            "working_dir": self.working_dir,
            "logger": self.logger,
            "hostname": self.hostname,
            "python_version": self.python_version,
            "invocation_command": self.invocation_command,
            "in_ipython": self.in_ipython,
            "in_pytest": self.in_pytest,
            "from_CLI": self.from_CLI,
        }
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
                    "is_interactive": self.is_interactive,
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
        return out

    def __repr__(self):
        out = f"{self.__class__.__name__}("
        out += ", ".join(f"{k}={v!r}" for k, v in self.to_dict().items())
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

    def show(self):
        tab = Table(show_header=False, box=None)
        tab.add_column()
        tab.add_column()
        for k, v in self.to_dict().items():
            tab.add_row(k, str(v))

        console = Console()
        console.print(tab)

    @property
    def executable_path(self):
        """Get the path that the user invoked to launch the frozen app, if the app is
        frozen.

        If the user launches the app via a symbolic link, then this returns that link,
        whereas `executable_path_resolved` returns the actual frozen app path.

        """
        if self.is_frozen:
            return Path(sys.argv[0])

    @property
    def resolved_executable_path(self):
        """Get the resolved path to the frozen app that the user launched, if the app is
        frozen.

        In a one-file app, this is the path to the bootloader. In the one-folder app, this
        is the path to the executable.

        References
        ----------
        [1] https://pyinstaller.org/en/stable/runtime-information.html#using-sys-executable-and-sys-argv-0

        """
        if self.is_frozen:
            return Path(sys.executable)

    @property
    def executable_name(self):
        """Get the name of the frozen app executable, if the app is frozen.

        If the user launches the app via a symbolic link, then this returns the name of
        that link, whereas `resolved_executable_name` returns the actual frozen app file
        name.

        """
        if self.is_frozen:
            return self.executable_path.name

    @property
    def resolved_executable_name(self):
        """Get the resolved name of the frozen app executable, if the app is frozen."""
        if self.is_frozen:
            return self.resolved_executable_path.name

    @property
    def script_path(self) -> Path:
        """Get the path to the Python script used to invoked this instance of the app, if
        the app is not frozen."""
        if not self.is_frozen:
            return Path(sys.argv[0])

    @property
    def resolved_script_path(self) -> Path:
        """Get the resolved path to the Python script used to invoked this instance of the
        app, if the app is not frozen."""
        if not self.is_frozen:
            return self.script_path.resolve()

    @property
    def invocation_command(self):
        """Get the command that was used to invoke this instance of the app."""
        if self.is_frozen:
            # (this also works if we are running tests using the frozen app)
            command = [str(self.resolved_executable_path)]
        elif self.from_CLI:
            script = str(self.resolved_script_path)
            if os.name == "nt" and script.endswith(".cmd"):
                # cannot reproduce locally, but on Windows GHA runners, if pytest is
                # invoked via `hpcflow test`, `resolved_script_path` seems to be the
                # batch script wrapper (ending in .cmd) rather than the Python entry point
                # itself, so trim if off:
                script = script.rstrip(".cmd")
            command = [str(self.python_executable_path), script]
        else:
            app_module = import_module(self.package_name)
            CLI_path = Path(*app_module.__path__, "cli.py")
            command = [str(self.python_executable_path), str(CLI_path)]

        return tuple(command)
