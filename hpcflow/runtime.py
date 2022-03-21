import logging
import os
import sys
from pathlib import Path
import warnings

logger = logging.getLogger(__name__)


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

    def __init__(self, name):

        is_frozen = getattr(sys, "frozen", False)
        bundle_dir = (
            sys._MEIPASS if is_frozen else os.path.dirname(os.path.abspath(__file__))
        )

        self.name = name.split(".")[0]  # if name is given as __name__
        self.is_frozen = is_frozen
        self.working_dir = os.getcwd()

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
            self.python_executable_path = path_exec

        self.is_venv = hasattr(sys, "real_prefix") or sys.base_prefix != sys.prefix
        self.is_conda_venv = "CONDA_PREFIX" in os.environ

        self.sys_prefix = getattr(sys, "prefix", None)
        self.sys_base_prefix = getattr(sys, "base_prefix", None)
        self.sys_real_prefix = getattr(sys, "real_prefix", None)
        self.conda_prefix = os.environ.get("CONDA_PREFIX")

        logger.info(
            f"is_frozen: {self.is_frozen!r}"
            f"{f' ({self.executable_name!r})' if self.is_frozen else ''}"
        )
        logger.info(
            f"is_venv: {self.is_venv!r}"
            f"{f' ({self.sys_prefix!r})' if self.is_venv else ''}"
        )
        logger.info(
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

    def __repr__(self):
        out = (
            f"{self.__class__.__name__}("
            f"console_log_level={self.console_log_level!r}, "
            f"file_log_level={self.file_log_level!r}, "
            f"file_log_path={self.file_log_path!r}, "
            f"is_frozen={self.is_frozen!r}, "
        )
        if self.is_frozen:
            out += (
                f"executable_name={self.executable_name!r}, "
                f"resolved_executable_name={self.resolved_executable_name!r}, "
                f"executable_path={self.executable_path!r}, "
                f"resolved_executable_path={self.resolved_executable_path!r}, "
            )
        else:
            out += (
                f"script_path={self.script_path!r}, "
                f"python_executable_path={self.python_executable_path!r}, "
                f"is_venv={self.is_venv!r}, "
                f"is_conda_venv={self.is_conda_venv!r}, "
                f"sys_prefix={self.sys_prefix!r}, "
                f"sys_base_prefix={self.sys_base_prefix!r}, "
                f"sys_real_prefix={self.sys_real_prefix!r}, "
                f"conda_prefix={self.conda_prefix!r}, "
            )

        out += f"working_dir={self.working_dir!r})"
        return out

    def get_activate_env_command(self):
        pass

    def get_deactivate_env_command(self):
        pass
