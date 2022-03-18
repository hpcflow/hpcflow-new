from imp import is_frozen
import os
from pathlib import Path
import sys


class RunTimeInfo:
    """Get the run-time information, including the executable name used to
    invoke the CLI, in the case a PyInstaller-built executable was used."""

    def __init__(self, name, debug=False):

        is_frozen = getattr(sys, "frozen", False)
        bundle_dir = (
            sys._MEIPASS if is_frozen else os.path.dirname(os.path.abspath(__file__))
        )

        self.name = name
        self.debug = debug
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

    def __repr__(self):
        if self.is_frozen:
            return (
                f"{self.__class__.__name__}(name={self.name!r}, debug={self.debug!r}, "
                f"is_frozen={self.is_frozen!r}, "
                f"executable_name={self.executable_name!r}, "
                f"resolved_executable_name={self.resolved_executable_name!r}, "
                f"executable_path={self.executable_path!r}, "
                f"resolved_executable_path={self.resolved_executable_path!r}, "
                f"working_dir={self.working_dir!r}"
                f")"
            )
        else:
            return (
                f"{self.__class__.__name__}(name={self.name!r}, debug={self.debug!r}, "
                f"is_frozen={self.is_frozen!r}, "
                f"script_path={self.script_path!r}, "
                f"python_executable_path={self.python_executable_path!r}, "
                f"working_dir={self.working_dir!r}"
                f")"
            )
