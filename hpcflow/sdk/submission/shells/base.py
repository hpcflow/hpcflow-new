from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar, Dict


class Shell(ABC):
    """Class to represent a shell and templates for jobscript composition.

    This class represents a combination of a shell and an OS. For example, running
    bash on a POSIX OS, and provides snippets that are used to compose a jobscript for
    that combination.

    """

    JS_EXT: ClassVar[str]

    def __init__(self, executable=None, os_args=None):
        self._executable = executable or self.DEFAULT_EXE
        self.os_args = os_args

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self._executable == other._executable and self.os_args == other.os_args:
            return True
        return False

    @property
    def executable(self) -> list[str]:
        return [self._executable]

    @property
    def shebang_executable(self) -> list[str]:
        return self.executable

    def get_direct_submit_command(self, js_path) -> list[str]:
        """Get the command for submitting a non-scheduled jobscript."""
        return self.executable + [js_path]

    @abstractmethod
    def get_version_info(self, exclude_os: bool = False) -> Dict:
        """Get shell and operating system information."""

    def get_wait_command(self, workflow_app_alias: str, sub_idx: int, deps: Dict):
        if deps:
            return (
                f'{workflow_app_alias} workflow $WK_PATH_ARG wait --jobscripts "{sub_idx}:'
                + ",".join(str(i) for i in deps.keys())
                + '"'
            )
        else:
            return ""

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe):
        return app_invoc_exe

    def process_JS_header_args(self, header_args: Dict) -> Dict:
        app_invoc = self.process_app_invoc_executable(header_args["app_invoc"][0])
        if len(header_args["app_invoc"]) > 1:
            app_invoc += ' "' + header_args["app_invoc"][1] + '"'

        header_args["app_invoc"] = app_invoc
        return header_args

    def prepare_JS_path(self, js_path: Path) -> str:
        return str(js_path)

    def prepare_element_run_dirs(self, run_dirs: list[list[Path]]) -> list[list[str]]:
        return [[str(j) for j in i] for i in run_dirs]

    @abstractmethod
    def format_save_parameter(
        self,
        workflow_app_alias: str,
        param_name: str,
        shell_var_name: str,
        EAR_ID: int,
        cmd_idx: int,
        stderr: bool,
    ): ...

    @abstractmethod
    def wrap_in_subshell(self, commands: str, abortable: bool) -> str:
        """
        Format commands to run within a child scope.

        This assumes `commands` ends in a newline.
        """

    @abstractmethod
    def format_loop_check(self, workflow_app_alias: str, loop_name: str, run_ID: int) -> str: ...
