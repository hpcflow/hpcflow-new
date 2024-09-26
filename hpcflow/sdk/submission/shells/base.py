"""
Base model of a shell.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TypedDict, TYPE_CHECKING
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Any, ClassVar
    from typing_extensions import NotRequired

# This needs PEP 728 for a better type, alas
VersionInfo: TypeAlias = "dict[str, str | list[str]]"


class JobscriptHeaderArgs(TypedDict):
    app_invoc: str | Sequence[str]
    config_dir: NotRequired[str]
    config_invoc_key: NotRequired[Any]
    EAR_file_name: NotRequired[str]
    element_run_dirs_file_path: NotRequired[str]
    env_setup: NotRequired[str]
    js_idx: NotRequired[int]
    run_log_file: NotRequired[str]
    sub_idx: NotRequired[int]
    workflow_app_alias: NotRequired[str]
    workflow_path: NotRequired[str]


class Shell(ABC):
    """Class to represent a shell and templates for jobscript composition.

    This class represents a combination of a shell and an OS. For example, running
    bash on a POSIX OS, and provides snippets that are used to compose a jobscript for
    that combination.

    Parameters
    ----------
    executable: str
        Which executable implements the shell.
    os_args:
        Arguments to pass to the shell.
    """

    JS_EXT: ClassVar[str]
    DEFAULT_EXE: ClassVar[str]
    JS_ENV_SETUP_INDENT: ClassVar[str]
    JS_SHEBANG: ClassVar[str]
    JS_HEADER: ClassVar[str]
    JS_SCHEDULER_HEADER: ClassVar[str]
    JS_DIRECT_HEADER: ClassVar[str]
    JS_MAIN: ClassVar[str]
    JS_ELEMENT_ARRAY: ClassVar[str]
    JS_ELEMENT_LOOP: ClassVar[str]
    JS_INDENT: ClassVar[str]
    __slots__ = ("_executable", "os_args")

    def __init__(
        self, executable: str | None = None, os_args: dict[str, str] | None = None
    ):
        self._executable = executable or self.DEFAULT_EXE
        self.os_args = os_args or {}

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self._executable == other._executable and self.os_args == other.os_args

    @property
    def executable(self) -> list[str]:
        """
        The executable to use plus any mandatory arguments.
        """
        return [self._executable]

    @property
    def shebang_executable(self) -> list[str]:
        """
        The executable to use in a shebang line.
        """
        return self.executable

    def get_direct_submit_command(self, js_path) -> list[str]:
        """Get the command for submitting a non-scheduled jobscript."""
        return self.executable + [js_path]

    @abstractmethod
    def get_version_info(self, exclude_os: bool = False) -> VersionInfo:
        """Get shell and operating system information."""

    def get_wait_command(
        self, workflow_app_alias: str, sub_idx: int, deps: Mapping[int, Any]
    ):
        """
        Get the command to wait for a workflow.
        """
        if deps:
            return (
                f'{workflow_app_alias} workflow $WK_PATH_ARG wait --jobscripts "{sub_idx}:'
                + ",".join(str(i) for i in deps.keys())
                + '"'
            )
        else:
            return ""

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe: str) -> str:
        """
        Perform any post-processing of an application invocation command name.
        """
        return app_invoc_exe

    def process_JS_header_args(
        self, header_args: JobscriptHeaderArgs
    ) -> JobscriptHeaderArgs:
        """
        Process the application invocation key in the jobscript header arguments.
        """
        app_invoc_ = header_args["app_invoc"]
        if not isinstance(app_invoc_, str):
            app_invoc = self.process_app_invoc_executable(app_invoc_[0])
            for item in app_invoc_[1:]:
                app_invoc += f' "{item}"'
            header_args["app_invoc"] = app_invoc
        return header_args

    def prepare_JS_path(self, js_path: Path) -> str:
        """
        Prepare the jobscript path for use.
        """
        return str(js_path)

    def prepare_element_run_dirs(self, run_dirs: list[list[Path]]) -> list[list[str]]:
        """
        Prepare the element run directory names for use.
        """
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
    ):
        ...

    @abstractmethod
    def wrap_in_subshell(self, commands: str, abortable: bool) -> str:
        """
        Format commands to run within a child scope.

        This assumes `commands` ends in a newline.
        """

    @abstractmethod
    def format_loop_check(
        self, workflow_app_alias: str, loop_name: str, run_ID: int
    ) -> str:
        ...

    @abstractmethod
    def format_stream_assignment(self, shell_var_name, command) -> str:
        ...
