"""
Job scheduler models.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
import sys
import time
from typing import Generic, TypeVar, TYPE_CHECKING
from typing_extensions import override
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.app_aware import AppAware

if TYPE_CHECKING:
    from typing import Any, ClassVar
    from ..shells import Shell
    from ..jobscript import Jobscript
    from ..enums import JobscriptElementState
    from ...config.types import SchedulerConfigDescriptor
    from ...core.element import ElementResources

#: The type of a jobscript reference.
JSRefType = TypeVar("JSRefType")


@hydrate
class Scheduler(ABC, Generic[JSRefType], AppAware):
    """
    Abstract base class for schedulers.

    Parameters
    ----------
    shell_args: str
        Arguments to pass to the shell. Pre-quoted.
    shebang_args: str
        Arguments to set on the shebang line. Pre-quoted.
    options: dict
        Options to the scheduler.
    """

    # This would be in the docstring except it renders really wrongly!
    # Type Parameters
    # ---------------
    # T
    #     The type of a jobscript reference.

    #: Default value for arguments to the shell.
    DEFAULT_SHELL_ARGS: ClassVar[str] = ""
    #: Default value for arguments on the shebang line.
    DEFAULT_SHEBANG_ARGS: ClassVar[str] = ""

    def __init__(
        self,
        shell_args: str | None = None,
        shebang_args: str | None = None,
        options: dict | None = None,
    ):
        self.shebang_args = shebang_args or self.DEFAULT_SHEBANG_ARGS
        self.shell_args = shell_args or self.DEFAULT_SHELL_ARGS
        self.options = options or {}

    @property
    def unique_properties(self) -> tuple[str, ...]:
        """
        Unique properties, for hashing.
        """
        return (self.__class__.__name__,)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.__dict__ == other.__dict__

    @abstractmethod
    def process_resources(
        self, resources: ElementResources, scheduler_config: SchedulerConfigDescriptor
    ) -> None:
        """
        Perform scheduler-specific processing to the element resources.

        Note
        ----
        This mutates `resources`.
        """

    def get_version_info(self) -> dict[str, str | list[str]]:
        """
        Get the version of the scheduler.
        """
        return {}

    def parse_submission_output(self, stdout: str) -> str | None:
        """
        Parse the output from a submission to determine the submission ID.
        """
        return None

    @staticmethod
    def is_num_cores_supported(num_cores: int | None, core_range: list[int]) -> bool:
        """
        Test whether particular number of cores is supported in given range of cores.
        """
        step = core_range[1] if core_range[1] is not None else 1
        upper = core_range[2] + 1 if core_range[2] is not None else sys.maxsize
        return num_cores in range(core_range[0], upper, step)

    @abstractmethod
    def get_submit_command(
        self,
        shell: Shell,
        js_path: str,
        deps: dict[Any, tuple[Any, ...]],
    ) -> list[str]:
        """
        Get a command for submitting a jobscript.
        """

    @abstractmethod
    def get_job_state_info(
        self, *, js_refs: list[JSRefType] | None = None, num_js_elements: int = 0
    ) -> Mapping[str, Mapping[int | None, JobscriptElementState]]:
        """
        Get the state of one or more jobscripts.
        """

    @abstractmethod
    def wait_for_jobscripts(self, js_refs: list[JSRefType]) -> None:
        """
        Wait for one or more jobscripts to complete.
        """

    @abstractmethod
    def cancel_jobs(
        self,
        js_refs: list[JSRefType],
        jobscripts: list[Jobscript] | None = None,
        num_js_elements: int = 0,  # Ignored!
    ) -> None:
        """
        Cancel one or more jobscripts.
        """


@hydrate
class QueuedScheduler(Scheduler[str]):
    """
    Base class for schedulers that use a job submission system.

    Parameters
    ----------
    submit_cmd: str
        The submission command, if overridden from default.
    show_cmd: str
        The show command, if overridden from default.
    del_cmd: str
        The delete command, if overridden from default.
    js_cmd: str
        The job script command, if overridden from default.
    login_nodes_cmd: str
        The login nodes command, if overridden from default.
    array_switch: str
        The switch to enable array jobs, if overridden from default.
    array_item_var: str
        The variable for array items, if overridden from default.
    """

    #: Default command for logging into nodes.
    DEFAULT_LOGIN_NODES_CMD: ClassVar[Sequence[str] | None] = None
    #: Default pattern for matching the names of login nodes.
    DEFAULT_LOGIN_NODE_MATCH: ClassVar[str] = "*login*"
    #: Default command for submitting a job.
    DEFAULT_SUBMIT_CMD: ClassVar[str]
    #: Default command for listing current submitted jobs.
    DEFAULT_SHOW_CMD: ClassVar[Sequence[str]]
    #: Default command for deleting a job.
    DEFAULT_DEL_CMD: ClassVar[str]
    #: Default marker for job control metadata in a job script.
    DEFAULT_JS_CMD: ClassVar[str]
    #: Default switch for enabling array mode.
    DEFAULT_ARRAY_SWITCH: ClassVar[str]
    #: Default shell variable containin the current array index.
    DEFAULT_ARRAY_ITEM_VAR: ClassVar[str]

    def __init__(
        self,
        submit_cmd: str | None = None,
        show_cmd: Sequence[str] | None = None,
        del_cmd=None,
        js_cmd=None,
        login_nodes_cmd=None,
        array_switch=None,
        array_item_var=None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.submit_cmd: str = submit_cmd or self.DEFAULT_SUBMIT_CMD
        self.show_cmd = show_cmd or self.DEFAULT_SHOW_CMD
        self.del_cmd = del_cmd or self.DEFAULT_DEL_CMD
        self.js_cmd = js_cmd or self.DEFAULT_JS_CMD
        self.login_nodes_cmd = login_nodes_cmd or self.DEFAULT_LOGIN_NODES_CMD
        self.array_switch = array_switch or self.DEFAULT_ARRAY_SWITCH
        self.array_item_var = array_item_var or self.DEFAULT_ARRAY_ITEM_VAR

    @property
    def unique_properties(self) -> tuple[str, str, Any, Any]:
        return (self.__class__.__name__, self.submit_cmd, self.show_cmd, self.del_cmd)

    def format_switch(self, switch: str) -> str:
        """
        Format a particular switch to use the JS command.
        """
        return f"{self.js_cmd} {switch}"

    def is_jobscript_active(self, job_ID: str) -> bool:
        """Query if a jobscript is running/pending."""
        return bool(self.get_job_state_info(js_refs=[job_ID]))

    @override
    def wait_for_jobscripts(self, js_refs: list[str]) -> None:
        """
        Wait for jobscripts to update their state.
        """
        while js_refs:
            info: Mapping[str, Any] = self.get_job_state_info(js_refs=js_refs)
            print(info)
            if not info:
                break
            js_refs = list(info)
            time.sleep(2)

    @abstractmethod
    def format_options(
        self, resources: ElementResources, num_elements: int, is_array: bool, sub_idx: int
    ) -> str:
        """
        Render options in a way that the scheduler can handle.
        """
