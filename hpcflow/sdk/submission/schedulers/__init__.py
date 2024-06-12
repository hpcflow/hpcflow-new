from __future__ import annotations
from collections.abc import Mapping, Sequence
import sys
import time
from typing import Any, ClassVar, Generic, TypeVar, override, TYPE_CHECKING
from abc import ABC, abstractmethod
if TYPE_CHECKING:
    from ..shells import Shell
    from ..jobscript_info import JobscriptElementState

T = TypeVar('T')


class NullScheduler(ABC, Generic[T]):
    DEFAULT_SHELL_ARGS: ClassVar[str] = ""
    DEFAULT_SHEBANG_ARGS: ClassVar[str] = ""

    def __init__(
        self,
        shell_args=None,
        shebang_args=None,
        options=None,
    ):
        self.shebang_args = shebang_args or self.DEFAULT_SHEBANG_ARGS
        self.shell_args = shell_args or self.DEFAULT_SHELL_ARGS
        self.options = options or {}

    @property
    def unique_properties(self) -> tuple[str, ...]:
        return (self.__class__.__name__,)

    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False
        else:
            return self.__dict__ == other.__dict__

    def get_version_info(self) -> dict[str, str]:
        return {}

    def parse_submission_output(self, stdout: str) -> str | None:
        return None

    @staticmethod
    def is_num_cores_supported(num_cores: int, core_range: list[int]) -> bool:
        step = core_range[1] if core_range[1] is not None else 1
        upper = core_range[2] + 1 if core_range[2] is not None else sys.maxsize
        return num_cores in range(core_range[0], upper, step)

    @abstractmethod
    def get_submit_command(
        self,
        shell: Shell,
        js_path: str,
        deps: dict[Any, tuple[Any, ...]],
    ) -> list[str]: ...

    @abstractmethod
    def get_job_state_info(
        self, js_refs: list[T] | None = None
    ) -> Mapping[str, Mapping[int | None, JobscriptElementState]]:
        ...

    @abstractmethod
    def wait_for_jobscripts(self, js_refs: list[T]) -> None: ...


class Scheduler(NullScheduler[str]):
    DEFAULT_LOGIN_NODES_CMD: ClassVar[Sequence[str] | None] = None
    DEFAULT_LOGIN_NODE_MATCH: ClassVar[str] = "*login*"
    DEFAULT_SUBMIT_CMD: ClassVar[str]
    DEFAULT_SHOW_CMD: ClassVar[Sequence[str]]
    DEFAULT_DEL_CMD: ClassVar[str]
    DEFAULT_JS_CMD: ClassVar[str]
    DEFAULT_ARRAY_SWITCH: ClassVar[str]
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

    def format_switch(self, switch) -> str:
        return f"{self.js_cmd} {switch}"

    def is_jobscript_active(self, job_ID: str) -> bool:
        """Query if a jobscript is running/pending."""
        return bool(self.get_job_state_info([job_ID]))

    @override
    def wait_for_jobscripts(self, js_refs: list[str]) -> None:
        while js_refs:
            info: dict[str, Any] = self.get_job_state_info(js_refs)
            print(info)
            if not info:
                break
            js_refs = list(info.keys())
            time.sleep(2)
