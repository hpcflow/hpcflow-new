from pathlib import Path
import sys
import time
from typing import Any, List, Tuple


class NullScheduler:
    DEFAULT_SHELL_ARGS = ""
    DEFAULT_SHEBANG_ARGS = ""

    def __init__(
        self,
        submit_cmd=None,
        shell_args=None,
        shebang_args=None,
        options=None,
    ):
        self.shebang_args = shebang_args or self.DEFAULT_SHEBANG_ARGS
        self.shell_args = shell_args or self.DEFAULT_SHELL_ARGS
        self.options = options or []

    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False
        else:
            return self.__dict__ == other.__dict__

    def get_version_info(self):
        return {}

    def parse_submission_output(self, stdout: str) -> None:
        return None

    @staticmethod
    def is_num_cores_supported(num_cores, core_range: List[int]):
        step = core_range[1] if core_range[1] is not None else 1
        upper = core_range[2] + 1 if core_range[2] is not None else sys.maxsize
        return num_cores in range(core_range[0], upper, step)


class Scheduler(NullScheduler):
    DEFAULT_LOGIN_NODES_CMD = None
    DEFAULT_LOGIN_NODE_MATCH = "*login*"

    def __init__(
        self,
        submit_cmd=None,
        show_cmd=None,
        del_cmd=None,
        js_cmd=None,
        login_nodes_cmd=None,
        array_switch=None,
        array_item_var=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.submit_cmd = submit_cmd or self.DEFAULT_SUBMIT_CMD
        self.show_cmd = show_cmd or self.DEFAULT_SHOW_CMD
        self.del_cmd = del_cmd or self.DEFAULT_DEL_CMD
        self.js_cmd = js_cmd or self.DEFAULT_JS_CMD
        self.login_nodes_cmd = login_nodes_cmd or self.DEFAULT_LOGIN_NODES_CMD
        self.array_switch = array_switch or self.DEFAULT_ARRAY_SWITCH
        self.array_item_var = array_item_var or self.DEFAULT_ARRAY_ITEM_VAR

    def format_switch(self, switch):
        return f"{self.js_cmd} {switch}"

    def is_jobscript_active(self, job_ID: str):
        """Query if a jobscript is running/pending."""
        return bool(self.get_job_state_info([job_ID]))

    def wait_for_jobscripts(self, js_refs: List[Any]) -> None:
        while js_refs:
            info = self.get_job_state_info(js_refs)
            print(info)
            if not info:
                break
            js_refs = list(info.keys())
            time.sleep(2)
