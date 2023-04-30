from pathlib import Path
from typing import List, Tuple
from hpcflow.sdk.submission.shells.base import Shell


class NullScheduler:

    DEFAULT_SHELL_ARGS = ""
    DEFAULT_SHEBANG_ARGS = ""

    def __init__(
        self,
        submit_cmd=None,
        shell_args=None,
        shebang_args=None,
    ):
        self.shebang_args = shebang_args or self.DEFAULT_SHEBANG_ARGS
        self.shell_args = shell_args or self.DEFAULT_SHELL_ARGS

    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False
        else:
            return self.__dict__ == other.__dict__

    def get_version_info(self):
        return {}

    def parse_submission_output(self, stdout: str) -> None:
        return None

    def get_submit_command(
        self,
        shell: Shell,
        js_path: str,
        deps: List[Tuple],
    ) -> List[str]:
        return shell.executable + [js_path]


class Scheduler(NullScheduler):
    def __init__(
        self,
        submit_cmd=None,
        show_cmd=None,
        del_cmd=None,
        js_cmd=None,
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
        self.array_switch = array_switch or self.DEFAULT_ARRAY_SWITCH
        self.array_item_var = array_item_var or self.DEFAULT_ARRAY_ITEM_VAR

    def format_switch(self, switch):
        return f"{self.js_cmd} {switch}"
