class NullScheduler:

    DEFAULT_SHELL_ARGS = ""

    def __init__(
        self,
        shell_executable=None,
        shell_args=None,
    ):
        self.shell_executable = shell_executable or self.DEFAULT_SHELL_EXECUTABLE
        self.shell_args = shell_args or self.DEFAULT_SHELL_ARGS

    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False
        else:
            return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        keys, vals = zip(*self.__dict__.items())
        return hash(tuple((keys, vals)))


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
