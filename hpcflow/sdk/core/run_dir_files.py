import re
from typing import Tuple
from hpcflow.sdk.core.utils import JSONLikeDirSnapShot


class RunDirAppFiles:
    """A class to encapsulate the naming/recognition of app-created files within run
    directories."""

    _app_attr = "app"

    CMD_FILES_RE_PATTERN = r"js_\d+_act_\d+\.?\w*"

    @classmethod
    def get_log_file_name(cls) -> str:
        """File name for the app log file."""
        return f"{cls.app.package_name}.log"

    @classmethod
    def get_std_file_name(cls) -> str:
        """File name for stdout and stderr streams from the app."""
        return f"{cls.app.package_name}_std.txt"

    @staticmethod
    def get_run_file_prefix(block_act_key: Tuple[int, int, int]) -> str:
        return f"js_{block_act_key[0]}_block_{block_act_key[1]}_act_{block_act_key[2]}"

    @classmethod
    def get_commands_file_name(cls, block_act_key: Tuple[int, int, int], shell) -> str:
        return cls.get_run_file_prefix(block_act_key) + shell.JS_EXT

    @classmethod
    def get_run_param_dump_file_prefix(cls, block_act_key: Tuple[int, int, int]) -> str:
        """Get the prefix to a file in the run directory that the app will dump parameter
        data to."""
        return cls.get_run_file_prefix(block_act_key) + "_inputs"

    @classmethod
    def get_run_param_load_file_prefix(cls, block_act_key: Tuple[int, int, int]) -> str:
        """Get the prefix to a file in the run directory that the app will load parameter
        data from."""
        return cls.get_run_file_prefix(block_act_key) + "_outputs"

    @classmethod
    def take_snapshot(cls, root_path=None):
        """Take a JSONLikeDirSnapShot, and process to ignore files created by the app.

        This includes command files that are invoked by jobscripts, the app log file, and
        the app standard out/error file.

        """
        snapshot = JSONLikeDirSnapShot()
        snapshot.take(root_path or ".")
        ss_js = snapshot.to_json_like(use_strings=True)
        ss_js.pop("root_path")  # always the current working directory of the run
        for k in list(ss_js["data"].keys()):
            if (
                k == cls.get_log_file_name()
                or k == cls.get_std_file_name()
                or re.match(cls.CMD_FILES_RE_PATTERN, k)
            ):
                ss_js["data"].pop(k)

        return ss_js
