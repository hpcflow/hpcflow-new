from pathlib import Path
import subprocess
from textwrap import dedent
from typing import Dict, List, Optional, Union
from hpcflow.sdk.submission.shells import Shell
from hpcflow.sdk.submission.shells.os_version import (
    get_OS_info_POSIX,
    get_OS_info_windows,
)


class Bash(Shell):
    """Class to represent using bash on a POSIX OS to generate and submit a jobscript."""

    DEFAULT_EXE = "/bin/bash"

    JS_EXT = ".sh"
    JS_INDENT = "  "
    JS_ENV_SETUP_INDENT = 2 * JS_INDENT
    JS_SHEBANG = """#!{shebang_executable} {shebang_args}"""
    JS_HEADER = dedent(
        """\
        {workflow_app_alias} () {{
        (
        {env_setup}{app_invoc}\\
            --config-dir "{config_dir}"\\
            --config-invocation-key "{config_invoc_key}"\\
            "$@"
        )
        }}

        WK_PATH=`pwd`
        WK_PATH_ARG=$WK_PATH
        SUB_IDX={sub_idx}
        JS_IDX={js_idx}
        EAR_ID_FILE="$WK_PATH/artifacts/submissions/${{SUB_IDX}}/{EAR_file_name}"
        ELEM_RUN_DIR_FILE="$WK_PATH/artifacts/submissions/${{SUB_IDX}}/{element_run_dirs_file_path}"
    """
    )
    JS_SCHEDULER_HEADER = dedent(
        """\
        {shebang}

        {scheduler_options}
        {header}
    """
    )
    JS_DIRECT_HEADER = dedent(
        """\
        {shebang}

        {header}
    """
    )
    JS_MAIN = dedent(
        """\
        elem_need_EARs=`sed "${{JS_elem_idx}}q;d" $EAR_ID_FILE`
        elem_run_dirs=`sed "${{JS_elem_idx}}q;d" $ELEM_RUN_DIR_FILE`

        for JS_act_idx in {{1..{num_actions}}}
        do
  
          need_EAR="$(cut -d'{EAR_files_delimiter}' -f $JS_act_idx <<< $elem_need_EARs)"
          if [ "$need_act" = "0" ]; then
              continue
          fi
  
          run_dir="$(cut -d'{EAR_files_delimiter}' -f $JS_act_idx <<< $elem_run_dirs)"
          cd $WK_PATH/$run_dir
  
          {workflow_app_alias} internal workflow $WK_PATH_ARG write-commands $SUB_IDX $JS_IDX $(($JS_elem_idx - 1)) $(($JS_act_idx - 1))
          {workflow_app_alias} internal workflow $WK_PATH_ARG set-ear-start $SUB_IDX $JS_IDX $(($JS_elem_idx - 1)) $(($JS_act_idx - 1))
          . {commands_file_name}
          {workflow_app_alias} internal workflow $WK_PATH_ARG set-ear-end $SUB_IDX $JS_IDX $(($JS_elem_idx - 1)) $(($JS_act_idx - 1))

        done
    """
    )
    JS_ELEMENT_LOOP = dedent(
        """\
        for JS_elem_idx in {{1..{num_elements}}}
        do
        {main}
        done
        cd $WK_PATH
    """
    )
    JS_ELEMENT_ARRAY = dedent(
        """\
        JS_elem_idx=${scheduler_array_item_var}
        {main}
        cd $WK_PATH
    """
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def linux_release_file(self):
        return self.os_args["linux_release_file"]

    def _get_OS_info_POSIX(self):
        return get_OS_info_POSIX(linux_release_file=self.linux_release_file)

    def get_version_info(self, exclude_os: Optional[bool] = False) -> Dict:
        """Get bash version information.

        Parameters
        ----------
        exclude_os
            If True, exclude operating system information.

        """

        bash_proc = subprocess.run(
            args=self.executable + ["--version"],
            stdout=subprocess.PIPE,
            text=True,
        )
        if bash_proc.returncode == 0:
            first_line = bash_proc.stdout.splitlines()[0]
            bash_version = first_line.split(" ")[3]
        else:
            raise RuntimeError("Failed to parse bash version information.")

        out = {
            "shell_name": "bash",
            "shell_executable": self.executable,
            "shell_version": bash_version,
        }

        if not exclude_os:
            out.update(**self._get_OS_info_POSIX())

        return out

    def format_stream_assignment(self, shell_var_name, command):
        return f"{shell_var_name}=`{command}`"

    def format_save_parameter(self, workflow_app_alias, param_name, shell_var_name):
        return (
            f"{workflow_app_alias}"
            f" internal workflow $WK_PATH_ARG save-parameter {param_name} ${shell_var_name}"
            f" $SUB_IDX $JS_IDX $(($JS_elem_idx - 1)) $(($JS_act_idx - 1))"
            f"\n"
        )


class WSLBash(Bash):

    DEFAULT_WSL_EXE = "wsl"

    JS_HEADER = Bash.JS_HEADER.replace(
        "WK_PATH_ARG=$WK_PATH",
        "WK_PATH_ARG=`wslpath -m $WK_PATH`",
    )

    def __init__(
        self,
        WSL_executable: Optional[str] = None,
        WSL_distribution: Optional[str] = None,
        WSL_user: Optional[str] = None,
        *args,
        **kwargs,
    ):
        self.WSL_executable = WSL_executable or self.DEFAULT_WSL_EXE
        self.WSL_distribution = WSL_distribution
        self.WSL_user = WSL_user
        super().__init__(*args, **kwargs)

    def __eq__(self, other) -> bool:
        return super().__eq__(other) and (
            self.WSL_executable == other.WSL_executable
            and self.WSL_distribution == other.WSL_distribution
            and self.WSL_user == other.WSL_user
        )

    def _get_WSL_command(self):
        out = [self.WSL_executable]
        if self.WSL_distribution:
            out += ["--distribution", self.WSL_distribution]
        if self.WSL_user:
            out += ["--user", self.WSL_user]
        return out

    @property
    def executable(self) -> List[str]:
        return self._get_WSL_command() + super().executable

    @property
    def shebang_executable(self) -> List[str]:
        return super().executable

    def _get_OS_info_POSIX(self):
        return get_OS_info_POSIX(
            WSL_executable=self._get_WSL_command(),
            use_py=False,
            linux_release_file=self.linux_release_file,
        )

    @staticmethod
    def _convert_to_wsl_path(win_path: Union[str, Path]) -> str:
        win_path = Path(win_path)
        parts = list(win_path.parts)
        parts[0] = f"/mnt/{win_path.drive.lower().rstrip(':')}"
        wsl_path = "/".join(parts)
        return wsl_path

    def process_JS_header_args(self, header_args):
        # convert executable windows paths to posix style as expected by WSL:
        header_args["app_invoc"][0] = self._convert_to_wsl_path(
            header_args["app_invoc"][0]
        )
        return super().process_JS_header_args(header_args)

    def prepare_JS_path(self, js_path: Path) -> str:
        return self._convert_to_wsl_path(js_path)

    def prepare_element_run_dirs(self, run_dirs: List[List[Path]]) -> List[List[str]]:
        return [["/".join(str(j).split("\\")) for j in i] for i in run_dirs]

    def get_version_info(self, exclude_os: Optional[bool] = False) -> Dict:
        """Get WSL and bash version information.

        Parameters
        ----------
        exclude_os
            If True, exclude operating system information.

        """
        vers_info = super().get_version_info(exclude_os=exclude_os)

        vers_info["shell_name"] = ("wsl+" + vers_info["shell_name"]).lower()
        vers_info["WSL_executable"] = self.WSL_executable
        vers_info["WSL_distribution"] = self.WSL_distribution
        vers_info["WSL_user"] = self.WSL_user

        for key in list(vers_info.keys()):
            if key.startswith("OS_"):
                vers_info[f"WSL_{key}"] = vers_info.pop(key)

        if not exclude_os:
            vers_info.update(**get_OS_info_windows())

        return vers_info
