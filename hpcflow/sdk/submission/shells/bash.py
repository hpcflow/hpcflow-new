from pathlib import Path
import subprocess
from textwrap import dedent, indent
from typing import Dict, List, Optional, Union
from hpcflow.sdk.core import ABORT_EXIT_CODE
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
    JS_FUNCS = dedent(
        """\
        {workflow_app_alias} () {{
        (
        {env_setup}{app_invoc}\\
                --with-config log_file_path "`pwd`/{run_log_file}"\\
                --config-dir "{config_dir}"\\
                --config-key "{config_invoc_key}"\\
                "$@"
        )
        }}
    """
    )
    JS_HEADER = dedent(
        """\
        SCRIPT_DIR=$( cd -- "$( dirname -- "${{BASH_SOURCE[0]}}" )" &> /dev/null && pwd )
        JS_FUNCS_PATH="$SCRIPT_DIR/{jobscript_functions_path}"

        . "$JS_FUNCS_PATH"

        WK_PATH=`pwd`
        WK_PATH_ARG="$WK_PATH"
        SUB_IDX={sub_idx}
        JS_IDX={js_idx}
        APP_CAPS={app_caps}

        export {app_caps}_WK_PATH=$WK_PATH
        export {app_caps}_WK_PATH_ARG=$WK_PATH_ARG
        export {app_caps}_JS_FUNCS_PATH=$JS_FUNCS_PATH
        export {app_caps}_STD_STREAM_FILE="{run_stream_file}"
        export {app_caps}_SUB_IDX={sub_idx}
        export {app_caps}_JS_IDX={js_idx}
        
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
        {wait_command}
    """
    )
    JS_RUN_CMD = dedent(
        """\
        {workflow_app_alias} internal workflow "$WK_PATH_ARG" execute-run $SUB_IDX $JS_IDX $block_idx $block_act_idx $EAR_ID
    """
    )
    JS_RUN = dedent(
        """\
        EAR_ID="$(cut -d'{EAR_files_delimiter}' -f $(($block_act_idx + 1)) <<< $elem_EAR_IDs)"
        if [ "$EAR_ID" = "-1" ]; then
            continue
        fi

        export {app_caps}_RUN_ID=$EAR_ID
        export {app_caps}_BLOCK_ACT_IDX=$block_act_idx
        
        run_dir="$(cut -d'{EAR_files_delimiter}' -f $(($block_act_idx + 1)) <<< $elem_run_dirs)"
        cd "$WK_PATH/$run_dir"
        
        {run_cmd}
    """
    )
    JS_ACT_MULTI = dedent(
        """\
        for ((block_act_idx=0;block_act_idx<{num_actions};block_act_idx++))
        do      
        {run_block}
        done  
        """
    )
    JS_ACT_SINGLE = dedent(
        """\
        block_act_idx=0        
        {run_block}
        """
    )
    JS_MAIN = dedent(
        """\
        block_elem_idx=$(( $JS_elem_idx - {block_start_elem_idx} ))
        elem_EAR_IDs=`sed "$((${{JS_elem_idx}} + 1))q;d" "$EAR_ID_FILE"`
        elem_run_dirs=`sed "$((${{JS_elem_idx}} + 1))q;d" "$ELEM_RUN_DIR_FILE"`
        export {app_caps}_JS_ELEM_IDX=$JS_elem_idx
        export {app_caps}_BLOCK_ELEM_IDX=$block_elem_idx
        
        {action}
    """
    )
    JS_BLOCK_HEADER = dedent(  # for single-block jobscripts only
        """\
        block_idx=0
        export {app_caps}_BLOCK_IDX=0
        """
    )
    JS_ELEMENT_SINGLE = dedent(
        """\
        JS_elem_idx={block_start_elem_idx}
        {main}
    """
    )
    JS_ELEMENT_MULTI_LOOP = dedent(
        """\
        for ((JS_elem_idx={block_start_elem_idx};JS_elem_idx<$(({block_start_elem_idx} + {num_elements}));JS_elem_idx++))
        do
        {main}
        done
    """
    )
    JS_ELEMENT_MULTI_ARRAY = dedent(
        """\
        JS_elem_idx=$(({scheduler_array_item_var} - 1))
        {main}
    """
    )
    JS_BLOCK_LOOP = dedent(
        """\
        num_elements={num_elements}
        num_actions={num_actions}
        block_start_elem_idx=0
        for ((block_idx=0;block_idx<{num_blocks};block_idx++))
        do
            export {app_caps}_BLOCK_IDX=$block_idx
        {element_loop}
            block_start_elem_idx=$(($block_start_elem_idx + $num_elements[$block_idx]))
        done
    """
    )
    JS_FOOTER = dedent(
        """\
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

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe):
        # escape spaces with a back slash:
        app_invoc_exe = app_invoc_exe.replace(" ", r"\ ")
        return app_invoc_exe

    def format_array(self, lst: List) -> str:
        return "(" + " ".join(str(i) for i in lst) + ")"

    def format_array_get_item(self, arr_name, index) -> str:
        return f"${{{arr_name}[{index}]}}"

    def format_stream_assignment(self, shell_var_name, command):
        return f"{shell_var_name}=`{command}`"

    def format_source_functions_file(self, app_name):
        return dedent(
            """\
            . "${app_name}_JS_FUNCS_PATH"
            WK_PATH=${app_name}_WK_PATH
            WK_PATH_ARG=${app_name}_WK_PATH_ARG
            RUN_ID=${app_name}_RUN_ID
            SUB_IDX=${app_name}_SUB_IDX
            JS_IDX=${app_name}_JS_IDX
            JS_ELEM_IDX=${app_name}_JS_ELEM_IDX
            JS_ACT_IDX=${app_name}_JS_ACT_IDX
            STD_STREAM_FILE=${app_name}_STD_STREAM_FILE
            RUN_PORT_NUMBER=${app_name}_RUN_PORT_NUMBER
            BLOCK_ACT_IDX=${app_name}_BLOCK_ACT_IDX
            BLOCK_IDX=${app_name}_BLOCK_IDX

            """
        ).format(app_name=app_name.upper())

    def format_save_parameter(
        self,
        workflow_app_alias: str,
        param_name: str,
        shell_var_name: str,
        EAR_ID: int,
        cmd_idx: int,
        stderr: bool,
    ):
        # TODO: quote shell_var_name as well? e.g. if it's a white-space delimited list?
        #   and test.
        stderr_str = " --stderr" if stderr else ""
        return (
            f"{workflow_app_alias} "
            f'internal workflow "$WK_PATH_ARG" save-parameter '
            f"{param_name} ${shell_var_name} {EAR_ID} {cmd_idx}{stderr_str} "
            f'>> "$STD_STREAM_FILE" 2>&1'
            f"\n"
        )

    def format_loop_check(self, workflow_app_alias: str, loop_name: str, run_ID: int):
        return (
            f"{workflow_app_alias} "
            f'internal workflow "$WK_PATH_ARG" check-loop '
            f"{loop_name} {run_ID} "
            f'>> "$STD_STREAM_FILE" 2>&1'
            f"\n"
        )

    def wrap_in_subshell(self, commands: str, abortable: bool) -> str:
        """Format commands to run within a subshell.

        This assumes commands ends in a newline.

        """
        commands = indent(commands, self.JS_INDENT)
        if abortable:
            # run commands in the background, and poll a file to check for abort requests:
            return dedent(
                """\
                (
                {commands}) &

                pid=$!
                abort_file=$WK_PATH/artifacts/submissions/$SUB_IDX/abort_EARs.txt
                while true
                do
                    is_abort=`sed "$(($EAR_ID + 1))q;d" $abort_file`
                    ps -p $pid > /dev/null
                    if [ $? == 1 ]; then
                        wait $pid
                        exitcode=$?
                        break
                    elif [ "$is_abort" = "1" ]; then
                        echo "Abort instruction received; stopping commands..." >> "$app_stream_file"
                        kill $pid
                        wait $pid 2>/dev/null
                        exitcode={abort_exit_code}
                        break
                    else
                        sleep 1 # TODO: TEMP: increase for production
                    fi
                done
                return $exitcode
                """
            ).format(commands=commands, abort_exit_code=ABORT_EXIT_CODE)
        else:
            # run commands in "foreground":
            return dedent(
                """\
                (
                {commands})
            """
            ).format(commands=commands)


class WSLBash(Bash):
    DEFAULT_WSL_EXE = "wsl"

    JS_HEADER = Bash.JS_HEADER.replace(
        'WK_PATH_ARG="$WK_PATH"',
        'WK_PATH_ARG=`wslpath -m "$WK_PATH"`',
    )
    JS_FUNCS = Bash.JS_FUNCS.replace(
        '--with-config log_file_path "`pwd`',
        '--with-config log_file_path "$(wslpath -m `pwd`)',
    )
    JS_RUN_CMD = (
        dedent(
            """\
        WSLENV=$WSLENV:${{APP_CAPS}}_WK_PATH
        WSLENV=$WSLENV:${{APP_CAPS}}_WK_PATH_ARG
        WSLENV=$WSLENV:${{APP_CAPS}}_JS_FUNCS_PATH
        WSLENV=$WSLENV:${{APP_CAPS}}_STD_STREAM_FILE
        WSLENV=$WSLENV:${{APP_CAPS}}_SUB_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_JS_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_RUN_ID
        WSLENV=$WSLENV:${{APP_CAPS}}_BLOCK_ACT_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_JS_ELEM_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_BLOCK_ELEM_IDX
        WSLENV=$WSLENV:${{APP_CAPS}}_BLOCK_IDX

    """
        )
        + Bash.JS_RUN_CMD
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

    def get_command_file_launch_command(self, cmd_file_path: str) -> List[str]:
        """Get the command for launching the commands file for a given run."""
        return self.executable + [self._convert_to_wsl_path(cmd_file_path)]
