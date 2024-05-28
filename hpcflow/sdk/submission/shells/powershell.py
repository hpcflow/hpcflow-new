import subprocess
from textwrap import dedent, indent
from typing import Dict, List, Optional
from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.submission.shells import Shell
from hpcflow.sdk.submission.shells.os_version import get_OS_info_windows


class WindowsPowerShell(Shell):
    """Class to represent using PowerShell on Windows to generate and submit a jobscript."""

    # TODO: add snippets that can be used in demo task schemas?

    DEFAULT_EXE = "powershell.exe"

    JS_EXT = ".ps1"
    JS_INDENT = "    "
    JS_ENV_SETUP_INDENT = 2 * JS_INDENT
    JS_SHEBANG = ""
    JS_FUNCS = dedent(
        """\
        function {workflow_app_alias} {{
            & {{
        {env_setup}{app_invoc} `
                    --with-config log_file_path "$pwd/{run_log_file}" `
                    --config-dir "{config_dir}" `
                    --config-key "{config_invoc_key}" `
                    $args
            }} @args
        }}

        function get_nth_line($file, $line) {{
            Get-Content $file | Select-Object -Skip $line -First 1
        }}

        function JoinMultiPath {{
            $numArgs = $args.Length
            $path = $args[0]
            for ($i = 1; $i -lt $numArgs; $i++) {{
                $path = Join-Path $path $args[$i]
            }}
            return $path
        }}

        function StartJobHere($block) {{
            $jobInitBlock = [scriptblock]::Create(@"
                Function wkflow_app {{ $function:wkflow_app }}
                Function get_nth_line {{ $function:get_nth_line }}
                Function JoinMultiPath {{ $function:JoinMultiPath }}
                Set-Location '$pwd'
        "@)
            Start-Job -InitializationScript $jobInitBlock -Script $block
        }}

    """
    )
    JS_HEADER = dedent(
        """\
        $ErrorActionPreference = 'Stop'
        $JS_FUNCS_PATH = (Join-Path -Path $PSScriptRoot -ChildPath {jobscript_functions_path})
        
        . $JS_FUNCS_PATH

        $WK_PATH = $(Get-Location)
        $WK_PATH_ARG = $WK_PATH
        $SUB_IDX = {sub_idx}
        $JS_IDX = {js_idx}

        $env:{app_caps}_WK_PATH = $WK_PATH
        $env:{app_caps}_WK_PATH_ARG = $WK_PATH_ARG
        $env:{app_caps}_JS_FUNCS_PATH = $JS_FUNCS_PATH
        $env:{app_caps}_STD_STREAM_FILE = "{run_stream_file}"
        $env:{app_caps}_SUB_IDX = {sub_idx}
        $env:{app_caps}_JS_IDX = {js_idx}
        
        $EAR_ID_FILE = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX {EAR_file_name}
        $ELEM_RUN_DIR_FILE = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX {element_run_dirs_file_path}
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
        {workflow_app_alias} internal workflow $WK_PATH execute-run $SUB_IDX $JS_IDX $block_idx $block_act_idx $EAR_ID
    """
    )
    JS_RUN = dedent(
        """\
        $EAR_ID = ($elem_EAR_IDs -split "{EAR_files_delimiter}")[$block_act_idx]
        if ($EAR_ID -eq -1) {{
            continue
        }}

        $env:{app_caps}_RUN_ID = $EAR_ID
        $env:{app_caps}_BLOCK_ACT_IDX = $block_act_idx            

        $run_dir = ($elem_run_dirs -split "{EAR_files_delimiter}")[$block_act_idx]
        $run_dir_abs = Join-Path "$WK_PATH" "$run_dir"
        Set-Location $run_dir_abs
        
        {run_cmd}
        """
    )
    JS_ACT_MULTI = dedent(
        """\
        for ($block_act_idx = 0; $block_act_idx -lt {num_actions}; $block_act_idx += 1) {{        
        {run_block}
        }}
        """
    )
    JS_ACT_SINGLE = dedent(
        """\
        $block_act_idx = 0        
        {run_block}
        """
    )
    JS_MAIN = dedent(
        """\
        $block_elem_idx = ($JS_elem_idx - {block_start_elem_idx})
        $elem_EAR_IDs = get_nth_line $EAR_ID_FILE $JS_elem_idx
        $elem_run_dirs = get_nth_line $ELEM_RUN_DIR_FILE $JS_elem_idx
        $env:{app_caps}_JS_ELEM_IDX = $JS_elem_idx
        $env:{app_caps}_BLOCK_ELEM_IDX = $block_elem_idx

        {action}
    """
    )
    JS_BLOCK_HEADER = dedent(  # for single-block jobscripts only
        """\
        $block_idx = 0
        $env:{app_caps}_BLOCK_IDX = 0
        """
    )
    JS_ELEMENT_SINGLE = dedent(
        """\
        $JS_elem_idx = {block_start_elem_idx}
        {main}
    """
    )
    JS_ELEMENT_MULTI_LOOP = dedent(
        """\
        for ($JS_elem_idx = {block_start_elem_idx}; $JS_elem_idx -lt ({block_start_elem_idx} + {num_elements}); $JS_elem_idx += 1) {{            
        {main}
        }}
    """
    )
    JS_ELEMENT_MULTI_ARRAY = None  # not implemented # TODO: add to Shell class
    JS_BLOCK_LOOP = dedent(
        """\
        $num_elements = {num_elements}
        $num_actions = {num_actions}
        $block_start_elem_idx = 0
        for ($block_idx = 0; $block_idx -lt {num_blocks}; $block_idx += 1 ) {{
            $env:{app_caps}_BLOCK_IDX = $block_idx
        {element_loop}
            $block_start_elem_idx += $num_elements[$block_idx]
        }}
    """
    )
    JS_FOOTER = dedent(
        """\
        Set-Location $WK_PATH
    """
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_direct_submit_command(self, js_path: str) -> List[str]:
        """Get the command for submitting a non-scheduled jobscript."""
        return self.executable + ["-File", js_path]

    def get_command_file_launch_command(self, cmd_file_path: str) -> List[str]:
        """Get the command for launching the commands file for a given run."""
        # note the "-File" argument is required for the correct exit code to be recorded.
        return self.executable + ["-File", cmd_file_path]

    def get_version_info(self, exclude_os: Optional[bool] = False) -> Dict:
        """Get powershell version information.

        Parameters
        ----------
        exclude_os
            If True, exclude operating system information.

        """

        proc = subprocess.run(
            args=self.executable + ["$PSVersionTable.PSVersion.ToString()"],
            stdout=subprocess.PIPE,
            text=True,
        )
        if proc.returncode == 0:
            PS_version = proc.stdout.strip()
        else:
            raise RuntimeError("Failed to parse PowerShell version information.")

        out = {
            "shell_name": "powershell",
            "shell_executable": self.executable,
            "shell_version": PS_version,
        }

        if not exclude_os:
            out.update(**get_OS_info_windows())

        return out

    @staticmethod
    def process_app_invoc_executable(app_invoc_exe):
        if " " in app_invoc_exe:
            # use call operator and single-quote the executable path:
            app_invoc_exe = f"& '{app_invoc_exe}'"
        return app_invoc_exe

    def format_array(self, lst: List) -> str:
        return "@(" + ", ".join(str(i) for i in lst) + ")"

    def format_array_get_item(self, arr_name, index) -> str:
        return f"${arr_name}[{index}]"

    def format_stream_assignment(self, shell_var_name, command):
        return f"${shell_var_name} = {command}"

    def format_source_functions_file(self, app_name):
        return dedent(
            """\
            . $env:{app_name}_JS_FUNCS_PATH
            $WK_PATH = $env:{app_name}_WK_PATH
            $WK_PATH_ARG = $env:{app_name}_WK_PATH_ARG
            $RUN_ID = $env:{app_name}_RUN_ID
            $SUB_IDX = $env:{app_name}_SUB_IDX
            $JS_IDX = $env:{app_name}_JS_IDX
            $JS_ELEM_IDX = $env:{app_name}_JS_ELEM_IDX
            $JS_ACT_IDX = $env:{app_name}_JS_ACT_IDX
            $STD_STREAM_FILE = $env:{app_name}_STD_STREAM_FILE
            $RUN_PORT_NUMBER = $env:{app_name}_RUN_PORT_NUMBER

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
            f"internal workflow $WK_PATH save-parameter "
            f"{param_name} ${shell_var_name} {EAR_ID} {cmd_idx}{stderr_str} "
            f"2>&1 >> $STD_STREAM_FILE"
            f"\n"
        )

    def format_loop_check(self, workflow_app_alias: str, loop_name: str, run_ID: int):
        return (
            f"{workflow_app_alias} "
            f"internal workflow $WK_PATH check-loop "
            f"{loop_name} {run_ID} "
            f"2>&1 >> $STD_STREAM_FILE"
            f"\n"
        )

    def wrap_in_subshell(self, commands: str, abortable: bool) -> str:
        """Format commands to run within a child scope.

        This assumes `commands` ends in a newline.

        """
        commands = indent(commands, self.JS_INDENT)
        if abortable:
            # TODO: won't work anymore!
            # !!!!!!!!!!!!!!!!!!!!!!!!!!
            # run commands as a background job, and poll a file to check for abort
            # requests:
            return dedent(
                """\
                $job = StartJobHere {{
                    $WK_PATH = $using:WK_PATH
                    $SUB_IDX = $using:SUB_IDX
                    $JS_IDX = $using:JS_IDX
                    $EAR_ID = $using:EAR_ID
                    $app_stream_file= $using:app_stream_file

                {commands}
                    if ($LASTEXITCODE -ne 0) {{
                        throw
                    }}
                }}

                $is_abort = $null
                $abort_file = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX abort_EARs.txt
                while ($true) {{
                    $is_abort = get_nth_line $abort_file $EAR_ID
                    if ($job.State -ne "Running") {{
                        break
                    }}
                    elseif ($is_abort -eq "1") {{
                        Add-Content -Path $app_stream_file -Value "Abort instruction received; stopping commands..."
                        Stop-Job -Job $job
                        Wait-Job -Job $job
                        break
                    }}
                    else {{
                        Receive-Job -job $job | Write-Output
                        Start-Sleep 1 # TODO: TEMP: increase for production
                    }}
                }}
                Receive-Job -job $job | Write-Output
                if ($job.state -eq "Completed") {{
                    exit 0
                }}
                elseif ($is_abort -eq "1") {{
                    exit {abort_exit_code}
                }}
                else {{
                    exit 1
                }}
            """
            ).format(commands=commands, abort_exit_code=ABORT_EXIT_CODE)
        else:
            # run commands in "foreground":
            return dedent(
                """\
                & {{
                {commands}}}
            """
            ).format(commands=commands)
