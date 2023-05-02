import subprocess
from textwrap import dedent
from typing import Dict, Optional
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
    JS_HEADER = dedent(
        """\
        function {workflow_app_alias} {{
            & {{
        {env_setup}{app_invoc} `
                    --config-dir "{config_dir}" `
                    --config-invocation-key "{config_invoc_key}" `
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

        $WK_PATH = $(Get-Location)
        $SUB_IDX = {sub_idx}
        $JS_IDX = {js_idx}
        $EAR_ID_FILE = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX {EAR_file_name}
        $ELEM_RUN_DIR_FILE = JoinMultiPath $WK_PATH artifacts submissions $SUB_IDX {element_run_dirs_file_path}
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
        $elem_need_EARs = get_nth_line $EAR_ID_FILE $JS_elem_idx
        $elem_run_dirs = get_nth_line $ELEM_RUN_DIR_FILE $JS_elem_idx

        for ($JS_act_idx = 0; $JS_act_idx -lt {num_actions}; $JS_act_idx += 1) {{

            $need_EAR = ($elem_need_EARs -split "{EAR_files_delimiter}")[$JS_act_idx]
            if ($need_EAR -eq 0) {{
                continue
            }}

            $run_dir = ($elem_run_dirs -split "{EAR_files_delimiter}")[$JS_act_idx]
            $run_dir_abs = "$WK_PATH\$run_dir"
            Set-Location $run_dir_abs

            {workflow_app_alias} internal workflow $WK_PATH write-commands $SUB_IDX $JS_IDX $JS_elem_idx $JS_act_idx
            {workflow_app_alias} internal workflow $WK_PATH set-ear-start $SUB_IDX $JS_IDX $JS_elem_idx $JS_act_idx

            . (Join-Path $run_dir_abs commands.ps1)
            {workflow_app_alias} internal workflow $WK_PATH set-ear-end $SUB_IDX $JS_IDX $JS_elem_idx $JS_act_idx

        }}
    """
    )
    JS_ELEMENT_LOOP = dedent(
        """\
        for ($JS_elem_idx = 0; $JS_elem_idx -lt {num_elements}; $JS_elem_idx += 1) {{
        {main}
        }}
        Set-Location $WK_PATH
    """
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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

    def format_stream_assignment(self, shell_var_name, command):
        return f"${shell_var_name} = {command}"

    def format_save_parameter(self, workflow_app_alias, param_name, shell_var_name):
        return (
            f"{workflow_app_alias}"
            f" internal workflow $WK_PATH save-parameter {param_name} ${shell_var_name}"
            f" $SUB_IDX $JS_IDX $JS_elem_idx $JS_act_idx"
            f"\n"
        )
