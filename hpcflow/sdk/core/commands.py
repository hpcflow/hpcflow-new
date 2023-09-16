from dataclasses import dataclass
from functools import partial
from pathlib import Path
import re
from typing import Dict, List, Any, Optional, Tuple
from hpcflow.sdk.core.errors import NoCLIFormatMethodError

from hpcflow.sdk.core.json_like import JSONLike
from hpcflow.sdk.core.parameters import ParameterValue


@dataclass
class Command(JSONLike):
    _app_attr = "app"

    command: Optional[str] = None
    executable: Optional[str] = None
    arguments: Optional[List[str]] = None
    variables: Optional[Dict[str, str]] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    stdin: Optional[str] = None

    def __repr__(self) -> str:
        out = []
        if self.command:
            out.append(f"command={self.command!r}")
        if self.executable:
            out.append(f"executable={self.executable!r}")
        if self.arguments:
            out.append(f"arguments={self.arguments!r}")
        if self.variables:
            out.append(f"variables={self.variables!r}")
        if self.stdout:
            out.append(f"stdout={self.stdout!r}")
        if self.stderr:
            out.append(f"stderr={self.stderr!r}")
        if self.stdin:
            out.append(f"stdin={self.stdin!r}")

        return f"{self.__class__.__name__}({', '.join(out)})"

    def get_command_line(self, EAR, shell, env) -> Tuple[str, List[Tuple[str, str]]]:
        """Return the resolved command line."""

        if self.command:
            cmd_str = self.command
        else:
            cmd_str = self.executable or ""

        def exec_script_repl(match_obj):
            typ, val = match_obj.groups()
            if typ == "executable":
                executable = env.executables.get(val)
                filterable = ("num_cores", "parallel_mode")
                filter_exec = {j: EAR.get_resources().get(j) for j in filterable}
                exec_cmd = executable.filter_instances(**filter_exec)[0].command
                out = exec_cmd.replace("<<num_cores>>", str(EAR.resources.num_cores))
            elif typ == "script":
                out = EAR.action.get_script_name(val)
            return out

        def input_param_repl(match_obj, inp_val):
            if isinstance(inp_val, ParameterValue):
                all_group, method, method_kwargs = match_obj.groups()
                if not method:
                    method = "CLI_format"
                if not hasattr(inp_val, method):
                    raise NoCLIFormatMethodError(
                        f"No CLI format method {method!r} exists for the "
                        f"object {inp_val!r}."
                    )
                kwargs = {}
                if method_kwargs:
                    args_split = [i.strip() for i in method_kwargs.split(",")]
                    for i in args_split:
                        arg_i, val_i = i.split("=")
                        kwargs[arg_i.strip()] = val_i.strip()

                inp_val = getattr(inp_val, method)(**kwargs)
            return str(inp_val)

        file_regex = r"(\<\<file:{}\>\>?)"
        exe_script_regex = r"\<\<(executable|script):(.*?)\>\>"

        # substitute executables:
        cmd_str = re.sub(
            pattern=exe_script_regex,
            repl=exec_script_repl,
            string=cmd_str,
        )

        # executable command might itself contain variables defined in `variables`, and/or
        # an `<<args>>` variable::
        for var_key, var_val in (self.variables or {}).items():
            cmd_str = cmd_str.replace(f"<<{var_key}>>", var_val)
            if "<<args>>" in cmd_str:
                args_str = " ".join(self.arguments or [])
                ends_in_args = cmd_str.endswith("<<args>>")
                cmd_str = cmd_str.replace("<<args>>", args_str)
                if ends_in_args and not args_str:
                    cmd_str = cmd_str.rstrip()

        # substitute input parameters in command:
        param_regex = r"(\<\<parameter:{}(?:\.(\w+)\((.*)\))?\>\>?)"
        for cmd_inp_full in EAR.action.get_command_input_types(sub_parameters=True):
            # remove any CLI formatting method, which will be the final component and will
            # include parentheses:
            cmd_inp_parts = cmd_inp_full.split(".")
            if "(" in cmd_inp_parts[-1]:
                cmd_inp = ".".join(cmd_inp_parts[:-1])
            else:
                cmd_inp = cmd_inp_full
            inp_val = EAR.get(f"inputs.{cmd_inp}")  # TODO: what if schema output?
            pattern_i = param_regex.format(re.escape(cmd_inp))
            cmd_str = re.sub(
                pattern=pattern_i,
                repl=partial(input_param_repl, inp_val=inp_val),
                string=cmd_str,
            )

        # substitute input files in command:
        for cmd_file in EAR.action.get_command_input_file_labels():
            file_path = EAR.get(f"input_files.{cmd_file}")  # TODO: what if out file?
            # assuming we have copied this file to the EAR directory, then we just
            # need the file name:
            file_name = Path(file_path).name
            cmd_str = re.sub(
                pattern=file_regex.format(cmd_file),
                repl=file_name,
                string=cmd_str,
            )

        shell_vars = []
        out_types = self.get_output_types()
        if out_types["stdout"]:
            # TODO: also map stderr/both if possible
            # assign stdout to a shell variable if required:
            param_name = f"outputs.{out_types['stdout']}"
            shell_var_name = f"parameter_{out_types['stdout']}"
            shell_vars.append((param_name, shell_var_name))
            cmd_str = shell.format_stream_assignment(
                shell_var_name=shell_var_name,
                command=cmd_str,
            )
        elif self.stdout:
            cmd_str += f" 1>> {self.stdout}"

        if self.stderr:
            cmd_str += f" 2>> {self.stderr}"

        return cmd_str, shell_vars

    def get_output_types(self):
        # note: we use "parameter" rather than "output", because it could be a schema
        # output or schema input.
        vars_regex = r"\<\<parameter:(.*?)\>\>"
        out = {"stdout": None, "stderr": None}
        for i, label in zip((self.stdout, self.stderr), ("stdout", "stderr")):
            if i:
                match = re.search(vars_regex, i)
                if match:
                    param_typ = match.group(1)
                    if match.span(0) != (0, len(i)):
                        raise ValueError(
                            f"If specified as a parameter, `{label}` must not include"
                            f" any characters other than the parameter "
                            f"specification, but this was given: {i!r}."
                        )
                    out[label] = param_typ
        return out
