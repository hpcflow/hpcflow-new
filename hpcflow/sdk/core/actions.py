from __future__ import annotations
import copy
from dataclasses import dataclass
from datetime import datetime
import enum
from pathlib import Path
import re
import subprocess
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple, Union

from valida.rules import Rule
from watchdog.utils.dirsnapshot import DirectorySnapshotDiff

from hpcflow.sdk import app
from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.core.errors import MissingCompatibleActionEnvironment
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.utils import JSONLikeDirSnapShot


ACTION_SCOPE_REGEX = r"(\w*)(?:\[(.*)\])?"


class ActionScopeType(enum.Enum):
    ANY = 0
    MAIN = 1
    PROCESSING = 2
    INPUT_FILE_GENERATOR = 3
    OUTPUT_FILE_PARSER = 4


ACTION_SCOPE_ALLOWED_KWARGS = {
    ActionScopeType.ANY.name: set(),
    ActionScopeType.MAIN.name: set(),
    ActionScopeType.PROCESSING.name: set(),
    ActionScopeType.INPUT_FILE_GENERATOR.name: {"file"},
    ActionScopeType.OUTPUT_FILE_PARSER.name: {"output"},
}


class EARStatus(str, enum.Enum):
    """Enumeration of all possible EAR statuses, and their associated status colour."""

    PENDING = "grey46"  # Not yet associated with a submission
    PREPARED = "grey46"  # Associated with a prepared submission that is not yet submitted
    SUBMITTED = "grey46"  # Submitted for execution
    RUNNING = "dodger_blue1"  # Executing now
    SKIPPED = "dark_orange"  # Not attempted due to a failure of an upstream EAR on which this depends
    ABORTED = "deep_pink4"  # User-initiated abort
    SUCCESS = "green3"  # Probably exited successfully
    ERROR = "red3"  # Probably failed

    @classmethod
    def get_non_running_submitted_states(cls):
        """Return the set of all non-running states, excluding those before submission."""
        return {
            cls.SKIPPED,
            cls.ABORTED,
            cls.SUCCESS,
            cls.ERROR,
        }


class ElementActionRun:
    _app_attr = "app"

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        element_action,
        index: int,
        data_idx: Dict,
        start_time: Union[datetime, None],
        end_time: Union[datetime, None],
        snapshot_start: Union[Dict, None],
        snapshot_end: Union[Dict, None],
        submission_idx: Union[int, None],
        success: Union[bool, None],
        skip: bool,
        exit_code: Union[int, None],
        metadata: Dict,
    ) -> None:
        self._id = id_
        self._is_pending = is_pending
        self._element_action = element_action
        self._index = index  # local index of this run with the action
        self._data_idx = data_idx
        self._start_time = start_time
        self._end_time = end_time
        self._submission_idx = submission_idx
        self._success = success
        self._skip = skip
        self._snapshot_start = snapshot_start
        self._snapshot_end = snapshot_end
        self._exit_code = exit_code
        self._metadata = metadata

        # assigned on first access of corresponding properties:
        self._inputs = None
        self._outputs = None
        self._resources = None
        self._input_files = None
        self._output_files = None
        self._ss_start_obj = None
        self._ss_end_obj = None
        self._ss_diff_obj = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"id={self.id_!r}, index={self.index!r}, "
            f"element_action={self.element_action!r})"
        )

    @property
    def id_(self) -> int:
        return self._id

    @property
    def is_pending(self) -> bool:
        return self._is_pending

    @property
    def element_action(self):
        return self._element_action

    @property
    def index(self):
        """Run index."""
        return self._index

    @property
    def action(self):
        return self.element_action.action

    @property
    def element_iteration(self):
        return self.element_action.element_iteration

    @property
    def element(self):
        return self.element_iteration.element

    @property
    def workflow(self):
        return self.element_iteration.workflow

    # @property
    # def EAR_ID(self):
    #     """EAR index object."""
    #     return EAR_ID(
    #         EAR_idx=self.index,
    #         task_insert_ID=self.task.insert_ID,
    #         element_idx=self.element.index,
    #         iteration_idx=self.element_iteration.index,
    #         action_idx=self.element_action.action_idx,
    #         run_idx=self.run_idx,
    #     )

    @property
    def data_idx(self):
        return self._data_idx

    @property
    def metadata(self):
        return self._metadata

    @property
    def start_time(self):
        return self._start_time

    @property
    def end_time(self):
        return self._end_time

    @property
    def submission_idx(self):
        return self._submission_idx

    @property
    def success(self):
        return self._success

    @property
    def skip(self):
        return self._skip

    @property
    def snapshot_start(self):
        if self._ss_start_obj is None and self._snapshot_start:
            self._ss_start_obj = JSONLikeDirSnapShot(**self._snapshot_start)
        return self._ss_start_obj

    @property
    def snapshot_end(self):
        if self._ss_end_obj is None and self._snapshot_end:
            self._ss_end_obj = JSONLikeDirSnapShot(**self._snapshot_end)
        return self._ss_end_obj

    @property
    def dir_diff(self) -> DirectorySnapshotDiff:
        """Get the changes to the EAR working directory due to the execution of this
        EAR."""
        if self._ss_diff_obj is None and self.snapshot_end:
            self._ss_diff_obj = DirectorySnapshotDiff(
                self.snapshot_start, self.snapshot_end
            )
        return self._ss_diff_obj

    @property
    def exit_code(self):
        return self._exit_code

    @property
    def task(self):
        return self.element_action.task

    @property
    def status(self):
        """Return the state of this EAR."""

        if self.skip:
            return EARStatus.SKIPPED

        elif self.end_time is not None:
            if self.exit_code == 0:
                return EARStatus.SUCCESS
            elif self.action.abortable and self.exit_code == ABORT_EXIT_CODE:
                return EARStatus.ABORTED
            else:
                return EARStatus.ERROR

        elif self.start_time is not None:
            return EARStatus.RUNNING

        elif self.submission_idx is not None:
            wk_sub_stat = self.workflow.submissions[self.submission_idx].status

            if wk_sub_stat.name == "PENDING":
                return EARStatus.PREPARED

            elif wk_sub_stat.name == "SUBMITTED":
                return EARStatus.SUBMITTED

            else:
                RuntimeError(f"Workflow submission status not understood: {wk_sub_stat}.")

        return EARStatus.PENDING

    def get_parameter_names(self, prefix):
        return self.element_action.get_parameter_names(prefix)

    def get_data_idx(self, path: str = None):
        return self.element_iteration.get_data_idx(
            path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
        )

    def get_parameter_sources(
        self,
        path: str = None,
        typ: str = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ):
        return self.element_iteration.get_parameter_sources(
            path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            typ=typ,
            as_strings=as_strings,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str = None,
        default: Any = None,
        raise_on_missing: bool = False,
    ):
        return self.element_iteration.get(
            path=path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            default=default,
            raise_on_missing=raise_on_missing,
        )

    def get_EAR_dependencies(self, as_objects=False):
        """Get EARs that this EAR depends on."""

        out = []
        for src in self.get_parameter_sources(typ="EAR_output").values():
            EAR_ID_i = src["EAR_ID"]
            if EAR_ID_i != self.id_:
                # don't record a self dependency!
                out.append(EAR_ID_i)

        out = sorted(out)

        if as_objects:
            out = self.workflow.get_EARs_from_IDs(out)

        return out

    def get_input_dependencies(self):
        """Get information about locally defined input, sequence, and schema-default
        values that this EAR depends on. Note this does not get values from this EAR's
        task/schema, because the aim of this method is to help determine which upstream
        tasks this EAR depends on."""

        out = {}
        for k, v in self.get_parameter_sources().items():
            if (
                v["type"] in ["local_input", "default_input"]
                and v["task_insert_ID"] != self.task.insert_ID
            ):
                out[k] = v

        return out

    def get_dependent_EARs(
        self, as_objects=False
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get downstream EARs that depend on this EAR."""
        deps = []
        for task in self.workflow.tasks[self.task.index :]:
            for elem in task.elements[:]:
                for iter_ in elem.iterations:
                    for run in iter_.action_runs:
                        for dep_EAR_i in run.get_EAR_dependencies(as_objects=True):
                            # does dep_EAR_i belong to self?
                            if dep_EAR_i.id_ == self._id:
                                deps.append(run.id_)
        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_EARs_from_IDs(deps)

        return deps

    @property
    def inputs(self):
        if not self._inputs:
            self._inputs = self.app.ElementInputs(element_action_run=self)
        return self._inputs

    @property
    def outputs(self):
        if not self._outputs:
            self._outputs = self.app.ElementOutputs(element_action_run=self)
        return self._outputs

    @property
    def resources(self):
        if not self._resources:
            self._resources = self.app.ElementResources(**self.get_resources())
        return self._resources

    @property
    def input_files(self):
        if not self._input_files:
            self._input_files = self.app.ElementInputFiles(element_action_run=self)
        return self._input_files

    @property
    def output_files(self):
        if not self._output_files:
            self._output_files = self.app.ElementOutputFiles(element_action_run=self)
        return self._output_files

    def get_template_resources(self):
        """Get template-level resources."""
        out = {}
        for res_i in self.workflow.template.resources:
            out[res_i.scope.to_string()] = res_i._get_value()
        return out

    def get_resources(self):
        """Resolve specific resources for this EAR, considering all applicable scopes and
        template-level resources."""

        resource_specs = copy.deepcopy(self.get("resources"))
        template_resource_specs = copy.deepcopy(self.get_template_resources())
        resources = {}
        for scope in self.action.get_possible_scopes()[::-1]:
            # loop in reverse so higher-specificity scopes take precedence:
            scope_s = scope.to_string()
            scope_res = resource_specs.get(scope_s, {})
            if scope_s in template_resource_specs:
                for k, v in template_resource_specs[scope_s].items():
                    if scope_res.get(k) is None and v is not None:
                        scope_res[k] = v

            resources.update({k: v for k, v in scope_res.items() if v is not None})

        return resources

    def get_environment(self):
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot choose a single environment from this EAR because the "
                f"associated action is not expanded, meaning multiple action "
                f"environments might exist."
            )
        return self.action.environments[0].environment

    def get_input_values(self) -> Dict[str, Any]:
        return {i.path[len("inputs.") :]: i.value for i in self.inputs}

    def get_IFG_input_values(self) -> Dict[str, Any]:
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot get input file generator inputs from this EAR because the "
                f"associated action is not expanded, meaning multiple IFGs might exists."
            )
        input_types = [i.typ for i in self.action.input_file_generators[0].inputs]
        inputs = {}
        for i in self.inputs:
            typ = i.path[len("inputs.") :]
            if typ in input_types:
                inputs[typ] = i.value
        return inputs

    def get_OFP_output_files(self) -> Dict[str, Union[str, List[str]]]:
        # TODO: can this return multiple files for a given FileSpec?
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot get output file parser files from this from EAR because the "
                f"associated action is not expanded, meaning multiple OFPs might exist."
            )
        out_files = {}
        for file_spec in self.action.output_file_parsers[0].output_files:
            out_files[file_spec.label] = Path(file_spec.name.value())
        return out_files

    def get_OFP_inputs(self) -> Dict[str, Union[str, List[str]]]:
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot get output file parser inputs from this from EAR because the "
                f"associated action is not expanded, meaning multiple OFPs might exist."
            )
        inputs = {}
        for inp_typ in self.action.output_file_parsers[0].inputs or []:
            inputs[inp_typ] = self.get(f"inputs.{inp_typ}")
        return inputs

    def get_OFP_outputs(self) -> Dict[str, Union[str, List[str]]]:
        if not self.action._from_expand:
            raise RuntimeError(
                f"Cannot get output file parser outputs from this from EAR because the "
                f"associated action is not expanded, meaning multiple OFPs might exist."
            )
        outputs = {}
        for out_typ in self.action.output_file_parsers[0].outputs or []:
            outputs[out_typ] = self.get(f"outputs.{out_typ}")
        return outputs

    def compose_source(self) -> str:
        """Generate the file contents of this source."""

        script_name = self.action.script
        script_path = self.app.scripts.get(script_name)
        script_main_func = Path(script_name).stem

        with script_path.open("rt") as fp:
            script_str = fp.read()

        main_block = dedent(
            """\
            if __name__ == "__main__":
                import sys
                from pathlib import Path
                import {app_module} as app
                app.load_config(
                    log_file_path=Path("{app_package_name}.log").resolve(),
                    config_dir=r"{cfg_dir}",
                    config_invocation_key=r"{cfg_invoc_key}",
                )
                wk_path, EAR_ID = sys.argv[1:]
                EAR_ID = int(EAR_ID)
                wk = app.Workflow(wk_path)
                EAR = wk.get_EARs_from_IDs([EAR_ID])[0]
                inputs = EAR.get_input_values()
                outputs = {script_main_func}(**inputs)
                outputs = {{"outputs." + k: v for k, v in outputs.items()}}
                for name_i, out_i in outputs.items():
                    wk.set_parameter_value(param_id=EAR.data_idx[name_i], value=out_i)

        """
        )
        main_block = main_block.format(
            app_package_name=self.app.package_name,
            app_module=self.app.module,
            cfg_dir=self.app.config.config_directory,
            cfg_invoc_key=self.app.config.config_invocation_key,
            script_main_func=script_main_func,
        )

        out = dedent(
            """\
            {script_str}
            {main_block}
        """
        )
        out = out.format(script_str=script_str, main_block=main_block)
        return out

    def write_source(self):
        script_path = self.action.get_script_path(self.action.script)
        with Path(script_path).open("wt", newline="\n") as fp:
            fp.write(self.compose_source())

    def compose_commands(self, jobscript: app.Jobscript) -> Tuple[str, List[str]]:
        """
        Returns
        -------
        commands
        shell_vars
            List of shell variable names that must be saved as workflow parameter data
            as strings.
        """

        env = self.get_environment()

        def exec_script_repl(match_obj):
            typ, val = match_obj.groups()
            if typ == "executable":
                executable = env.executables.get(val)
                filterable = ("num_cores", "parallel_mode")
                filter_exec = {j: self.get_resources().get(j) for j in filterable}
                exec_cmd = executable.filter_instances(**filter_exec)[0].command
                out = exec_cmd.replace("<<num_cores>>", str(self.resources.num_cores))
            elif typ == "script":
                out = self.action.get_script_path(val)
            return out

        for ifg in self.action.input_file_generators:
            # TODO: there should only be one at this stage if expanded?
            ifg.write_source(self.action)

        for ofp in self.action.output_file_parsers:
            # TODO: there should only be one at this stage if expanded?
            ofp.write_source(self.action)

        if self.action.script:
            self.write_source()

        param_regex = r"(\<\<parameter:{}\>\>?)"
        file_regex = r"(\<\<file:{}\>\>?)"
        exe_script_regex = r"\<\<(executable|script):(.*?)\>\>"

        command_lns = []
        if env.setup:
            command_lns += list(env.setup)

        shell_vars = []
        for command in self.action.commands:
            cmd_str = command.command

            # substitute executables:
            cmd_str = re.sub(
                pattern=exe_script_regex,
                repl=exec_script_repl,
                string=cmd_str,
            )

            # substitute input parameters in command:
            for cmd_inp in self.action.get_command_input_types():
                inp_val = self.get(f"inputs.{cmd_inp}")  # TODO: what if schema output?
                cmd_str = re.sub(
                    pattern=param_regex.format(cmd_inp),
                    repl=str(inp_val),
                    string=cmd_str,
                )

            # substitute input files in command:
            for cmd_file in self.action.get_command_input_file_labels():
                file_path = self.get(f"input_files.{cmd_file}")  # TODO: what if out file?
                # assuming we have copied this file to the EAR directory, then we just
                # need the file name:
                file_name = Path(file_path).name
                cmd_str = re.sub(
                    pattern=file_regex.format(cmd_file),
                    repl=file_name,
                    string=cmd_str,
                )

            out_types = command.get_output_types()
            if out_types["stdout"]:
                # TODO: also map stderr/both if possible
                # assign stdout to a shell variable if required:
                param_name = f"outputs.{out_types['stdout']}"
                shell_var_name = f"parameter_{out_types['stdout']}"
                shell_vars.append((param_name, shell_var_name))
                cmd_str = jobscript.shell.format_stream_assignment(
                    shell_var_name=shell_var_name,
                    command=cmd_str,
                )
            elif command.stdout:
                cmd_str += f" 1>> {command.stdout}"

            if command.stderr:
                cmd_str += f" 2>> {command.stderr}"

            command_lns.append(cmd_str)

        commands = "\n".join(command_lns) + "\n"

        return commands, shell_vars


class ElementAction:
    _app_attr = "app"

    def __init__(self, element_iteration, action_idx, runs):
        self._element_iteration = element_iteration
        self._action_idx = action_idx
        self._runs = runs

        # assigned on first access of corresponding properties:
        self._run_objs = None
        self._inputs = None
        self._outputs = None
        self._resources = None
        self._input_files = None
        self._output_files = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"iter_ID={self.element_iteration.id_}, "
            f"scope={self.action.get_precise_scope().to_string()!r}, "
            f"action_idx={self.action_idx}, num_runs={self.num_runs}"
            f")"
        )

    @property
    def element_iteration(self):
        return self._element_iteration

    @property
    def element(self):
        return self.element_iteration.element

    @property
    def num_runs(self):
        return len(self._runs)

    @property
    def runs(self):
        if self._run_objs is None:
            self._run_objs = [
                self.app.ElementActionRun(
                    element_action=self,
                    index=idx,
                    **{
                        k: v
                        for k, v in i.items()
                        if k not in ("elem_iter_ID", "action_idx")
                    },
                )
                for idx, i in enumerate(self._runs)
            ]
        return self._run_objs

    @property
    def task(self):
        return self.element_iteration.task

    @property
    def action_idx(self):
        return self._action_idx

    @property
    def action(self):
        return self.task.template.get_schema_action(self.action_idx)

    @property
    def inputs(self):
        if not self._inputs:
            self._inputs = self.app.ElementInputs(element_action=self)
        return self._inputs

    @property
    def outputs(self):
        if not self._outputs:
            self._outputs = self.app.ElementOutputs(element_action=self)
        return self._outputs

    @property
    def input_files(self):
        if not self._input_files:
            self._input_files = self.app.ElementInputFiles(element_action=self)
        return self._input_files

    @property
    def output_files(self):
        if not self._output_files:
            self._output_files = self.app.ElementOutputFiles(element_action=self)
        return self._output_files

    def get_data_idx(self, path: str = None, run_idx: int = -1):
        return self.element_iteration.get_data_idx(
            path,
            action_idx=self.action_idx,
            run_idx=run_idx,
        )

    def get_parameter_sources(
        self,
        path: str = None,
        run_idx: int = -1,
        typ: str = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ):
        return self.element_iteration.get_parameter_sources(
            path,
            action_idx=self.action_idx,
            run_idx=run_idx,
            typ=typ,
            as_strings=as_strings,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str = None,
        run_idx: int = -1,
        default: Any = None,
        raise_on_missing: bool = False,
    ):
        return self.element_iteration.get(
            path=path,
            action_idx=self.action_idx,
            run_idx=run_idx,
            default=default,
            raise_on_missing=raise_on_missing,
        )

    def get_parameter_names(self, prefix):
        if prefix == "inputs":
            return list(f"{i}" for i in self.action.get_input_types())
        elif prefix == "outputs":
            return list(f"{i}" for i in self.action.get_output_types())
        elif prefix == "input_files":
            return list(f"{i}" for i in self.action.get_input_file_labels())
        elif prefix == "output_files":
            return list(f"{i}" for i in self.action.get_output_file_labels())


@dataclass
class ElementActionOLD:
    _app_attr = "app"

    element: app.Element
    root_action: app.Action
    commands: List[app.Command]

    input_file_generator: Optional[app.InputFileGenerator] = None
    output_parser: Optional[app.OutputFileParser] = None

    def get_environment(self):
        # TODO: select correct environment according to scope:
        return self.root_action.environments[0].environment

    def execute(self):
        vars_regex = r"\<\<(executable|parameter|script|file):(.*?)\>\>"
        env = None
        resolved_commands = []
        scripts = []
        for command in self.commands:
            command_resolved = command.command
            re_groups = re.findall(vars_regex, command.command)
            for typ, val in re_groups:
                sub_str_original = f"<<{typ}:{val}>>"

                if typ == "executable":
                    if env is None:
                        env = self.get_environment()
                    exe = env.executables.get(val)
                    sub_str_new = exe.instances[0].command  # TODO: ...

                elif typ == "parameter":
                    param = self.element.get(f"inputs.{val}")
                    sub_str_new = str(param)  # TODO: custom formatting...

                elif typ == "script":
                    script_name = val
                    sub_str_new = '"' + str(self.element.dir_path / script_name) + '"'
                    scripts.append(script_name)

                elif typ == "file":
                    sub_str_new = self.app.command_files.get(val).value()

                command_resolved = command_resolved.replace(sub_str_original, sub_str_new)

            resolved_commands.append(command_resolved)

        # generate scripts:
        for script in scripts:
            script_path = self.element.dir_path / script
            snippet_path = self.app.scripts.get(script)
            with snippet_path.open("rt") as fp:
                script_body = fp.readlines()

            main_func_name = script.strip(".py")  # TODO: don't assume this

            script_lns = script_body
            script_lns += [
                "\n\n",
                'if __name__ == "__main__":\n',
                "    import zarr\n",
            ]

            if self.input_file_generator:
                input_file = self.input_file_generator.input_file
                invoc_args = f"path=Path('./{input_file.value()}'), **params"
                input_zarr_groups = {
                    k.typ: self.element.data_index[f"inputs.{k.typ}"]
                    for k in self.input_file_generator.inputs
                }
                script_lns += [
                    f"    from hpcflow.sdk.core.zarr_io import zarr_decode\n\n",
                    f"    params = {{}}\n",
                    f"    param_data = Path('../../../parameter_data')\n",
                    f"    for param_group_idx in {list(input_zarr_groups.values())!r}:\n",
                ]
                for k in input_zarr_groups:
                    script_lns += [
                        f"        grp_i = zarr.open(param_data / str(param_group_idx), mode='r')\n",
                        f"        params[{k!r}] = zarr_decode(grp_i)\n",
                    ]

                script_lns += [
                    f"\n    {main_func_name}({invoc_args})\n\n",
                ]

            elif self.output_parser:
                out_name = self.output_parser.output.typ
                out_files = {k.label: k.value() for k in self.output_parser.output_files}
                invoc_args = ", ".join(f"{k}={v!r}" for k, v in out_files.items())
                output_zarr_group = self.element.data_index[f"outputs.{out_name}"]

                script_lns += [
                    f"    from hpcflow.sdk.core.zarr_io import zarr_encode\n\n",
                    f"    {out_name} = {main_func_name}({invoc_args})\n\n",
                ]

                script_lns += [
                    f"    param_data = Path('../../../parameter_data')\n",
                    f"    output_group = zarr.open(param_data / \"{str(output_zarr_group)}\", mode='r+')\n",
                    f"    zarr_encode({out_name}, output_group)\n",
                ]

            with script_path.open("wt", newline="") as fp:
                fp.write("".join(script_lns))

        for command in resolved_commands:
            proc_i = subprocess.run(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.element.dir_path,
            )
            stdout = proc_i.stdout.decode()
            stderr = proc_i.stderr.decode()
            if stdout:
                print(stdout)
            if stderr:
                print(stderr)


class ActionScope(JSONLike):
    """Class to represent the identification of a subset of task schema actions by a
    filtering process.
    """

    _child_objects = (
        ChildObjectSpec(
            name="typ",
            json_like_name="type",
            class_name="ActionScopeType",
            is_enum=True,
        ),
    )

    def __init__(self, typ: Union[app.ActionScopeType, str], **kwargs):
        if isinstance(typ, str):
            typ = getattr(self.app.ActionScopeType, typ.upper())

        self.typ = typ
        self.kwargs = {k: v for k, v in kwargs.items() if v is not None}

        bad_keys = set(kwargs.keys()) - ACTION_SCOPE_ALLOWED_KWARGS[self.typ.name]
        if bad_keys:
            raise TypeError(
                f"The following keyword arguments are unknown for ActionScopeType "
                f"{self.typ.name}: {bad_keys}."
            )

    def __repr__(self):
        kwargs_str = ""
        if self.kwargs:
            kwargs_str = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        return f"{self.__class__.__name__}.{self.typ.name.lower()}({kwargs_str})"

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.typ is other.typ and self.kwargs == other.kwargs:
            return True
        return False

    @classmethod
    def _parse_from_string(cls, string):
        typ_str, kwargs_str = re.search(ACTION_SCOPE_REGEX, string).groups()
        kwargs = {}
        if kwargs_str:
            for i in kwargs_str.split(","):
                name, val = i.split("=")
                kwargs[name.strip()] = val.strip()
        return {"type": typ_str, **kwargs}

    def to_string(self):
        kwargs_str = ""
        if self.kwargs:
            kwargs_str = "[" + ", ".join(f"{k}={v}" for k, v in self.kwargs.items()) + "]"
        return f"{self.typ.name.lower()}{kwargs_str}"

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if isinstance(json_like, str):
            json_like = cls._parse_from_string(json_like)
        else:
            typ = json_like.pop("type")
            json_like = {"type": typ, **json_like.pop("kwargs", {})}
        return super().from_json_like(json_like, shared_data)

    @classmethod
    def any(cls):
        return cls(typ=ActionScopeType.ANY)

    @classmethod
    def main(cls):
        return cls(typ=ActionScopeType.MAIN)

    @classmethod
    def processing(cls):
        return cls(typ=ActionScopeType.PROCESSING)

    @classmethod
    def input_file_generator(cls, file=None):
        return cls(typ=ActionScopeType.INPUT_FILE_GENERATOR, file=file)

    @classmethod
    def output_file_parser(cls, output=None):
        return cls(typ=ActionScopeType.OUTPUT_FILE_PARSER, output=output)


@dataclass
class ActionEnvironment(JSONLike):
    _app_attr = "app"

    _child_objects = (
        ChildObjectSpec(
            name="scope",
            class_name="ActionScope",
        ),
        ChildObjectSpec(
            name="environment",
            class_name="Environment",
            shared_data_name="environments",
            shared_data_primary_key="name",
        ),
    )

    environment: app.Environment
    scope: Optional[app.ActionScope] = None

    def __post_init__(self):
        if self.scope is None:
            self.scope = self.app.ActionScope.any()


@dataclass
class ActionRule(JSONLike):
    """Class to represent a rule/condition that must be True if an action is to be
    included."""

    _app_attr = "app"

    _child_objects = (ChildObjectSpec(name="rule", class_obj=Rule),)

    check_exists: Optional[str] = None
    check_missing: Optional[str] = None
    rule: Optional[Rule] = None

    def __post_init__(self):
        if (
            self.check_exists is not None
            and self.check_missing is not None
            and self.rule is not None
        ) or (
            self.check_exists is None and self.check_missing is None and self.rule is None
        ):
            raise ValueError(
                "Specify exactly one of `check_exists`, `check_missing` and `rule`."
            )

    def __repr__(self):
        out = f"{self.__class__.__name__}("
        if self.check_exists:
            out += f"check_exists={self.check_exists!r}"
        elif self.check_missing:
            out += f"check_missing={self.check_missing!r}"
        else:
            out += f"rule={self.rule}"
        out += ")"
        return out


class Action(JSONLike):
    """"""

    _app_attr = "app"
    _child_objects = (
        ChildObjectSpec(
            name="commands",
            class_name="Command",
            is_multiple=True,
        ),
        ChildObjectSpec(
            name="input_file_generators",
            is_multiple=True,
            class_name="InputFileGenerator",
            dict_key_attr="input_file",
        ),
        ChildObjectSpec(
            name="output_file_parsers",
            is_multiple=True,
            class_name="OutputFileParser",
            dict_key_attr="output",
        ),
        ChildObjectSpec(
            name="input_files",
            is_multiple=True,
            class_name="FileSpec",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="output_files",
            is_multiple=True,
            class_name="FileSpec",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="environments",
            class_name="ActionEnvironment",
            is_multiple=True,
        ),
        ChildObjectSpec(
            name="rules",
            class_name="ActionRule",
            is_multiple=True,
        ),
    )

    def __init__(
        self,
        environments: List[app.ActionEnvironment],
        commands: Optional[List[app.Command]] = None,
        script: Optional[str] = None,
        abortable: Optional[bool] = False,
        input_file_generators: Optional[List[app.InputFileGenerator]] = None,
        output_file_parsers: Optional[List[app.OutputFileParser]] = None,
        input_files: Optional[List[app.FileSpec]] = None,
        output_files: Optional[List[app.FileSpec]] = None,
        rules: Optional[List[app.ActionRule]] = None,
    ):
        self.commands = commands or []
        self.script = script
        self.environments = environments
        self.abortable = abortable
        self.input_file_generators = input_file_generators or []
        self.output_file_parsers = output_file_parsers or []
        self.input_files = self._resolve_input_files(input_files or [])
        self.output_files = self._resolve_output_files(output_files or [])
        self.rules = rules or []

        self._task_schema = None  # assigned by parent TaskSchema
        self._from_expand = False  # assigned on creation of new Action by `expand`

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        _from_expand = kwargs.pop("_from_expand")
        _task_schema = kwargs.pop("_task_schema", None)
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._from_expand = _from_expand
        obj._task_schema = _task_schema
        return obj

    @property
    def task_schema(self):
        return self._task_schema

    def _resolve_input_files(self, input_files):
        in_files = input_files
        for i in self.input_file_generators:
            if i.input_file not in in_files:
                in_files.append(i.input_file)
        return in_files

    def _resolve_output_files(self, output_files):
        out_files = output_files
        for i in self.output_file_parsers:
            for j in i.output_files:
                if j not in out_files:
                    out_files.append(j)
        return out_files

    def __repr__(self) -> str:
        IFGs = {
            i.input_file.label: [j.typ for j in i.inputs]
            for i in self.input_file_generators
        }
        OFPs = {
            i.output.typ: [j.label for j in i.output_files]
            for i in self.output_file_parsers
        }

        out = []
        if self.commands:
            out.append(f"commands={self.commands!r}")
        if self.script:
            out.append(f"script={self.script!r}")
        if self.environments:
            out.append(f"environments={self.environments!r}")
        if IFGs:
            out.append(f"input_file_generators={IFGs!r}")
        if OFPs:
            out.append(f"output_file_parsers={OFPs!r}")
        if self.rules:
            out.append(f"rules={self.rules!r}")

        return f"{self.__class__.__name__}({', '.join(out)})"

    def __eq__(self, other):
        if type(other) is not self.__class__:
            return False
        if (
            self.commands == other.commands
            and self.script == other.script
            and self.environments == other.environments
            and self.abortable == other.abortable
            and self.input_file_generators == other.input_file_generators
            and self.output_file_parsers == other.output_file_parsers
            and self.rules == other.rules
        ):
            return True
        return False

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        _from_expand = json_like.pop("_from_expand", None)
        obj = cls(**json_like)
        obj._from_expand = _from_expand
        return obj

    def get_parameter_dependence(self, parameter: app.SchemaParameter):
        """Find if/where a given parameter is used by the action."""
        writer_files = [
            i.input_file
            for i in self.input_file_generators
            if parameter.parameter in i.inputs
        ]  # names of input files whose generation requires this parameter
        commands = []  # TODO: indices of commands in which this parameter appears
        out = {"input_file_writers": writer_files, "commands": commands}
        return out

    def get_resolved_action_env(
        self,
        relevant_scopes: Tuple[app.ActionScopeType],
        input_file_generator: app.InputFileGenerator = None,
        output_file_parser: app.OutputFileParser = None,
        commands: List[app.Command] = None,
    ):
        possible = [i for i in self.environments if i.scope.typ in relevant_scopes]
        if not possible:
            if input_file_generator:
                msg = f"input file generator {input_file_generator.input_file.label!r}"
            elif output_file_parser:
                msg = f"output file parser {output_file_parser.output.typ!r}"
            else:
                msg = f"commands {commands!r}"
            raise MissingCompatibleActionEnvironment(
                f"No compatible environment is specified for the {msg}."
            )

        # sort by scope type specificity:
        possible_srt = sorted(possible, key=lambda i: i.scope.typ.value, reverse=True)
        return possible_srt[0]

    def get_input_file_generator_action_env(
        self, input_file_generator: app.InputFileGenerator
    ):
        return self.get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.INPUT_FILE_GENERATOR,
            ),
            input_file_generator=input_file_generator,
        )

    def get_output_file_parser_action_env(self, output_file_parser: app.OutputFileParser):
        return self.get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.OUTPUT_FILE_PARSER,
            ),
            output_file_parser=output_file_parser,
        )

    def get_commands_action_env(self):
        return self.get_resolved_action_env(
            relevant_scopes=(ActionScopeType.ANY, ActionScopeType.MAIN),
            commands=self.commands,
        )

    def get_script_path(self, script_name):
        """Return the script path, relative to the EAR directory."""
        return Path(*script_name.split("/")).parts[-1]

    def expand(self):
        if self._from_expand:
            # already expanded
            return [self]

        else:
            # run main if:
            #   - one or more output files are not passed
            # run IFG if:
            #   - one or more output files are not passed
            #   - AND input file is not passed
            # always run OPs, for now

            out_file_rules = [
                self.app.ActionRule(check_missing=f"output_files.{j.label}")
                for i in self.output_file_parsers
                for j in i.output_files
            ]

            main_rules = self.rules + out_file_rules

            # note we keep the IFG/OPs in the new actions, so we can check the parameters
            # used/produced.

            inp_files = []
            inp_acts = []
            for ifg in self.input_file_generators:
                cmd = f"<<executable:python>> <<script:{ifg.script}>> $WK_PATH $EAR_ID"
                act_i = self.app.Action(
                    commands=[app.Command(cmd)],
                    input_file_generators=[ifg],
                    environments=[self.get_input_file_generator_action_env(ifg)],
                    rules=main_rules + [ifg.get_action_rule()],
                    abortable=ifg.abortable,
                )
                act_i._task_schema = self.task_schema
                if ifg.input_file not in inp_files:
                    inp_files.append(ifg.input_file)
                act_i._from_expand = True
                inp_acts.append(act_i)

            out_files = []
            out_acts = []
            for ofp in self.output_file_parsers:
                cmd = f"<<executable:python>> <<script:{ofp.script}>> $WK_PATH $EAR_ID"
                act_i = self.app.Action(
                    commands=[app.Command(cmd)],
                    output_file_parsers=[ofp],
                    environments=[self.get_output_file_parser_action_env(ofp)],
                    rules=list(self.rules),
                    abortable=ofp.abortable,
                )
                act_i._task_schema = self.task_schema
                for j in ofp.output_files:
                    if j not in out_files:
                        out_files.append(j)
                act_i._from_expand = True
                out_acts.append(act_i)

            commands = self.commands
            if self.script:
                commands += [
                    self.app.Command(
                        f"<<executable:python>> <<script:{self.script}>> $WK_PATH $EAR_ID"
                    )
                ]

            main_act = self.app.Action(
                commands=commands,
                script=self.script,
                environments=[self.get_commands_action_env()],
                abortable=self.abortable,
                rules=main_rules,
                input_files=inp_files,
                output_files=out_files,
            )
            main_act._task_schema = self.task_schema
            main_act._from_expand = True

            cmd_acts = inp_acts + [main_act] + out_acts

            return cmd_acts

    def get_command_input_types(self) -> Tuple[str]:
        """Get parameter types from commands."""
        params = []
        # note: we use "parameter" rather than "input", because it could be a schema input
        # or schema output.
        vars_regex = r"\<\<parameter:(.*?)\>\>"
        for command in self.commands:
            for val in re.findall(vars_regex, command.command):
                params.append(val)
            # TODO: consider stdin?
        return tuple(set(params))

    def get_command_input_file_labels(self) -> Tuple[str]:
        """Get input files types from commands."""
        files = []
        vars_regex = r"\<\<file:(.*?)\>\>"
        for command in self.commands:
            for val in re.findall(vars_regex, command.command):
                files.append(val)
            # TODO: consider stdin?
        return tuple(set(files))

    def get_command_output_types(self) -> Tuple[str]:
        """Get parameter types from command stdout and stderr arguments."""
        params = []
        for command in self.commands:
            out_params = command.get_output_types()
            if out_params["stdout"]:
                params.append(out_params["stdout"])
            if out_params["stderr"]:
                params.append(out_params["stderr"])

        return tuple(set(params))

    def get_input_types(self) -> Tuple[str]:
        """Get the input types that are consumed by commands and input file generators of
        this action."""
        is_script = (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        )
        if is_script:
            params = self.task_schema.input_types
        else:
            params = list(self.get_command_input_types())
            for i in self.input_file_generators:
                params.extend([j.typ for j in i.inputs])
            for i in self.output_file_parsers:
                params.extend([j for j in i.inputs or []])
        return tuple(set(params))

    def get_output_types(self) -> Tuple[str]:
        """Get the output types that are produced by command standard outputs and errors,
        and by output file parsers of this action."""
        is_script = (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        )
        if is_script:
            params = self.task_schema.output_types
        else:
            params = list(self.get_command_output_types())
            for i in self.output_file_parsers:
                params.append(i.output.typ)
                params.extend([j for j in i.outputs or []])
        return tuple(set(params))

    def get_input_file_labels(self):
        return tuple(i.label for i in self.input_files)

    def get_output_file_labels(self):
        return tuple(i.label for i in self.output_files)

    def generate_data_index(
        self,
        act_idx,
        EAR_ID,
        schema_data_idx,
        all_data_idx,
        workflow,
        param_source,
    ) -> List[int]:
        """Generate the data index for this action of an element iteration whose overall
        data index is passed.

        This mutates `all_data_idx`.

        """

        # output keys must be processed first for this to work, since when processing an
        # output key, we may need to update the index of an output in a previous action's
        # data index, which could affect the data index in an input of this action.
        keys = [f"outputs.{i}" for i in self.get_output_types()]
        keys += [f"inputs.{i}" for i in self.get_input_types()]
        for i in self.input_files:
            keys.append(f"input_files.{i.label}")
        for i in self.output_files:
            keys.append(f"output_files.{i.label}")

        # these are consumed by the OFP, so should not be considered to generate new data:
        OFP_outs = [j for i in self.output_file_parsers for j in i.outputs or []]

        # keep all resources data:
        sub_data_idx = {k: v for k, v in schema_data_idx.items() if "resources" in k}
        param_src_update = []
        for key in keys:
            sub_param_idx = {}
            if (
                key.startswith("input_files")
                or key.startswith("output_files")
                or key.startswith("inputs")
                or (key.startswith("outputs") and key.split("outputs.")[1] in OFP_outs)
            ):
                # look for an index in previous data indices (where for inputs we look
                # for *output* parameters of the same name):
                k_idx = None
                for prev_data_idx in all_data_idx.values():
                    if key.startswith("inputs"):
                        k_param = key.split("inputs.")[1]
                        k_out = f"outputs.{k_param}"
                        if k_out in prev_data_idx:
                            k_idx = prev_data_idx[k_out]

                    else:
                        if key in prev_data_idx:
                            k_idx = prev_data_idx[key]

                if k_idx is None:
                    # otherwise take from the schema_data_idx:
                    if key in schema_data_idx:
                        k_idx = schema_data_idx[key]
                        # add any associated sub-parameters:
                        for k, v in schema_data_idx.items():
                            if k.startswith(f"{key}."):  # sub-parameter (note dot)
                                sub_param_idx[k] = v
                    else:
                        # otherwise we need to allocate a new parameter datum:
                        # (for input/output_files keys)
                        k_idx = workflow._add_unset_parameter_data(param_source)

            else:
                # outputs
                k_idx = None
                for (act_idx_i, EAR_ID_i), prev_data_idx in all_data_idx.items():
                    if key in prev_data_idx:
                        k_idx = prev_data_idx[key]

                        # allocate a new parameter datum for this intermediate output:
                        param_source_i = copy.deepcopy(param_source)
                        # param_source_i["action_idx"] = act_idx_i
                        param_source_i["EAR_ID"] = EAR_ID_i
                        new_k_idx = workflow._add_unset_parameter_data(param_source_i)

                        # mutate `all_data_idx`:
                        prev_data_idx[key] = new_k_idx

                if k_idx is None:
                    # otherwise take from the schema_data_idx:
                    k_idx = schema_data_idx[key]

                # can now set the EAR/act idx in the associated parameter source
                param_src_update.append(k_idx)

            sub_data_idx[key] = k_idx
            sub_data_idx.update(sub_param_idx)

        all_data_idx[(act_idx, EAR_ID)] = sub_data_idx

        return param_src_update

    def get_possible_scopes(self) -> Tuple[app.ActionScope]:
        """Get the action scopes that are inclusive of this action, ordered by decreasing
        specificity."""

        scope = self.get_precise_scope()

        if self.input_file_generators:
            scopes = (
                scope,
                self.app.ActionScope.input_file_generator(),
                self.app.ActionScope.processing(),
                self.app.ActionScope.any(),
            )
        elif self.output_file_parsers:
            scopes = (
                scope,
                self.app.ActionScope.output_file_parser(),
                self.app.ActionScope.processing(),
                self.app.ActionScope.any(),
            )
        else:
            scopes = (scope, self.app.ActionScope.any())

        return scopes

    def get_precise_scope(self) -> app.ActionScope:
        if not self._from_expand:
            raise RuntimeError(
                "Precise scope cannot be unambiguously defined until the Action has been "
                "expanded."
            )

        if self.input_file_generators:
            return self.app.ActionScope.input_file_generator(
                file=self.input_file_generators[0].input_file.label
            )
        elif self.output_file_parsers:
            return self.app.ActionScope.output_file_parser(
                output=self.output_file_parsers[0].output.typ
            )
        else:
            return self.app.ActionScope.main()

    def is_input_type_required(
        self, typ: str, provided_files: List[app.FileSpec]
    ) -> bool:
        # TODO: for now assume a script takes all inputs
        if (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        ):
            return True

        # typ is required if is appears in any command:
        if typ in self.get_command_input_types():
            return True

        # typ is required if used in any input file generators and input file is not
        # provided:
        for IFG in self.input_file_generators:
            if typ in (i.typ for i in IFG.inputs):
                if IFG.input_file not in provided_files:
                    return True

        # typ is required if used in any output file parser
        for OFP in self.output_file_parsers:
            if typ in (OFP.inputs or []):
                return True
