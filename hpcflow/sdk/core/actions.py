from __future__ import annotations
import copy
from dataclasses import dataclass
import enum
import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Union

from valida.rules import Rule

from hpcflow.sdk.core.command_files import FileSpec, InputFileGenerator, OutputFileParser
from hpcflow.sdk.core.commands import Command
from hpcflow.sdk.core.environment import Environment
from hpcflow.sdk.core.errors import MissingCompatibleActionEnvironment
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike


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


class ElementActionRun:
    _app_attr = "app"

    def __init__(self, element_action, index, data_idx: Dict) -> None:
        self._element_action = element_action
        self._index = index
        self._data_idx = data_idx

        # assigned on first access of corresponding properties:
        self._inputs = None
        self._outputs = None
        self._resources = None
        self._input_files = None
        self._output_files = None

    @property
    def element_action(self):
        return self._element_action

    @property
    def action(self):
        return self.element_action.action

    @property
    def element(self):
        return self.element_action.element

    @property
    def workflow(self):
        return self.element.workflow

    @property
    def element_index(self):
        return self.element.index

    @property
    def index(self):
        return self._index

    @property
    def data_idx(self):
        return self._data_idx

    @property
    def task(self):
        return self.element_action.task

    def get_parameter_names(self, prefix):
        return self.element_action.get_parameter_names(prefix)

    def get_data_idx(self, path: str = None):
        return self.element.get_data_idx(
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
        return self.element.get_parameter_sources(
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
        return self.element.get(
            path=path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            default=default,
            raise_on_missing=raise_on_missing,
        )

    def get_EAR_dependencies(self, as_objects=False):
        """Get EARs that this EAR depends on."""

        out = []
        self_EAR = (
            self.task.insert_ID,
            self.element_index,
            self.element_action.action_idx,
            self.index,
        )
        for src in self.get_parameter_sources(typ="EAR_output").values():
            src_i = (
                src["task_insert_ID"],
                src["element_idx"],
                src["action_idx"],
                src["run_idx"],
            )
            if src_i != self_EAR:
                # don't record a self dependency!
                out.append(src_i)

        out = sorted(out)

        if as_objects:
            out = self.workflow.get_EARs_from_indices(out)

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

    def get_resources(self):
        """"""
        resource_specs = self.get("resources")
        for scope in self.action.get_possible_scopes():
            scope_s = scope.to_string()
            if scope_s in resource_specs:
                resources = resource_specs[scope_s]
                break

        return resources


class ElementAction:

    _app_attr = "app"

    def __init__(self, element, action_idx, runs):
        self._element = element
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
            f"scope={self.action.get_precise_scope().to_string()!r}, "
            f"action_idx={self.action_idx}, num_runs={self.num_runs}"
            f")"
        )

    @property
    def element(self):
        return self._element

    @property
    def element_index(self):
        return self.element.index

    @property
    def num_runs(self):
        return len(self._runs)

    @property
    def runs(self):
        if self._run_objs is None:
            self._run_objs = [
                self.app.ElementActionRun(self, index=idx, **i)
                for idx, i in enumerate(self._runs)
            ]
        return self._run_objs

    @property
    def task(self):
        return self.element.task

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
    def resources(self):
        if not self._resources:
            self._resources = self.app.ElementResources(element_action=self)
        return self._resources

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
        return self.element.get_data_idx(
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
        return self.element.get_parameter_sources(
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
        return self.element.get(
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

    element: Element
    root_action: Action
    commands: List[Command]

    input_file_generator: Optional[InputFileGenerator] = None
    output_parser: Optional[OutputFileParser] = None

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

    def __init__(self, typ: Union[ActionScopeType, str], **kwargs):

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

    environment: Environment
    scope: Optional[ActionScope] = None

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
        commands: List[Command],
        environments: List[ActionEnvironment],
        input_file_generators: Optional[List[InputFileGenerator]] = None,
        output_file_parsers: Optional[List[OutputFileParser]] = None,
        input_files: Optional[List[FileSpec]] = None,
        output_files: Optional[List[FileSpec]] = None,
        rules: Optional[List[ActionRule]] = None,
    ):
        self.commands = commands
        self.environments = environments
        self.input_file_generators = input_file_generators or []
        self.output_file_parsers = output_file_parsers or []
        self.input_files = self._resolve_input_files(input_files or [])
        self.output_files = self._resolve_output_files(output_files or [])
        self.rules = rules or []

        self._from_expand = False  # assigned on creation of new Action by `expand`

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
            and self.environments == other.environments
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

    def get_parameter_dependence(self, parameter: SchemaParameter):
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
        relevant_scopes: Tuple[ActionScopeType],
        input_file_generator: InputFileGenerator = None,
        output_file_parser: OutputFileParser = None,
        commands: List[Command] = None,
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
        self, input_file_generator: InputFileGenerator
    ):
        return self.get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.INPUT_FILE_GENERATOR,
            ),
            input_file_generator=input_file_generator,
        )

    def get_output_file_parser_action_env(self, output_file_parser: OutputFileParser):
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
            for IFG_i in self.input_file_generators:
                script = "script-name"  # TODO
                act_i = self.app.Action(
                    commands=[
                        self.app.Command(f"<<executable:python>> <<script:{script}>>")
                    ],
                    input_file_generators=[IFG_i],
                    environments=[self.get_input_file_generator_action_env(IFG_i)],
                    rules=main_rules + [IFG_i.get_action_rule()],
                )
                inp_files.append(IFG_i.input_file)
                act_i._from_expand = True
                inp_acts.append(act_i)

            out_files = []
            out_acts = []
            for OP_i in self.output_file_parsers:
                script = "script-name"  # TODO
                act_i = self.app.Action(
                    commands=[
                        self.app.Command(f"<<executable:python>> <<script:{script}>>")
                    ],
                    output_file_parsers=[OP_i],
                    environments=[self.get_output_file_parser_action_env(OP_i)],
                    rules=list(self.rules),
                )
                out_files.extend(OP_i.output_files)
                act_i._from_expand = True
                out_acts.append(act_i)

            main_act = self.app.Action(
                commands=self.commands,
                environments=[self.get_commands_action_env()],
                rules=main_rules,
                input_files=inp_files,
                output_files=out_files,
            )
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

    def get_command_output_types(self) -> Tuple[str]:
        """Get parameter types from command stdout and stderr arguments."""
        params = []
        # note: we use "parameter" rather than "output", because it could be a schema
        # output or schema input.
        vars_regex = r"\<\<parameter:(.*?)\>\>"
        for command in self.commands:
            for i, label in zip((command.stdout, command.stderr), ("stdout", "stderr")):
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
                        params.append(param_typ)
        return tuple(set(params))

    def get_input_types(self) -> Tuple[str]:
        """Get the input types that are consumed by commands and input file generators of
        this action."""
        params = list(self.get_command_input_types())
        for i in self.input_file_generators:
            params.extend([j.typ for j in i.inputs])
        return tuple(set(params))

    def get_output_types(self) -> Tuple[str]:
        """Get the output types that are produced by command standard outputs and errors,
        and by output file parsers of this action."""
        params = list(self.get_command_output_types())
        for i in self.output_file_parsers:
            params.append(i.output.typ)
        return tuple(params)

    def get_input_file_labels(self):
        return tuple(i.label for i in self.input_files)

    def get_output_file_labels(self):
        return tuple(i.label for i in self.output_files)

    def generate_data_index(self, data_idx, workflow, param_source):
        """Generate the data index for this action of an element whose overall data index
        is passed."""

        keys = [f"inputs.{i}" for i in self.get_input_types()]
        keys += [f"outputs.{i}" for i in self.get_output_types()]
        for i in self.input_files:
            keys.append(f"input_files.{i.label}")
        for i in self.output_files:
            keys.append(f"output_files.{i.label}")
        keys = set(keys)

        # keep all resources data:
        sub_data_idx = {k: v for k, v in data_idx.items() if "resources" in k}

        # allocate parameter data for intermediate input/output files:
        for k in keys:
            if k in data_idx:
                sub_data_idx[k] = data_idx[k]
            else:
                sub_data_idx[k] = workflow._add_unset_parameter_data(param_source)

        return sub_data_idx

    # def test_element(self, element):
    #     return all(i.test_element(element) for i in self.rules)

    def get_possible_scopes(self) -> Tuple[ActionScope]:
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

    def get_precise_scope(self) -> ActionScope:
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
