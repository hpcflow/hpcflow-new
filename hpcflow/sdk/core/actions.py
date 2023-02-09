from __future__ import annotations
from dataclasses import dataclass, field
import enum
import re
import subprocess
from textwrap import dedent
from typing import List, Optional, Tuple, Union

from valida.conditions import ConditionLike, NullCondition
from valida.rules import Rule
from valida.datapath import DataPath

from hpcflow.sdk.core.command_files import InputFileGenerator, OutputFileParser
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


@dataclass
class ElementActionNEW:

    _app_attr = "app"

    element: Element
    action: Action


@dataclass
class ElementAction:

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
            shared_data_name="envs",
            shared_data_primary_key="name",
        ),
    )

    environment: Environment
    scope: Optional[ActionScope] = None

    def __post_init__(self):
        if self.scope is None:
            self.scope = self.app.ActionScope.any()

    @classmethod
    def prepare_from_json_like(cls, json_like):

        # TODO: delete?

        print(f"ActEnv.prep: json_like {json_like}")

        json_like = {
            "scope": {
                "typ": json_like["scope"],
                "kwargs": {
                    k: v
                    for k, v in json_like.items()
                    if k not in ("environment", "scope")
                },
            },
            "environment": json_like["environment"],
        }
        return super().prepare_from_json_like(json_like)


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

    def test_element(self, element) -> bool:
        """Test if an element satisfies the rule."""

        check = self.check_exists or self.check_missing
        if check:
            param_s = check.split(".")
            if len(param_s) > 2:
                # sub-parameter, so need to retrieve parameter data
                try:
                    element.get(check, raise_on_missing=True)
                    return True if self.check_exists else False
                except ValueError:
                    return False if self.check_exists else True
            else:
                if self.check_exists:
                    return self.check_exists in element.data_index
                elif self.check_missing:
                    return self.check_missing not in element.data_index

        else:
            # retrieve parameter data
            param_path = ".".join(
                i.condition.callable.kwargs["value"] for i in rule.path.parts[:2]
            )
            element_dat = self.get(param_path)

            data_path = DataPath(*rule.path.parts[2:])
            rule = Rule(path=data_path, condition=rule.condition, cast=rule.cast)

            return rule.test(element_dat).is_valid


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
            json_like_name="input_files",
            is_multiple=True,
            class_name="InputFileGenerator",
            dict_key_attr="input_file",
        ),
        ChildObjectSpec(
            name="output_file_parsers",
            json_like_name="outputs",
            is_multiple=True,
            class_name="OutputFileParser",
            dict_key_attr="output",
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
        rules: Optional[List[ActionRule]] = None,
    ):
        self.commands = commands
        self.environments = environments
        self.input_file_generators = input_file_generators or []
        self.output_file_parsers = output_file_parsers or []
        self.rules = rules or []

        self._from_expand = False  # assigned on creation of new Action by `expand`

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

            cmd_acts = []

            # note we keep the IFG/OPs in the new actions, so we can check the parameters
            # used/produced.

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
                act_i._from_expand = True
                cmd_acts.append(act_i)

            cmd_acts.append(
                self.app.Action(
                    commands=self.commands,
                    environments=[self.get_commands_action_env()],
                    rules=main_rules,
                )
            )

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
                act_i._from_expand = True
                cmd_acts.append(act_i)

            return cmd_acts

    def resolve_actions(self):

        cmd_acts = []
        for i in self.input_file_generators:
            act_i = InputFileGeneratorAction(
                input_file_generator=i,
                conditions=self.rules,
                environment=self.get_input_file_generator_action_env(i),
            )
            cmd_acts.append(act_i)

        cmd_acts.append(
            CommandsAction(
                commands=self.commands,
                environment=self.get_commands_action_env(),
                conditions=self.rules,
            )
        )

        for i in self.output_file_parsers:
            act_i = OutputFileParserAction(
                output_file_parser=i,
                environment=self.get_output_file_parser_action_env(i),
                conditions=self.rules,
            )
            cmd_acts.append(act_i)

        return cmd_acts

    def resolve_element_actions(self, element):
        element_actions = []

        for i in self.input_file_generators:
            element_act_i = self.app.ElementAction(
                element=element,
                root_action=self,
                commands=[
                    Command(command=f"<<executable:python>> <<script:{i.script}>>")
                ],
                input_file_generator=i,
            )
            element_actions.append(element_act_i)

        element_actions.append(
            self.app.ElementAction(
                element=element,
                root_action=self,
                commands=self.commands,
            )
        )

        for i in self.output_file_parsers:
            element_act_i = self.app.ElementAction(
                element=element,
                root_action=self,
                commands=[
                    Command(command=f"<<executable:python>> <<script:{i.script}>>")
                ],
                output_parser=i,
            )
            element_actions.append(element_act_i)

        return element_actions

    def get_command_input_types(self):
        params = []
        vars_regex = r"\<\<parameter:(.*?)\>\>"
        for command in self.commands:
            re_groups = re.findall(vars_regex, command.command)
            for val in re_groups:
                params.append(val)
            # TODO: consider arguments/stdin/stderr/stdout
        return tuple(set(params))

    def get_input_types(self):
        params = list(self.get_command_input_types())
        for i in self.input_file_generators:
            params.extend([j.typ for j in i.inputs])
        return tuple(set(params))

    def get_output_types(self):
        params = []
        for i in self.output_file_parsers:
            params.append(i.output.typ)
        return tuple(params)

    def test_element(self, element):
        return all(i.test_element(element) for i in self.rules)


@dataclass
class ResolvedAction:
    """"""

    # TODO: is this used?

    pass


@dataclass
class CommandsAction(ResolvedAction):
    """Represents an action without any associated input file generators and output
    parsers."""

    # TODO: is this used?

    commands: List[Command]


@dataclass
class InputFileGeneratorAction(ResolvedAction):
    input_file_generator: InputFileGenerator

    # TODO: is this used?

    def __post_init__(self):
        self.conditions = (
            []
        )  # TODO: add a condition, according to non-presence of input file?


@dataclass
class OutputFileParserAction(ResolvedAction):
    # TODO: is this used?

    output_file_parser: OutputFileParser
