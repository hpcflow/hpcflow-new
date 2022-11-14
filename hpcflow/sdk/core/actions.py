from __future__ import annotations
from dataclasses import dataclass
import enum
import re
from typing import List, Optional, Tuple

from valida.conditions import ConditionLike

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

    def __init__(self, typ: ActionScopeType, **kwargs):
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
    scope: ActionScope

    @classmethod
    def prepare_from_json_like(cls, json_like):

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
class ActionCondition:
    """Class to represent a condition that must be met if an action is to be included."""

    path: List[str]
    condition: Optional[ConditionLike] = None


class Action(JSONLike):
    """"""

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
    )

    def __init__(
        self,
        commands: List[Command],
        environments: List[ActionEnvironment],
        input_file_generators: Optional[List[InputFileGenerator]] = None,
        output_file_parsers: Optional[List[OutputFileParser]] = None,
        conditions: Optional[List[ActionCondition]] = None,
    ):
        self.commands = commands
        self.environments = environments
        self.input_file_generators = input_file_generators or []
        self.output_file_parsers = output_file_parsers or []
        self.conditions = conditions or []

    def __eq__(self, other):
        if type(other) is not self.__class__:
            return False
        if (
            self.commands == other.commands
            and self.environments == other.environments
            and self.input_file_generators == other.input_file_generators
            and self.output_file_parsers == other.output_file_parsers
            and self.conditions == other.conditions
        ):
            return True
        return False

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
        print(f"relevant_scopes: {relevant_scopes}")
        print(f"self.environments: {self.environments}")
        print(
            f"[i.scope.typ for i in self.environments]: {[i.scope.typ for i in self.environments]}"
        )
        possible = [
            i.scope.type for i in self.environments if i.scope.typ in relevant_scopes
        ]
        print(f"possible: {possible}")
        if not possible:
            if input_file_generator:
                msg = f"input file generator {input_file_generator!r}."
            elif output_file_parser:
                msg = f"output file parser {output_file_parser!r}"
            else:
                msg = f"commands {commands!r}"
            raise MissingCompatibleActionEnvironment(
                f"No compatible environment is specified for the {msg}."
            )

        # sort by scope specificity:
        possible_srt = sorted(possible, key=lambda i: i.scope.value, reverse=True)
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

    def resolve_actions(self):

        cmd_acts = []
        for i in self.input_file_generators:
            act_i = InputFileGeneratorAction(
                input_file_generator=i,
                conditions=self.conditions,
                environment=self.get_input_file_generator_action_env(i),
            )
            cmd_acts.append(act_i)

        cmd_acts.append(
            CommandsAction(
                commands=self.commands,
                environment=self.get_commands_action_env(),
                conditions=self.conditions,
            )
        )

        for i in self.output_file_parsers:
            act_i = OutputFileParserAction(
                output_file_parser=i,
                environment=self.get_output_file_parser_action_env(i),
                conditions=self.conditions,
            )
            cmd_acts.append(act_i)

        return cmd_acts


@dataclass
class ResolvedAction:
    """"""

    environment: Environment
    conditions: List[ActionCondition]

    def __post_init__(self):
        # select correct environment
        pass


@dataclass
class CommandsAction(ResolvedAction):
    """Represents an action without any associated input file generators and output
    parsers."""

    commands: List[Command]


@dataclass
class InputFileGeneratorAction(ResolvedAction):
    input_file_generator: InputFileGenerator

    def __post_init__(self):
        self.conditions = (
            []
        )  # TODO: add a condition, according to non-presence of input file?


@dataclass
class OutputFileParserAction(ResolvedAction):
    output_file_parser: OutputFileParser
