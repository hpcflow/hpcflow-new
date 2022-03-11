import copy
from dataclasses import dataclass, field
import enum
from lib2to3.pytree import Base
from re import I
from typing import Dict, List, Optional, Tuple
from numpy import isin, sort

from valida.conditions import ConditionLike

from hpcflow.command_files import InputFileGenerator, OutputFileParser
from hpcflow.commands import Command, CommandArgument
from hpcflow.environment import Environment
from hpcflow.errors import MissingActionEnvironment, MissingCompatibleActionEnvironment
from hpcflow.parameters import SchemaParameter
from hpcflow.utils import classproperty


class ActionScopeType(enum.Enum):

    ALL = 0
    MAIN = 1
    PROCESSING = 2
    INPUT_FILE_GENERATOR = 3
    OUTPUT_FILE_PARSER = 4


@dataclass
class ActionScope:
    """Class to represent the identification of a subset of task schema actions by a
    filtering process.
    """

    typ: ActionScopeType
    kwargs: Optional[Dict] = field(default_factory=lambda: {})

    @classmethod
    def main(cls):
        return cls(typ=ActionScopeType.MAIN)

    @classmethod
    def processing(cls):
        return cls(typ=ActionScopeType.PROCESSING)

    @classmethod
    def input_file_generator(cls, file=None):
        return cls(typ=ActionScopeType.INPUT_FILE_GENERATOR, kwargs={"file": file})

    @classmethod
    def output_file_parser(cls, output=None):
        return cls(typ=ActionScopeType.OUTPUT_FILE_PARSER, kwargs={"output": output})


@dataclass
class ActionEnvironment:
    environment: Environment
    scope: ActionScope

    @classmethod
    def from_spec(cls, scope, env, all_envs):
        env = [i for i in all_envs if i.name == env][0]
        scope = getattr(ActionScope, scope)()
        return cls(scope=scope, environment=env)


@dataclass
class ActionCondition:
    """Class to represent a condition that must be met if an action is to be included."""

    path: List[str]
    condition: Optional[ConditionLike] = None


class Action:
    """"""

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

    @classmethod
    def from_spec(cls, spec, all_envs, parameters, cmd_files):
        """Parse an Action definition from a JSON-like dict.

        Parameters
        ----------
        spec : dict with keys:
            commands
            input_file_generators
            output_file_parsers
            environments: dict
            environment: str
                Instead of `environments` (plural), this can be used to specify a single
                environment to be used for all resolved actions of this action.


        """
        spec = copy.deepcopy(spec)
        spec["commands"] = [Command.from_spec(i) for i in spec.get("commands", [])]
        spec["input_file_generators"] = [
            InputFileGenerator.from_spec(label, info, parameters, cmd_files)
            for label, info in spec.pop("input_files", {}).items()
        ]
        spec["output_file_parsers"] = [
            OutputFileParser.from_spec(param_typ, info, parameters, cmd_files)
            for param_typ, info in spec.pop("outputs", {}).items()
        ]
        given_envs = spec.pop("environments") or spec.pop("environment")
        if isinstance(given_envs, str):
            given_envs = {"all": given_envs}
        elif not isinstance(given_envs, dict):
            raise ValueError()

        spec["environments"] = [
            ActionEnvironment.from_spec(scope, env, all_envs)
            for scope, env in given_envs.items()
        ]
        return cls(**spec)

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
                ActionScopeType.ALL,
                ActionScopeType.PROCESSING,
                ActionScopeType.INPUT_FILE_GENERATOR,
            ),
            input_file_generator=input_file_generator,
        )

    def get_output_file_parser_action_env(self, output_file_parser: OutputFileParser):
        return self.get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ALL,
                ActionScopeType.PROCESSING,
                ActionScopeType.OUTPUT_FILE_PARSER,
            ),
            output_file_parser=output_file_parser,
        )

    def get_commands_action_env(self):
        return self.get_resolved_action_env(
            relevant_scopes=(ActionScopeType.ALL, ActionScopeType.MAIN),
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
