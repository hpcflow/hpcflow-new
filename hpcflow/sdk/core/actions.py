"""
Actions are base components of elements.
Element action runs (EARs) are the basic components of any enactment;
they may be grouped together within a jobscript for efficiency.  
"""

from __future__ import annotations
from collections.abc import Mapping
import copy
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
from pathlib import Path
import re
from textwrap import indent, dedent
from typing import cast, final, overload, TypedDict, TYPE_CHECKING

from watchdog.utils.dirsnapshot import DirectorySnapshotDiff

from hpcflow.sdk.core import ABORT_EXIT_CODE
from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.core.errors import (
    ActionEnvironmentMissingNameError,
    MissingCompatibleActionEnvironment,
    OutputFileParserNoOutputError,
    UnknownScriptDataKey,
    UnknownScriptDataParameter,
    UnsupportedScriptDataFormat,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.utils import (
    JSONLikeDirSnapShot,
    split_param_label,
    swap_nested_dict_keys,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.core.run_dir_files import RunDirAppFiles

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any, ClassVar, Literal
    from typing_extensions import NotRequired, Self
    from valida.conditions import ConditionLike  # type: ignore

    from ..typing import DataIndex, ParamSource
    from ..submission.jobscript import Jobscript
    from .commands import Command
    from .command_files import InputFileGenerator, OutputFileParser, FileSpec
    from .element import (
        Element,
        ElementIteration,
        ElementInputs,
        ElementOutputs,
        ElementResources,
        ElementInputFiles,
        ElementOutputFiles,
    )
    from .environment import Environment
    from .object_list import ParametersList
    from .parameters import SchemaParameter, ParameterValue, Parameter
    from .rule import Rule
    from .task import WorkflowTask
    from .task_schema import TaskSchema
    from .workflow import Workflow


ACTION_SCOPE_REGEX = r"(\w*)(?:\[(.*)\])?"


class ParameterDependence(TypedDict):
    """
    Dependency descriptor for a parameter.
    """

    #: The input file writers that can use the parameter.
    input_file_writers: list[FileSpec]
    #: The commands that can use the parameter.
    commands: list[int]


class ScriptData(TypedDict, total=False):
    """
    Descriptor for data relating to a script.
    """

    #: The format of the data.
    format: str
    #: Whether the data is required for all iterations.
    all_iterations: NotRequired[bool]


class ActionScopeType(Enum):
    """
    Types of action scope.
    """

    #: Scope that applies to anything.
    ANY = 0
    #: Scope that only applies to main scripts.
    MAIN = 1
    #: Scope that applies to processing steps.
    PROCESSING = 2
    #: Scope that applies to input file generators.
    INPUT_FILE_GENERATOR = 3
    #: Scope that applies to output file parsers.
    OUTPUT_FILE_PARSER = 4


#: Keyword arguments permitted for particular scopes.
ACTION_SCOPE_ALLOWED_KWARGS = {
    ActionScopeType.ANY.name: set(),
    ActionScopeType.MAIN.name: set(),
    ActionScopeType.PROCESSING.name: set(),
    ActionScopeType.INPUT_FILE_GENERATOR.name: {"file"},
    ActionScopeType.OUTPUT_FILE_PARSER.name: {"output"},
}


@dataclass(frozen=True)
class _EARStatus:
    """
    Model of the state of an EARStatus.
    """

    _value: int
    #: Symbol to use when rendering a status.
    symbol: str
    #: Colour to use when rendering a status.
    colour: str
    __doc__: str = ""


class EARStatus(_EARStatus, Enum):
    """Enumeration of all possible EAR statuses, and their associated status colour."""

    #: Not yet associated with a submission.
    pending = (
        0,
        ".",
        "grey46",
        "Not yet associated with a submission.",
    )
    #: Associated with a prepared submission that is not yet submitted.
    prepared = (
        1,
        ".",
        "grey46",
        "Associated with a prepared submission that is not yet submitted.",
    )
    #: Submitted for execution.
    submitted = (
        2,
        ".",
        "grey46",
        "Submitted for execution.",
    )
    #: Executing now.
    running = (
        3,
        "●",
        "dodger_blue1",
        "Executing now.",
    )
    #: Not attempted due to a failure of an upstream action on which this depends,
    #: or a loop termination condition being satisfied.
    skipped = (
        4,
        "s",
        "dark_orange",
        (
            "Not attempted due to a failure of an upstream action on which this depends, "
            "or a loop termination condition being satisfied."
        ),
    )
    #: Aborted by the user; downstream actions will be attempted.
    aborted = (
        5,
        "A",
        "deep_pink4",
        "Aborted by the user; downstream actions will be attempted.",
    )
    #: Probably exited successfully.
    success = (
        6,
        "■",
        "green3",
        "Probably exited successfully.",
    )
    #: Probably failed.
    error = (
        7,
        "E",
        "red3",
        "Probably failed.",
    )

    @property
    def value(self) -> int:
        #: The value of the status.
        return self._value

    @classmethod
    def get_non_running_submitted_states(cls) -> frozenset[EARStatus]:
        """Return the set of all non-running states, excluding those before submission."""
        return frozenset(
            {
                cls.skipped,
                cls.aborted,
                cls.success,
                cls.error,
            }
        )

    @property
    def rich_repr(self) -> str:
        """
        The rich representation of the value.
        """
        return f"[{self.colour}]{self.symbol}[/{self.colour}]"


class ElementActionRun(AppAware):
    """
    The Element Action Run (EAR) is an atomic unit of an enacted workflow, representing
    one unit of work (e.g., particular submitted job to run a program) within that
    overall workflow. With looping over, say, parameter spaces, there may be many EARs
    per element.

    Parameters
    ----------
    id_: int
        The ID of the EAR.
    is_pending: bool
        Whether this EAR is pending.
    element_action:
        The particular element action that this is a run of.
    index: int:
        The index of the run within the collection of runs.
    data_idx: dict
        Used for looking up input data to the EAR.
    commands_idx: list[int]
        Indices of commands to apply.
    start_time: datetime
        Time of start of run, if the run has ever been started.
    end_time: datetime
        Time of end of run, if the run has ever ended.
    snapshot_start: dict
        Parameters for taking a snapshot of the data directory before the run.
        If unspecified, no snapshot will be taken.
    snapshot_end: dict
        Parameters for taking a snapshot of the data directory after the run.
        If unspecified, no snapshot will be taken.
    submission_idx: int
        What submission was this (if it has been submitted)?
    success: bool
        Whether this EAR succeeded (if it has run).
    skip: bool
        Whether this EAR was skipped.
    exit_code: int
        The exit code, if known.
    metadata: dict
        Metadata about the EAR.
    run_hostname: str
        Where to run the EAR (if not locally).
    """

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        element_action: ElementAction,
        index: int,
        data_idx: DataIndex,
        commands_idx: list[int],
        start_time: datetime | None,
        end_time: datetime | None,
        snapshot_start: dict[str, Any] | None,
        snapshot_end: dict[str, Any] | None,
        submission_idx: int | None,
        success: bool | None,
        skip: bool,
        exit_code: int | None,
        metadata: dict[str, Any],
        run_hostname: str | None,
    ) -> None:
        self._id = id_
        self._is_pending = is_pending
        self._element_action = element_action
        self._index = index  # local index of this run with the action
        self._data_idx = data_idx
        self._commands_idx = commands_idx
        self._start_time = start_time
        self._end_time = end_time
        self._submission_idx = submission_idx
        self._success = success
        self._skip = skip
        self._snapshot_start = snapshot_start
        self._snapshot_end = snapshot_end
        self._exit_code = exit_code
        self._metadata = metadata
        self._run_hostname = run_hostname

        # assigned on first access of corresponding properties:
        self._inputs: ElementInputs | None = None
        self._outputs: ElementOutputs | None = None
        self._resources: ElementResources | None = None
        self._input_files: ElementInputFiles | None = None
        self._output_files: ElementOutputFiles | None = None
        self._ss_start_obj: JSONLikeDirSnapShot | None = None
        self._ss_end_obj: JSONLikeDirSnapShot | None = None
        self._ss_diff_obj: DirectorySnapshotDiff | None = None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"id={self.id_!r}, index={self.index!r}, "
            f"element_action={self.element_action!r})"
        )

    @property
    def id_(self) -> int:
        """
        The ID of the EAR.
        """
        return self._id

    @property
    def is_pending(self) -> bool:
        """
        Whether this EAR is pending.
        """
        return self._is_pending

    @property
    def element_action(self) -> ElementAction:
        """
        The particular element action that this is a run of.
        """
        return self._element_action

    @property
    def index(self) -> int:
        """Run index."""
        return self._index

    @property
    def action(self) -> Action:
        """
        The action this is a run of.
        """
        return self.element_action.action

    @property
    def element_iteration(self) -> ElementIteration:
        """
        The iteration information of this run.
        """
        return self.element_action.element_iteration

    @property
    def element(self) -> Element:
        """
        The element this is a run of.
        """
        return self.element_iteration.element

    @property
    def workflow(self) -> Workflow:
        """
        The workflow this is a run of.
        """
        return self.element_iteration.workflow

    @property
    def data_idx(self) -> DataIndex:
        """
        Used for looking up input data to the EAR.
        """
        return self._data_idx

    @property
    def commands_idx(self) -> list[int]:
        """
        Indices of commands to apply.
        """
        return self._commands_idx

    @property
    def metadata(self) -> dict[str, Any]:
        """
        Metadata about the EAR.
        """
        return self._metadata

    @property
    def run_hostname(self) -> str | None:
        """
        Where to run the EAR, if known/specified.
        """
        return self._run_hostname

    @property
    def start_time(self) -> datetime | None:
        """
        When the EAR started.
        """
        return self._start_time

    @property
    def end_time(self) -> datetime | None:
        """
        When the EAR finished.
        """
        return self._end_time

    @property
    def submission_idx(self) -> int | None:
        """
        What actual submission index was this?
        """
        return self._submission_idx

    @property
    def success(self) -> bool | None:
        """
        Did the EAR succeed?
        """
        return self._success

    @property
    def skip(self) -> bool:
        """
        Was the EAR skipped?
        """
        return self._skip

    @property
    def snapshot_start(self) -> JSONLikeDirSnapShot | None:
        """
        The snapshot of the data directory at the start of the run.
        """
        if self._ss_start_obj is None and self._snapshot_start:
            self._ss_start_obj = JSONLikeDirSnapShot(
                root_path=".",
                **self._snapshot_start,
            )
        return self._ss_start_obj

    @property
    def snapshot_end(self) -> JSONLikeDirSnapShot | None:
        """
        The snapshot of the data directory at the end of the run.
        """
        if self._ss_end_obj is None and self._snapshot_end:
            self._ss_end_obj = JSONLikeDirSnapShot(root_path=".", **self._snapshot_end)
        return self._ss_end_obj

    @property
    def dir_diff(self) -> DirectorySnapshotDiff | None:
        """
        The changes to the EAR working directory due to the execution of this EAR.
        """
        ss = self.snapshot_start
        se = self.snapshot_end
        if self._ss_diff_obj is None and ss and se:
            self._ss_diff_obj = DirectorySnapshotDiff(ss, se)
        return self._ss_diff_obj

    @property
    def exit_code(self) -> int | None:
        """
        The exit code of the underlying program run by the EAR, if known.
        """
        return self._exit_code

    @property
    def task(self) -> WorkflowTask:
        """
        The task that this EAR is part of the implementation of.
        """
        return self.element_action.task

    @property
    def status(self) -> EARStatus:
        """
        The state of this EAR.
        """

        if self.skip:
            return EARStatus.skipped

        elif self.end_time is not None:
            if self.exit_code == 0:
                return EARStatus.success
            elif self.action.abortable and self.exit_code == ABORT_EXIT_CODE:
                return EARStatus.aborted
            else:
                return EARStatus.error

        elif self.start_time is not None:
            return EARStatus.running

        elif self.submission_idx is not None:
            wk_sub_stat = self.workflow.submissions[self.submission_idx].status

            if wk_sub_stat.name == "PENDING":
                return EARStatus.prepared

            elif wk_sub_stat.name == "SUBMITTED":
                return EARStatus.submitted

            else:
                RuntimeError(f"Workflow submission status not understood: {wk_sub_stat}.")

        return EARStatus.pending

    def get_parameter_names(self, prefix: str) -> list[str]:
        """Get parameter types associated with a given prefix.

        For inputs, labels are ignored. See `Action.get_parameter_names` for more
        information.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs", "input_files", "output_files".

        """
        return self.action.get_parameter_names(prefix)

    def get_data_idx(self, path: str | None = None) -> DataIndex:
        """
        Get the data index of a value in the most recent iteration.

        Parameters
        ----------
        path:
            Path to the parameter.
        """
        return self.element_iteration.get_data_idx(
            path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
        )

    @overload
    def get_parameter_sources(
        self,
        *,
        path: str | None = None,
        typ: str | None = None,
        as_strings: Literal[False] = False,
        use_task_index: bool = False,
    ) -> dict[str, dict[str, Any]]:
        ...

    @overload
    def get_parameter_sources(
        self,
        *,
        path: str | None = None,
        typ: str | None = None,
        as_strings: Literal[True],
        use_task_index: bool = False,
    ) -> dict[str, str]:
        ...

    @TimeIt.decorator
    def get_parameter_sources(
        self,
        *,
        path: str | None = None,
        typ: str | None = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ):
        """
        Get the source or sources of a parameter in the most recent iteration.

        Parameters
        ----------
        path:
            Path to the parameter.
        typ:
            The parameter type.
        as_strings:
            Whether to return the result as human-readable strings.
        use_task_index:
            Whether to use the task index.
        """
        if as_strings:
            return self.element_iteration.get_parameter_sources(
                path,
                action_idx=self.element_action.action_idx,
                run_idx=self.index,
                typ=typ,
                as_strings=True,
                use_task_index=use_task_index,
            )
        return self.element_iteration.get_parameter_sources(
            path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            typ=typ,
            as_strings=False,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str | None = None,
        default: Any | None = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ) -> Any:
        """
        Get a value (parameter, input, output, etc.) from the most recent iteration.

        Parameters
        ----------
        path:
            Path to the value.
        default:
            Default value to provide if value absent.
        raise_on_missing:
            Whether to raise an exception on an absent value.
            If not, the default is returned.
        raise_on_unset:
            Whether to raise an exception on an explicitly unset value.
            If not, the default is returned.
        """
        return self.element_iteration.get(
            path=path,
            action_idx=self.element_action.action_idx,
            run_idx=self.index,
            default=default,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
        )

    @overload
    def get_EAR_dependencies(self, as_objects: Literal[False] = False) -> list[int]:
        ...

    @overload
    def get_EAR_dependencies(self, as_objects: Literal[True]) -> list[ElementActionRun]:
        ...

    @TimeIt.decorator
    def get_EAR_dependencies(
        self, as_objects=False
    ) -> list[ElementActionRun] | list[int]:
        """Get EARs that this EAR depends on."""

        out: list[int] = []
        for src in self.get_parameter_sources(typ="EAR_output").values():
            for src_i in src if isinstance(src, list) else [src]:
                EAR_ID_i: int = src_i["EAR_ID"]
                if EAR_ID_i != self.id_:
                    # don't record a self dependency!
                    out.append(EAR_ID_i)

        out = sorted(out)

        if as_objects:
            return self.workflow.get_EARs_from_IDs(out)

        return out

    def get_input_dependencies(self) -> dict[str, dict[str, Any]]:
        """Get information about locally defined input, sequence, and schema-default
        values that this EAR depends on. Note this does not get values from this EAR's
        task/schema, because the aim of this method is to help determine which upstream
        tasks this EAR depends on."""

        out: dict[str, dict[str, Any]] = {}
        wanted_types = ("local_input", "default_input")
        for k, v in self.get_parameter_sources().items():
            for v_i in v if isinstance(v, list) else [v]:
                if (
                    v_i["type"] in wanted_types
                    and v_i["task_insert_ID"] != self.task.insert_ID
                ):
                    out[k] = v_i

        return out

    @overload
    def get_dependent_EARs(self, as_objects: Literal[False] = False) -> list[int]:
        ...

    @overload
    def get_dependent_EARs(self, as_objects: Literal[True]) -> list[ElementActionRun]:
        ...

    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> list[ElementActionRun] | list[int]:
        """Get downstream EARs that depend on this EAR."""
        deps: list[int] = []
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
            return self.workflow.get_EARs_from_IDs(deps)

        return deps

    @property
    def inputs(self) -> ElementInputs:
        """
        The inputs to this EAR.
        """
        if not self._inputs:
            self._inputs = self._app.ElementInputs(element_action_run=self)
        return self._inputs

    @property
    def outputs(self) -> ElementOutputs:
        """
        The outputs from this EAR.
        """
        if not self._outputs:
            self._outputs = self._app.ElementOutputs(element_action_run=self)
        return self._outputs

    @property
    @TimeIt.decorator
    def resources(self) -> ElementResources:
        """
        The resources to use with (or used by) this EAR.
        """
        if not self._resources:
            self._resources = self._app.ElementResources(**self.get_resources())
        return self._resources

    @property
    def input_files(self) -> ElementInputFiles:
        """
        The input files to the controlled program.
        """
        if not self._input_files:
            self._input_files = self._app.ElementInputFiles(element_action_run=self)
        return self._input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        """
        The output files from the controlled program.
        """
        if not self._output_files:
            self._output_files = self._app.ElementOutputFiles(element_action_run=self)
        return self._output_files

    @property
    def env_spec(self) -> dict[str, Any]:
        """
        Environment details.
        """
        envs = self.resources.environments
        if envs is None:
            return {}
        return envs[self.action.get_environment_name()]

    @TimeIt.decorator
    def get_resources(self) -> Mapping[str, Any]:
        """Resolve specific resources for this EAR, considering all applicable scopes and
        template-level resources."""
        return self.element_iteration.get_resources(self.action)

    def get_environment_spec(self) -> dict[str, Any]:
        """
        What environment to run in?
        """
        return self.action.get_environment_spec()

    def get_environment(self) -> Environment:
        """
        What environment to run in?
        """
        return self.action.get_environment()

    def get_all_previous_iteration_runs(
        self, include_self: bool = True
    ) -> list[ElementActionRun]:
        """Get a list of run over all iterations that correspond to this run, optionally
        including this run."""
        self_iter = self.element_iteration
        self_elem = self_iter.element
        self_act_idx = self.element_action.action_idx
        max_idx = self_iter.index + 1 if include_self else self_iter.index
        all_runs: list[ElementActionRun] = []
        for iter_i in self_elem.iterations[:max_idx]:
            all_runs.append(iter_i.actions[self_act_idx].runs[-1])
        return all_runs

    def get_input_values(
        self,
        inputs: Sequence[str] | dict[str, dict] | None = None,
        label_dict: bool = True,
    ) -> dict[str, Any]:
        """Get a dict of (optionally a subset of) inputs values for this run.

        Parameters
        ----------
        inputs:
            If specified, a list of input parameter types to include, or a dict whose keys
            are input parameter types to include. For schema inputs that have
            `multiple=True`, the input type should be labelled. If a dict is passed, and
            the key "all_iterations` is present and `True`, the return for that input
            will be structured to include values for all previous iterations.
        label_dict:
            If True, arrange the values of schema inputs with multiple=True as a dict
            whose keys are the labels. If False, labels will be included in the top level
            keys.

        """
        if not inputs:
            inputs = self.get_parameter_names("inputs")

        out: dict[str, dict[str, Any]] = {}
        for inp_name in inputs:
            path_i, label_i = split_param_label(inp_name)

            try:
                all_iters = (
                    isinstance(inputs, dict) and inputs[inp_name]["all_iterations"]
                )
            except (TypeError, KeyError):
                all_iters = False

            if all_iters:
                all_runs = self.get_all_previous_iteration_runs(include_self=True)
                val_i = {
                    f"iteration_{run_i.element_iteration.index}": {
                        "loop_idx": run_i.element_iteration.loop_idx,
                        "value": run_i.get(f"inputs.{inp_name}"),
                    }
                    for run_i in all_runs
                }
            else:
                val_i = self.get(f"inputs.{inp_name}")

            key = inp_name
            if label_dict and label_i and path_i:
                key = path_i  # exclude label from key

            if "." in key:
                # for sub-parameters, take only the final part as the dict key:
                key = key.split(".")[-1]

            if label_dict and label_i:
                out.setdefault(key, {})[label_i] = val_i
            else:
                out[key] = val_i

        if self.action.script_pass_env_spec:
            out["env_spec"] = self.env_spec

        return out

    def get_input_values_direct(self, label_dict: bool = True) -> dict[str, Any]:
        """Get a dict of input values that are to be passed directly to a Python script
        function."""
        inputs = self.action.script_data_in_grouped.get("direct", {})
        return self.get_input_values(inputs=inputs, label_dict=label_dict)

    def get_IFG_input_values(self) -> dict[str, Any]:
        """
        Get a dict of input values that are to be passed via an input file generator.
        """
        if not self.action._from_expand:
            raise RuntimeError(
                "Cannot get input file generator inputs from this EAR because the "
                "associated action is not expanded, meaning multiple IFGs might exists."
            )
        input_types = [i.typ for i in self.action.input_file_generators[0].inputs]
        inputs: dict[str, Any] = {}
        for i in self.inputs:
            assert not isinstance(i, dict)
            typ = i.path[len("inputs.") :]
            if typ in input_types:
                inputs[typ] = i.value

        if self.action.script_pass_env_spec:
            inputs["env_spec"] = self.env_spec

        return inputs

    def get_OFP_output_files(self) -> dict[str, Path]:
        """
        Get a dict of output files that are going to be parsed to generate one or more
        outputs.
        """
        # TODO: can this return multiple files for a given FileSpec?
        if not self.action._from_expand:
            raise RuntimeError(
                "Cannot get output file parser files from this from EAR because the "
                "associated action is not expanded, meaning multiple OFPs might exist."
            )
        out_files: dict[str, Path] = {}
        for file_spec in self.action.output_file_parsers[0].output_files:
            name = file_spec.name.value()
            assert isinstance(name, str)
            out_files[file_spec.label] = Path(name)
        return out_files

    def get_OFP_inputs(self) -> dict[str, str | list[str] | dict[str, Any]]:
        """
        Get a dict of input values that are to be passed to output file parsers.
        """
        if not self.action._from_expand:
            raise RuntimeError(
                "Cannot get output file parser inputs from this from EAR because the "
                "associated action is not expanded, meaning multiple OFPs might exist."
            )
        inputs: dict[str, str | list[str] | dict[str, Any]] = {}
        for inp_typ in self.action.output_file_parsers[0].inputs or []:
            inputs[inp_typ] = self.get(f"inputs.{inp_typ}")

        if self.action.script_pass_env_spec:
            inputs["env_spec"] = self.env_spec

        return inputs

    def get_OFP_outputs(self) -> dict[str, str | list[str]]:
        """
        Get the outputs obtained by parsing an output file.
        """
        if not self.action._from_expand:
            raise RuntimeError(
                "Cannot get output file parser outputs from this from EAR because the "
                "associated action is not expanded, meaning multiple OFPs might exist."
            )
        outputs = {}
        for out_typ in self.action.output_file_parsers[0].outputs or []:
            outputs[out_typ] = self.get(f"outputs.{out_typ}")
        return outputs

    @staticmethod
    def __cast_param_value(v: Any) -> ParameterValue:
        # UGLY but could be worse
        return v

    def write_source(self, js_idx: int, js_act_idx: int) -> None:
        """
        Write values to files in standard formats.
        """
        import h5py  # type: ignore

        for fmt, ins in self.action.script_data_in_grouped.items():
            if fmt == "json":
                in_vals = self.get_input_values(inputs=ins, label_dict=False)
                dump_path = self.action.get_param_dump_file_path_JSON(js_idx, js_act_idx)
                in_vals_processed: dict[str, Any] = {}
                for k, v in in_vals.items():
                    try:
                        v = self.__cast_param_value(v).prepare_JSON_dump()
                    except (AttributeError, NotImplementedError):
                        pass
                    in_vals_processed[k] = v

                with dump_path.open("wt") as fp:
                    json.dump(in_vals_processed, fp)

            elif fmt == "hdf5":
                in_vals = self.get_input_values(inputs=ins, label_dict=False)
                dump_path = self.action.get_param_dump_file_path_HDF5(js_idx, js_act_idx)
                with h5py.File(dump_path, mode="w") as f:
                    for k, v in in_vals.items():
                        self.__cast_param_value(v).dump_to_HDF5_group(f.create_group(k))

        # write the script if it is specified as a app data script, otherwise we assume
        # the script already exists in the working directory:
        snip_path = self.action.get_snippet_script_path(self.action.script, self.env_spec)
        if snip_path:
            script_name = snip_path.name
            source_str = self.action.compose_source(snip_path)
            with Path(script_name).open("wt", newline="\n") as fp:
                fp.write(source_str)

    def _param_save(self, js_idx: int, js_act_idx: int):
        """Save script-generated parameters that are stored within the supported script
        data output formats (HDF5, JSON, etc)."""
        import h5py

        parameters: ParametersList = self._app.parameters

        for fmt in self.action.script_data_out_grouped:
            if fmt == "json":
                load_path = self.action.get_param_load_file_path_JSON(js_idx, js_act_idx)
                with load_path.open(mode="rt") as f:
                    file_data = json.load(f)
                    for param_name, param_dat in file_data.items():
                        param_id = cast(int, self.data_idx[f"outputs.{param_name}"])
                        param_cls = parameters.get(param_name)._force_value_class()
                        if param_cls is not None:
                            param_cls.save_from_JSON(param_dat, param_id, self.workflow)
                            continue
                        # try to save as a primitive:
                        self.workflow.set_parameter_value(
                            param_id=param_id, value=param_dat
                        )

            elif fmt == "hdf5":
                load_path = self.action.get_param_load_file_path_HDF5(js_idx, js_act_idx)
                with h5py.File(load_path, mode="r") as f:
                    for param_name, h5_grp in f.items():
                        param_id = cast(int, self.data_idx[f"outputs.{param_name}"])
                        param_cls = parameters.get(param_name)._force_value_class()
                        if param_cls is not None:
                            param_cls.save_from_HDF5_group(
                                h5_grp, param_id, self.workflow
                            )
                        else:
                            # Unlike with JSON, we've no fallback so we warn
                            self._app.logger.warn(
                                "parameter %s could not be saved; serializer not found",
                                param_name,
                            )

    def compose_commands(
        self, jobscript: Jobscript, JS_action_idx: int
    ) -> tuple[str, dict[int, list[tuple[str, ...]]]]:
        """
        Write the EAR's enactment to disk in preparation for submission.

        Returns
        -------
        commands:
            List of argument words for the command that enacts the EAR.
        shell_vars:
            Dict whose keys are command indices, and whose values are lists of tuples,
            where each tuple contains: (parameter name, shell variable name,
            "stdout"/"stderr").
        """
        self._app.persistence_logger.debug("EAR.compose_commands")
        env_spec = self.env_spec

        for ifg in self.action.input_file_generators:
            # TODO: there should only be one at this stage if expanded?
            ifg.write_source(self.action, env_spec)

        for ofp in self.action.output_file_parsers:
            # TODO: there should only be one at this stage if expanded?
            if ofp.output is None:
                raise OutputFileParserNoOutputError()
            ofp.write_source(self.action, env_spec)

        if self.action.script:
            self.write_source(js_idx=jobscript.index, js_act_idx=JS_action_idx)

        command_lns = []
        env = jobscript.submission.environments.get(**env_spec)
        if env.setup:
            command_lns += list(env.setup)

        shell_vars: dict[
            int, list[tuple[str, ...]]
        ] = {}  # keys are cmd_idx, each value is a list of tuples
        for cmd_idx, command in enumerate(self.action.commands):
            if cmd_idx in self.commands_idx:
                # only execute commands that have no rules, or all valid rules:
                cmd_str, shell_vars_i = command.get_command_line(
                    EAR=self, shell=jobscript.shell, env=env
                )
                shell_vars[cmd_idx] = shell_vars_i
                command_lns.append(cmd_str)

        commands = "\n".join(command_lns) + "\n"

        return commands, shell_vars


class ElementAction(AppAware):
    """
    An abstract representation of an element's action at a particular iteration and
    the runs that enact that element iteration.

    Parameters
    ----------
    element_iteration:
        The iteration
    action_idx:
        The action index.
    runs:
        The list of run indices.
    """

    def __init__(
        self,
        element_iteration: ElementIteration,
        action_idx: int,
        runs: dict[Mapping[str, Any], Any],
    ):
        self._element_iteration = element_iteration
        self._action_idx = action_idx
        self._runs = runs

        # assigned on first access of corresponding properties:
        self._run_objs: list[ElementActionRun] | None = None
        self._inputs: ElementInputs | None = None
        self._outputs: ElementOutputs | None = None
        self._resources: ElementResources | None = None
        self._input_files: ElementInputFiles | None = None
        self._output_files: ElementOutputFiles | None = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"iter_ID={self.element_iteration.id_}, "
            f"scope={self.action.get_precise_scope().to_string()!r}, "
            f"action_idx={self.action_idx}, num_runs={self.num_runs}"
            f")"
        )

    @property
    def element_iteration(self) -> ElementIteration:
        """
        The iteration for this action.
        """
        return self._element_iteration

    @property
    def element(self) -> Element:
        """
        The element for this action.
        """
        return self.element_iteration.element

    @property
    def num_runs(self) -> int:
        """
        The number of runs associated with this action.
        """
        return len(self._runs)

    @property
    def runs(self) -> list[ElementActionRun]:
        """
        The EARs that this action is enacted by.
        """
        if self._run_objs is None:
            self._run_objs = [
                self._app.ElementActionRun(
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
    def task(self) -> WorkflowTask:
        """
        The task that this action is an instance of.
        """
        return self.element_iteration.task

    @property
    def action_idx(self) -> int:
        """
        The index of the action.
        """
        return self._action_idx

    @property
    def action(self) -> Action:
        """
        The abstract task that this is a concrete model of.
        """
        return self.task.template.get_schema_action(self.action_idx)

    @property
    def inputs(self) -> ElementInputs:
        """
        The inputs to this action.
        """
        if not self._inputs:
            self._inputs = self._app.ElementInputs(element_action=self)
        return self._inputs

    @property
    def outputs(self) -> ElementOutputs:
        """
        The outputs from this action.
        """
        if not self._outputs:
            self._outputs = self._app.ElementOutputs(element_action=self)
        return self._outputs

    @property
    def input_files(self) -> ElementInputFiles:
        """
        The input files to this action.
        """
        if not self._input_files:
            self._input_files = self._app.ElementInputFiles(element_action=self)
        return self._input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        """
        The output files from this action.
        """
        if not self._output_files:
            self._output_files = self._app.ElementOutputFiles(element_action=self)
        return self._output_files

    def get_data_idx(self, path: str | None = None, run_idx: int = -1) -> DataIndex:
        """
        Get the data index for some path/run.
        """
        return self.element_iteration.get_data_idx(
            path,
            action_idx=self.action_idx,
            run_idx=run_idx,
        )

    @overload
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[False] = False,
        use_task_index: bool = False,
    ) -> dict[str, dict[str, Any]]:
        ...

    @overload
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[True],
        use_task_index: bool = False,
    ) -> dict[str, str]:
        ...

    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ):
        """
        Get information about where parameters originated.
        """
        if as_strings:
            return self.element_iteration.get_parameter_sources(
                path,
                action_idx=self.action_idx,
                run_idx=run_idx,
                typ=typ,
                as_strings=True,
                use_task_index=use_task_index,
            )
        return self.element_iteration.get_parameter_sources(
            path,
            action_idx=self.action_idx,
            run_idx=run_idx,
            typ=typ,
            as_strings=False,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str | None = None,
        run_idx: int = -1,
        default: Any | None = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ):
        """
        Get the value of a parameter.
        """
        return self.element_iteration.get(
            path=path,
            action_idx=self.action_idx,
            run_idx=run_idx,
            default=default,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
        )

    def get_parameter_names(self, prefix: str) -> list[str]:
        """Get parameter types associated with a given prefix.

        For inputs, labels are ignored.
        See :py:meth:`.Action.get_parameter_names` for more information.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs", "input_files", "output_files".

        """
        return self.action.get_parameter_names(prefix)


@final
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

    def __init__(self, typ: ActionScopeType | str, **kwargs):
        if isinstance(typ, str):
            #: Action scope type.
            self.typ = self._app.ActionScopeType[typ.upper()]
        else:
            self.typ = typ

        #: Any provided extra keyword arguments.
        self.kwargs = {k: v for k, v in kwargs.items() if v is not None}

        bad_keys = set(kwargs.keys()) - ACTION_SCOPE_ALLOWED_KWARGS[self.typ.name]
        if bad_keys:
            raise TypeError(
                f"The following keyword arguments are unknown for ActionScopeType "
                f"{self.typ.name}: {bad_keys}."
            )

    def __repr__(self) -> str:
        kwargs_str = ""
        if self.kwargs:
            kwargs_str = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        return f"{self.__class__.__name__}.{self.typ.name.lower()}({kwargs_str})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ActionScope):
            return False
        if self.typ is other.typ and self.kwargs == other.kwargs:
            return True
        return False

    class _customdict(dict):
        pass

    @classmethod
    def _parse_from_string(cls, string: str) -> dict[str, str]:
        match = re.search(ACTION_SCOPE_REGEX, string)
        if not match:
            raise TypeError(f"unparseable ActionScope: '{string}'")
        typ_str, kwargs_str = match.groups()
        # The types of the above two variables are idiotic, but bug reports to fix it
        # get closed because "it would break existing code that makes dumb assumptions"
        kwargs: dict[str, str] = cls._customdict({"type": cast(str, typ_str)})
        if kwargs_str:
            for i in kwargs_str.split(","):
                name, val = i.split("=")
                kwargs[name.strip()] = val.strip()
        return kwargs

    def to_string(self) -> str:
        """
        Render this action scope as a string.
        """
        kwargs_str = ""
        if self.kwargs:
            kwargs_str = "[" + ", ".join(f"{k}={v}" for k, v in self.kwargs.items()) + "]"
        return f"{self.typ.name.lower()}{kwargs_str}"

    @classmethod
    def _from_json_like(
        cls,
        json_like: Mapping[str, Any] | Sequence[Mapping[str, Any]],
        shared_data: Mapping[str, Any],
    ) -> Self:
        if not isinstance(json_like, Mapping):
            raise TypeError("only mappings are supported for becoming an ActionScope")
        if not isinstance(json_like, cls._customdict):
            json_like = {"type": json_like["type"], **json_like.get("kwargs", {})}
        return super()._from_json_like(json_like, shared_data)

    @classmethod
    def any(cls) -> ActionScope:
        """
        Any scope.
        """
        return cls(typ=ActionScopeType.ANY)

    @classmethod
    def main(cls) -> ActionScope:
        """
        The main scope.
        """
        return cls(typ=ActionScopeType.MAIN)

    @classmethod
    def processing(cls) -> ActionScope:
        """
        The processing scope.
        """
        return cls(typ=ActionScopeType.PROCESSING)

    @classmethod
    def input_file_generator(cls, file: str | None = None) -> ActionScope:
        """
        The scope of an input file generator.
        """
        return cls(typ=ActionScopeType.INPUT_FILE_GENERATOR, file=file)

    @classmethod
    def output_file_parser(cls, output: Parameter | str | None = None) -> ActionScope:
        """
        The scope of an output file parser.
        """
        return cls(typ=ActionScopeType.OUTPUT_FILE_PARSER, output=output)


@dataclass()
@hydrate
class ActionEnvironment(JSONLike):
    """
    The environment that an action is enacted within.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(
            name="scope",
            class_name="ActionScope",
        ),
    )

    #: The environment document.
    environment: dict[str, Any]
    #: The scope.
    scope: ActionScope

    def __init__(
        self, environment: str | dict[str, Any], scope: ActionScope | None = None
    ):
        if scope is None:
            self.scope = self._app.ActionScope.any()
        else:
            self.scope = scope

        if isinstance(environment, str):
            self.environment = {"name": environment}
        else:
            if "name" not in environment:
                raise ActionEnvironmentMissingNameError(
                    "The action-environment environment specification must include a string "
                    "`name` key, or be specified as string that is that name. Provided "
                    f"environment key was {environment!r}."
                )
            self.environment = copy.deepcopy(environment)


class ActionRule(JSONLike):
    """
    Class to represent a rule/condition that must be True if an action is to be
    included.

    Parameters
    ----------
    rule: ~hpcflow.app.Rule
        The rule to apply.
    check_exists: str
        A special rule that is enabled if this named attribute is present.
    check_missing: str
        A special rule that is enabled if this named attribute is absent.
    path: str
        Where to find the attribute to check.
    condition: dict | ConditionLike
        A more complex condition to apply.
    cast: str
        The name of a class to cast the attribute to before checking.
    doc: str
        Documentation for this rule, if any.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
        ChildObjectSpec(name="rule", class_name="Rule"),
    )

    def __init__(
        self,
        rule: Rule | None = None,
        check_exists: str | None = None,
        check_missing: str | None = None,
        path: str | None = None,
        condition: dict[str, Any] | ConditionLike | None = None,
        cast: str | None = None,
        doc: str | None = None,
    ):
        if rule is None:
            #: The rule to apply.
            self.rule = self._app.Rule(
                check_exists=check_exists,
                check_missing=check_missing,
                path=path,
                condition=condition,
                cast=cast,
                doc=doc,
            )
        elif any(
            i is not None
            for i in (check_exists, check_missing, path, condition, cast, doc)
        ):
            raise TypeError(
                f"{self.__class__.__name__} `rule` specified in addition to rule "
                f"constructor arguments."
            )
        else:
            self.rule = rule

        #: The action that contains this rule.
        self.action: Action | None = None  # assigned by parent action
        #: The command that is guarded by this rule.
        self.command: Command | None = None  # assigned by parent command

    def __eq__(self, other: Any) -> bool:
        if type(other) is not self.__class__:
            return False
        return self.rule == other.rule

    @TimeIt.decorator
    def test(self, element_iteration: ElementIteration) -> bool:
        """
        Test if this rule holds for a particular iteration.

        Parameter
        ---------
        element_iteration:
            The iteration to apply this rule to.
        """
        return self.rule.test(element_like=element_iteration, action=self.action)

    @classmethod
    def check_exists(cls, check_exists: str) -> ActionRule:
        """
        Make an action rule that checks if a named attribute is present.

        Parameter
        ---------
        check_exists:
            The path to the attribute to check for.
        """
        return cls(rule=cls._app.Rule(check_exists=check_exists))

    @classmethod
    def check_missing(cls, check_missing: str) -> ActionRule:
        """
        Make an action rule that checks if a named attribute is absent.

        Parameter
        ---------
        check_missing:
            The path to the attribute to check for.
        """
        return cls(rule=cls._app.Rule(check_missing=check_missing))


class Action(JSONLike):
    """
    An atomic component of a workflow that will be enacted within an iteration
    structure.

    Parameters
    ----------
    environments: list[ActionEnvironment]
        The environments in which this action can run.
    commands: list[~hpcflow.app.Command]
        The commands to be run by this action.
    script: str
        The name of the Python script to run.
    script_data_in: str
        Information about data input to the script.
    script_data_out: str
        Information about data output from the script.
    script_data_files_use_opt: bool
        If True, script data input and output file paths will be passed to the script
        execution command line with an option like ``--input-json`` or ``--output-hdf5``
        etc. If False, the file paths will be passed on their own. For Python scripts,
        options are always passed, and this parameter is overwritten to be True,
        regardless of its initial value.
    script_exe: str
        The executable to use to run the script.
    script_pass_env_spec: bool
        Whether to pass the environment details to the script.
    abortable: bool
        Whether this action can be aborted.
    input_file_generators: list[~hpcflow.app.InputFileGenerator]
        Any applicable input file generators.
    output_file_parsers: list[~hpcflow.app.OutputFileParser]
        Any applicable output file parsers.
    input_files: list[~hpcflow.app.FileSpec]
        The input files to the action's commands.
    output_files: list[~hpcflow.app.FileSpec]
        The output files from the action's commands.
    rules: list[ActionRule]
        How to determine whether to run the action.
    save_files: list[str]
        The names of files to be explicitly saved after each step.
    clean_up: list[str]
        The names of files to be deleted after each step.
    """

    _child_objects: ClassVar[tuple[ChildObjectSpec, ...]] = (
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
            parent_ref="action",
        ),
        ChildObjectSpec(
            name="save_files",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="clean_up",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
    )
    _script_data_formats: ClassVar[tuple[str, ...]] = ("direct", "json", "hdf5")

    def __init__(
        self,
        environments: list[ActionEnvironment] | None = None,
        commands: list[Command] | None = None,
        script: str | None = None,
        script_data_in: str | Mapping[str, str | ScriptData] | None = None,
        script_data_out: str | Mapping[str, str | ScriptData] | None = None,
        script_data_files_use_opt: bool = False,
        script_exe: str | None = None,
        script_pass_env_spec: bool = False,
        abortable: bool = False,
        input_file_generators: list[InputFileGenerator] | None = None,
        output_file_parsers: list[OutputFileParser] | None = None,
        input_files: list[FileSpec] | None = None,
        output_files: list[FileSpec] | None = None,
        rules: list[ActionRule] | None = None,
        save_files: list[FileSpec] | None = None,
        clean_up: list[str] | None = None,
    ):
        #: The commands to be run by this action.
        self.commands = commands or []
        #: The name of the Python script to run.
        self.script = script
        #: Information about data input to the script.
        self.script_data_in: dict[str, ScriptData] | None = None
        self._script_data_in = script_data_in
        #: Information about data output from the script.
        self.script_data_out: dict[str, ScriptData] | None = None
        self._script_data_out = script_data_out
        #: If True, script data input and output file paths will be passed to the script
        #: execution command line with an option like `--input-json` or `--output-hdf5`
        #: etc. If False, the file paths will be passed on their own. For Python scripts,
        #: options are always passed, and this parameter is overwritten to be True,
        #: regardless of its initial value.
        self.script_data_files_use_opt = (
            script_data_files_use_opt if not self.script_is_python else True
        )
        #: The executable to use to run the script.
        self.script_exe = script_exe.lower() if script_exe else None
        #: Whether to pass the environment details to the script.
        self.script_pass_env_spec = script_pass_env_spec
        #: The environments in which this action can run.
        self.environments = environments or [
            self._app.ActionEnvironment(environment="null_env")
        ]
        #: Whether this action can be aborted.
        self.abortable = abortable
        #: Any applicable input file generators.
        self.input_file_generators = input_file_generators or []
        #: Any applicable output file parsers.
        self.output_file_parsers = output_file_parsers or []
        #: The input files to the action's commands.
        self.input_files = self._resolve_input_files(input_files or [])
        #: The output files from the action's commands.
        self.output_files = self._resolve_output_files(output_files or [])
        #: How to determine whether to run the action.
        self.rules = rules or []
        #: The names of files to be explicitly saved after each step.
        self.save_files = save_files or []
        #: The names of files to be deleted after each step.
        self.clean_up = clean_up or []

        self._task_schema: TaskSchema | None = None  # assigned by parent TaskSchema
        self._from_expand = False  # assigned on creation of new Action by `expand`

        self._set_parent_refs()

    def process_script_data_formats(self) -> None:
        """
        Convert script data information into standard form.
        """
        self.script_data_in = self.__process_script_data(self._script_data_in, "inputs")
        self.script_data_out = self.__process_script_data(
            self._script_data_out, "outputs"
        )

    def __process_script_data_str(
        self, data_fmt: str, param_names: list[str]
    ) -> dict[str, ScriptData]:
        # include all input parameters, using specified data format
        data_fmt = data_fmt.lower()
        return {k: {"format": data_fmt} for k in param_names}

    def __process_script_data_dict(
        self,
        data_fmt: Mapping[str, str | ScriptData],
        prefix: str,
        param_names: list[str],
    ) -> dict[str, ScriptData]:
        _all_other_sym = "*"
        all_params: dict[str, ScriptData] = {}
        for k, v in data_fmt.items():
            # values might be strings, or dicts with "format" and potentially other
            # kwargs:
            if isinstance(v, dict):
                # Make sure format is first key
                v2: ScriptData = {
                    "format": v["format"],
                }
                all_params[k] = v2
                v2.update(v)
            else:
                all_params[k] = {"format": v.lower()}

        if prefix == "inputs":
            # expand unlabelled-multiple inputs to multiple labelled inputs:
            multi_types = self.task_schema.multi_input_types
            multis: dict[str, ScriptData] = {}
            for k in list(all_params.keys()):
                if k in multi_types:
                    k_fmt = all_params.pop(k)
                    for i in param_names:
                        if i.startswith(k):
                            multis[i] = copy.deepcopy(k_fmt)
            if multis:
                all_params = {
                    **multis,
                    **all_params,
                }

        if _all_other_sym in all_params:
            # replace catch-all with all other input/output names:
            other_fmt = all_params[_all_other_sym]
            all_params = {k: v for k, v in all_params.items() if k != _all_other_sym}
            other = set(param_names) - set(all_params.keys())
            for i in other:
                all_params[i] = copy.deepcopy(other_fmt)
        return all_params

    def __process_script_data(
        self, data_fmt: str | Mapping[str, str | ScriptData] | None, prefix: str
    ) -> dict[str, ScriptData]:
        if not data_fmt:
            return {}

        param_names = self.get_parameter_names(prefix)
        if isinstance(data_fmt, str):
            all_params = self.__process_script_data_str(data_fmt, param_names)
        else:
            all_params = self.__process_script_data_dict(data_fmt, prefix, param_names)

        # validation:
        allowed_keys = ("format", "all_iterations")
        for k, v in all_params.items():
            # validate parameter name (sub-parameters are allowed):
            if k.split(".")[0] not in param_names:
                raise UnknownScriptDataParameter(
                    f"Script data parameter {k!r} is not a known parameter of the "
                    f"action. Parameters ({prefix}) are: {param_names!r}."
                )
            # validate format:
            if v["format"] not in self._script_data_formats:
                raise UnsupportedScriptDataFormat(
                    f"Script data format {v!r} for {prefix[:-1]} parameter {k!r} is not "
                    f"understood. Available script data formats are: "
                    f"{self._script_data_formats!r}."
                )

            for k2 in v:
                if k2 not in allowed_keys:
                    raise UnknownScriptDataKey(
                        f"Script data key {k2!r} is not understood. Allowed keys are: "
                        f"{allowed_keys!r}."
                    )

        return all_params

    @property
    def script_data_in_grouped(self) -> dict[str, dict[str, dict[str, str]]]:
        """Get input parameter types by script data-in format."""
        if self.script_data_in is None:
            self.process_script_data_formats()
            assert self.script_data_in is not None
        return swap_nested_dict_keys(
            dct=cast(dict, self.script_data_in), inner_key="format"
        )

    @property
    def script_data_out_grouped(self) -> dict[str, dict[str, dict[str, str]]]:
        """Get output parameter types by script data-out format."""
        if self.script_data_out is None:
            self.process_script_data_formats()
            assert self.script_data_out is not None
        return swap_nested_dict_keys(
            dct=cast(dict, self.script_data_out), inner_key="format"
        )

    @property
    def script_data_in_has_files(self) -> bool:
        """Return True if the script requires some inputs to be passed via an
        intermediate file format."""
        return bool(set(self.script_data_in_grouped.keys()) - {"direct"})  # TODO: test

    @property
    def script_data_out_has_files(self) -> bool:
        """Return True if the script produces some outputs via an intermediate file
        format."""
        return bool(set(self.script_data_out_grouped.keys()) - {"direct"})  # TODO: test

    @property
    def script_data_in_has_direct(self) -> bool:
        """Return True if the script requires some inputs to be passed directly from the
        app."""
        return "direct" in self.script_data_in_grouped  # TODO: test

    @property
    def script_data_out_has_direct(self) -> bool:
        """Return True if the script produces some outputs to be passed directly to the
        app."""
        return "direct" in self.script_data_out_grouped  # TODO: test

    @property
    def script_is_python(self) -> bool:
        """Return True if the script is a Python script (determined by the file
        extension)"""
        if self.script:
            snip_path = self.get_snippet_script_path(self.script)
            if snip_path:
                return snip_path.suffix == ".py"
        return False

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d["script_data_in"] = d.pop("_script_data_in")
        d["script_data_out"] = d.pop("_script_data_out")
        return d

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        kwargs = self.to_dict()
        _from_expand = kwargs.pop("_from_expand")
        _task_schema = kwargs.pop("_task_schema", None)
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._from_expand = _from_expand
        obj._task_schema = _task_schema
        return obj

    @property
    def task_schema(self) -> TaskSchema:
        """
        The task schema that this action came from.
        """
        assert self._task_schema is not None
        return self._task_schema

    def _resolve_input_files(self, input_files: list[FileSpec]) -> list[FileSpec]:
        in_files = input_files
        for i in self.input_file_generators:
            if i.input_file not in in_files:
                in_files.append(i.input_file)
        return in_files

    def _resolve_output_files(self, output_files: list[FileSpec]) -> list[FileSpec]:
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
        OFPs = {}
        for idx, i in enumerate(self.output_file_parsers):
            if i.output is not None:
                key = i.output.typ
            else:
                key = f"OFP_{idx}"
            OFPs[key] = [j.label for j in i.output_files]

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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Action):
            return False
        return (
            self.commands == other.commands
            and self.script == other.script
            and self.environments == other.environments
            and self.abortable == other.abortable
            and self.input_file_generators == other.input_file_generators
            and self.output_file_parsers == other.output_file_parsers
            and self.rules == other.rules
        )

    @classmethod
    def _json_like_constructor(cls, json_like) -> Self:
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        _from_expand = json_like.pop("_from_expand", None)
        obj = cls(**json_like)
        obj._from_expand = _from_expand
        return obj

    def get_parameter_dependence(self, parameter: SchemaParameter) -> ParameterDependence:
        """Find if/where a given parameter is used by the action."""
        writer_files = [
            i.input_file
            for i in self.input_file_generators
            if parameter.parameter in i.inputs
        ]  # names of input files whose generation requires this parameter
        commands: list[
            int
        ] = []  # TODO: indices of commands in which this parameter appears
        return {"input_file_writers": writer_files, "commands": commands}

    def _get_resolved_action_env(
        self,
        relevant_scopes: tuple[ActionScopeType, ...],
        input_file_generator: InputFileGenerator | None = None,
        output_file_parser: OutputFileParser | None = None,
        commands: list[Command] | None = None,
    ) -> ActionEnvironment:
        possible = [
            i for i in self.environments if i.scope and i.scope.typ in relevant_scopes
        ]
        if not possible:
            if input_file_generator:
                msg = f"input file generator {input_file_generator.input_file.label!r}"
            elif output_file_parser:
                if output_file_parser.output is not None:
                    ofp_id = output_file_parser.output.typ
                else:
                    ofp_id = "<unnamed>"
                msg = f"output file parser {ofp_id!r}"
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
    ) -> ActionEnvironment:
        """
        Get the actual environment to use for an input file generator.
        """
        return self._get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.INPUT_FILE_GENERATOR,
            ),
            input_file_generator=input_file_generator,
        )

    def get_output_file_parser_action_env(
        self, output_file_parser: OutputFileParser
    ) -> ActionEnvironment:
        """
        Get the actual environment to use for an output file parser.
        """
        return self._get_resolved_action_env(
            relevant_scopes=(
                ActionScopeType.ANY,
                ActionScopeType.PROCESSING,
                ActionScopeType.OUTPUT_FILE_PARSER,
            ),
            output_file_parser=output_file_parser,
        )

    def get_commands_action_env(self) -> ActionEnvironment:
        """
        Get the actual environment to use for the action commands.
        """
        return self._get_resolved_action_env(
            relevant_scopes=(ActionScopeType.ANY, ActionScopeType.MAIN),
            commands=self.commands,
        )

    def get_environment_name(self) -> str:
        """
        Get the name of the primary environment.
        """
        return self.get_environment_spec()["name"]

    def get_environment_spec(self) -> dict[str, Any]:
        """
        Get the specification for the primary envionment, assuming it has been expanded.
        """
        if not self._from_expand:
            raise RuntimeError(
                "Cannot choose a single environment from this action because it is not "
                "expanded, meaning multiple action environments might exist."
            )
        return self.environments[0].environment

    def get_environment(self) -> Environment:
        """
        Get the primary environment.
        """
        return self._app.envs.get(**self.get_environment_spec())

    @staticmethod
    def is_snippet_script(script: str | None) -> bool:
        """Returns True if the provided script string represents a script snippets that is
        to be modified before execution (e.g. to receive and provide parameter data)."""
        if script is None:
            return False
        return script.startswith("<<script:")

    _SCRIPT_NAME_RE: ClassVar = re.compile(r"\<\<script:(?:.*(?:\/|\\))*(.*)\>\>")

    @classmethod
    def get_script_name(cls, script: str) -> str:
        """Return the script name."""
        if cls.is_snippet_script(script):
            match_obj = re.match(cls._SCRIPT_NAME_RE, script)
            if not match_obj:
                raise ValueError("incomplete <<script:>>")
            return match_obj.group(1)
        # a script we can expect in the working directory:
        return script

    _SCRIPT_RE: ClassVar = re.compile(r"\<\<script:(.*:?)\>\>")
    _ENV_RE: ClassVar = re.compile(r"\<\<env:(.*?)\>\>")

    @classmethod
    def get_snippet_script_str(
        cls, script, env_spec: dict[str, Any] | None = None
    ) -> str:
        """
        Get the substituted script snippet path as a string.
        """
        if not cls.is_snippet_script(script):
            raise ValueError(
                f"Must be an app-data script name (e.g. "
                f"<<script:path/to/app/data/script.py>>), but received {script}"
            )
        match_obj = re.match(cls._SCRIPT_RE, script)
        if not match_obj:
            raise ValueError("incomplete <<script:>>")
        out = cast(str, match_obj.group(1))

        if env_spec:
            out = re.sub(
                pattern=cls._ENV_RE,
                repl=lambda match_obj: env_spec[match_obj.group(1)],
                string=out,
            )
        return out

    @classmethod
    def get_snippet_script_path(
        cls, script_path: str | None, env_spec: dict[str, Any] | None = None
    ) -> Path | None:
        """
        Get the substituted script snippet path, or False if there is no snippet.
        """
        if not cls.is_snippet_script(script_path):
            return None

        path = cls.get_snippet_script_str(script_path, env_spec)
        return Path(cls._app.scripts.get(path, path))

    @staticmethod
    def __get_param_dump_file_stem(js_idx: int | str, js_act_idx: int | str) -> str:
        return RunDirAppFiles.get_run_param_dump_file_prefix(js_idx, js_act_idx)

    @staticmethod
    def __get_param_load_file_stem(js_idx: int | str, js_act_idx: int | str) -> str:
        return RunDirAppFiles.get_run_param_load_file_prefix(js_idx, js_act_idx)

    def get_param_dump_file_path_JSON(
        self, js_idx: int | str, js_act_idx: int | str
    ) -> Path:
        """
        Get the path of the JSON dump file.
        """
        return Path(self.__get_param_dump_file_stem(js_idx, js_act_idx) + ".json")

    def get_param_dump_file_path_HDF5(
        self, js_idx: int | str, js_act_idx: int | str
    ) -> Path:
        """
        Get the path of the HDF56 dump file.
        """
        return Path(self.__get_param_dump_file_stem(js_idx, js_act_idx) + ".h5")

    def get_param_load_file_path_JSON(
        self, js_idx: int | str, js_act_idx: int | str
    ) -> Path:
        """
        Get the path of the JSON load file.
        """
        return Path(self.__get_param_load_file_stem(js_idx, js_act_idx) + ".json")

    def get_param_load_file_path_HDF5(
        self, js_idx: int | str, js_act_idx: int | str
    ) -> Path:
        """
        Get the path of the HDF5 load file.
        """
        return Path(self.__get_param_load_file_stem(js_idx, js_act_idx) + ".h5")

    def expand(self) -> list[Action]:
        """
        Expand this action into a list of actions if necessary.
        This converts input file generators and output file parsers into their own actions.
        """
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

            main_rules = self.rules + [
                self._app.ActionRule.check_missing(f"output_files.{i.label}")
                for i in self.output_files
            ]

            # note we keep the IFG/OPs in the new actions, so we can check the parameters
            # used/produced.

            inp_files = []
            inp_acts: list[Action] = []
            for ifg in self.input_file_generators:
                exe = "<<executable:python_script>>"
                args = [
                    '"$WK_PATH"',
                    "$EAR_ID",
                ]  # WK_PATH could have a space in it
                if ifg.script:
                    script_name = self.get_script_name(ifg.script)
                    variables = {
                        "script_name": script_name,
                        "script_name_no_ext": str(Path(script_name).stem),
                    }
                else:
                    variables = {}
                act_i = self._app.Action(
                    commands=[
                        self._app.Command(
                            executable=exe, arguments=args, variables=variables
                        )
                    ],
                    input_file_generators=[ifg],
                    environments=[self.get_input_file_generator_action_env(ifg)],
                    rules=main_rules + ifg.get_action_rules(),
                    script_pass_env_spec=ifg.script_pass_env_spec,
                    abortable=ifg.abortable,
                    # TODO: add script_data_in etc? and to OFP?
                )
                act_i._task_schema = self.task_schema
                if ifg.input_file not in inp_files:
                    inp_files.append(ifg.input_file)
                act_i._from_expand = True
                inp_acts.append(act_i)

            out_files = []
            out_acts: list[Action] = []
            for ofp in self.output_file_parsers:
                exe = "<<executable:python_script>>"
                args = [
                    '"$WK_PATH"',
                    "$EAR_ID",
                ]  # WK_PATH could have a space in it
                if ofp.script:
                    script_name = self.get_script_name(ofp.script)
                    variables = {
                        "script_name": script_name,
                        "script_name_no_ext": str(Path(script_name).stem),
                    }
                else:
                    variables = {}
                act_i = self._app.Action(
                    commands=[
                        self._app.Command(
                            executable=exe, arguments=args, variables=variables
                        )
                    ],
                    output_file_parsers=[ofp],
                    environments=[self.get_output_file_parser_action_env(ofp)],
                    rules=list(self.rules) + ofp.get_action_rules(),
                    script_pass_env_spec=ofp.script_pass_env_spec,
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
                exe = f"<<executable:{self.script_exe}>>"
                args = []
                if self.script:
                    script_name = self.get_script_name(self.script)
                    variables = {
                        "script_name": script_name,
                        "script_name_no_ext": str(Path(script_name).stem),
                    }
                else:
                    variables = {}
                if self.script_data_in_has_direct or self.script_data_out_has_direct:
                    # WK_PATH could have a space in it:
                    args.extend(["--wk-path", '"$WK_PATH"', "--run-id", "$EAR_ID"])

                fn_args = {"js_idx": "${JS_IDX}", "js_act_idx": "${JS_act_idx}"}

                for fmt in self.script_data_in_grouped:
                    if fmt == "json":
                        if self.script_data_files_use_opt:
                            args.append("--inputs-json")
                        args.append(str(self.get_param_dump_file_path_JSON(**fn_args)))
                    elif fmt == "hdf5":
                        if self.script_data_files_use_opt:
                            args.append("--inputs-hdf5")
                        args.append(str(self.get_param_dump_file_path_HDF5(**fn_args)))

                for fmt in self.script_data_out_grouped:
                    if fmt == "json":
                        if self.script_data_files_use_opt:
                            args.append("--outputs-json")
                        args.append(str(self.get_param_load_file_path_JSON(**fn_args)))
                    elif fmt == "hdf5":
                        if self.script_data_files_use_opt:
                            args.append("--outputs-hdf5")
                        args.append(str(self.get_param_load_file_path_HDF5(**fn_args)))

                commands += [
                    self._app.Command(executable=exe, arguments=args, variables=variables)
                ]

            # TODO: store script_args? and build command with executable syntax?
            main_act = self._app.Action(
                commands=commands,
                script=self.script,
                script_data_in=self.script_data_in,
                script_data_out=self.script_data_out,
                script_exe=self.script_exe,
                script_pass_env_spec=self.script_pass_env_spec,
                environments=[self.get_commands_action_env()],
                abortable=self.abortable,
                rules=main_rules,
                input_files=inp_files,
                output_files=out_files,
                save_files=self.save_files,
                clean_up=self.clean_up,
            )
            main_act._task_schema = self.task_schema
            main_act._from_expand = True
            main_act.process_script_data_formats()

            cmd_acts = inp_acts + [main_act] + out_acts

            return cmd_acts

    def get_command_input_types(self, sub_parameters: bool = False) -> tuple[str, ...]:
        """Get parameter types from commands.

        Parameters
        ----------
        sub_parameters:
            If True, sub-parameters (i.e. dot-delimited parameter types) will be returned
            untouched. If False (default), only return the root parameter type and
            disregard the sub-parameter part.
        """
        params = []
        # note: we use "parameter" rather than "input", because it could be a schema input
        # or schema output.
        vars_regex = r"\<\<(?:\w+(?:\[(?:.*)\])?\()?parameter:(.*?)\)?\>\>"
        for command in self.commands:
            for val in re.findall(vars_regex, command.command or ""):
                if not sub_parameters:
                    val = val.split(".")[0]
                params.append(val)
            for arg in command.arguments or []:
                for val in re.findall(vars_regex, arg):
                    if not sub_parameters:
                        val = val.split(".")[0]
                    params.append(val)
            # TODO: consider stdin?
        return tuple(set(params))

    def get_command_input_file_labels(self) -> tuple[str, ...]:
        """Get input files types from commands."""
        files = []
        vars_regex = r"\<\<file:(.*?)\>\>"
        for command in self.commands:
            for val in re.findall(vars_regex, command.command or ""):
                files.append(val)
            for arg in command.arguments or []:
                for val in re.findall(vars_regex, arg):
                    files.append(val)
            # TODO: consider stdin?
        return tuple(set(files))

    def get_command_output_types(self) -> tuple[str, ...]:
        """Get parameter types from command stdout and stderr arguments."""
        params = []
        for command in self.commands:
            out_params = command.get_output_types()
            if out_params["stdout"]:
                params.append(out_params["stdout"])
            if out_params["stderr"]:
                params.append(out_params["stderr"])

        return tuple(set(params))

    def get_input_types(self, sub_parameters: bool = False) -> tuple[str, ...]:
        """Get the input types that are consumed by commands and input file generators of
        this action.

        Parameters
        ----------
        sub_parameters:
            If True, sub-parameters (i.e. dot-delimited parameter types) in command line
            inputs will be returned untouched. If False (default), only return the root
            parameter type and disregard the sub-parameter part.
        """
        if (
            self.script
            and not self.input_file_generators
            and not self.output_file_parsers
        ):
            params = self.task_schema.input_types
        else:
            params = list(self.get_command_input_types(sub_parameters))
            for ifg in self.input_file_generators:
                params.extend(j.typ for j in ifg.inputs)
            for ofp in self.output_file_parsers:
                params.extend(ofp.inputs or [])
        return tuple(set(params))

    def get_output_types(self) -> tuple[str, ...]:
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
                if i.output is not None:
                    params.append(i.output.typ)
                params.extend([j for j in i.outputs or []])
        return tuple(set(params))

    def get_input_file_labels(self) -> tuple[str, ...]:
        """
        Get the labels from the input files.
        """
        return tuple(i.label for i in self.input_files)

    def get_output_file_labels(self) -> tuple[str, ...]:
        """
        Get the labels from the output files.
        """
        return tuple(i.label for i in self.output_files)

    @TimeIt.decorator
    def generate_data_index(
        self,
        act_idx: int,
        EAR_ID: int,
        schema_data_idx: DataIndex,
        all_data_idx: dict[tuple[int, int], DataIndex],
        workflow: Workflow,
        param_source: ParamSource,
    ) -> list[int | list[int]]:
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

        # keep all resources and repeats data:
        sub_data_idx = {
            k: v
            for k, v in schema_data_idx.items()
            if ("resources" in k or "repeats" in k)
        }
        param_src_update: list[int | list[int]] = []
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
                k_idx: int | list[int] | None = None
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

    def get_possible_scopes(self) -> tuple[ActionScope, ...]:
        """Get the action scopes that are inclusive of this action, ordered by decreasing
        specificity."""

        scope = self.get_precise_scope()
        scopes: tuple[ActionScope, ...]

        if self.input_file_generators:
            scopes = (
                scope,
                self._app.ActionScope.input_file_generator(),
                self._app.ActionScope.processing(),
                self._app.ActionScope.any(),
            )
        elif self.output_file_parsers:
            scopes = (
                scope,
                self._app.ActionScope.output_file_parser(),
                self._app.ActionScope.processing(),
                self._app.ActionScope.any(),
            )
        else:
            scopes = (scope, self._app.ActionScope.any())

        return scopes

    def get_precise_scope(self) -> ActionScope:
        """
        Get the exact scope of this action.
        The action must have been expanded prior to calling this.
        """
        if not self._from_expand:
            raise RuntimeError(
                "Precise scope cannot be unambiguously defined until the Action has been "
                "expanded."
            )

        if self.input_file_generators:
            return self._app.ActionScope.input_file_generator(
                file=self.input_file_generators[0].input_file.label
            )
        elif self.output_file_parsers:
            if self.output_file_parsers[0].output is not None:
                return self._app.ActionScope.output_file_parser(
                    output=self.output_file_parsers[0].output
                )
            else:
                return self._app.ActionScope.output_file_parser()
        else:
            return self._app.ActionScope.main()

    def is_input_type_required(self, typ: str, provided_files: list[FileSpec]) -> bool:
        """
        Determine if the given input type is required by this action.
        """
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
        return False

        # Appears to be not required
        return False

    @TimeIt.decorator
    def test_rules(self, element_iter) -> tuple[bool, list[int]]:
        """Test all rules against the specified element iteration."""
        rules_valid = [rule.test(element_iteration=element_iter) for rule in self.rules]
        action_valid = all(rules_valid)
        commands_idx = []
        if action_valid:
            for cmd_idx, cmd in enumerate(self.commands):
                if any(not i.test(element_iteration=element_iter) for i in cmd.rules):
                    continue
                commands_idx.append(cmd_idx)
        return action_valid, commands_idx

    def get_required_executables(self) -> tuple[str, ...]:
        """Return executable labels required by this action."""
        exec_labs = []
        for command in self.commands:
            exec_labs.extend(command.get_required_executables())
        return tuple(set(exec_labs))

    def compose_source(self, snip_path: Path) -> str:
        """Generate the file contents of this source."""

        script_name = snip_path.name
        with snip_path.open("rt") as fp:
            script_str = fp.read()

        if not self.script_is_python:
            return script_str

        py_imports = dedent(
            """\
            import argparse, sys
            from pathlib import Path

            parser = argparse.ArgumentParser()
            parser.add_argument("--wk-path")
            parser.add_argument("--run-id", type=int)
            parser.add_argument("--inputs-json")
            parser.add_argument("--inputs-hdf5")
            parser.add_argument("--outputs-json")
            parser.add_argument("--outputs-hdf5")
            args = parser.parse_args()

            """
        )

        # if any direct inputs/outputs, we must load the workflow (must be python):
        if self.script_data_in_has_direct or self.script_data_out_has_direct:
            py_main_block_workflow_load = dedent(
                """\
                    import {app_module} as app
                    app.load_config(
                        log_file_path=Path("{run_log_file}").resolve(),
                        config_dir=r"{cfg_dir}",
                        config_key=r"{cfg_invoc_key}",
                    )
                    wk_path, EAR_ID = args.wk_path, args.run_id
                    wk = app.Workflow(wk_path)
                    EAR = wk.get_EARs_from_IDs([EAR_ID])[0]
                """
            ).format(
                run_log_file=self._app.RunDirAppFiles.get_log_file_name(),
                app_module=self._app.module,
                cfg_dir=self._app.config.config_directory,
                cfg_invoc_key=self._app.config.config_key,
            )
        else:
            py_main_block_workflow_load = ""

        func_kwargs_lst = []
        if "direct" in self.script_data_in_grouped:
            direct_ins_str = "direct_ins = EAR.get_input_values_direct()"
            direct_ins_arg_str = "**direct_ins"
            func_kwargs_lst.append(direct_ins_arg_str)
        else:
            direct_ins_str = ""

        if self.script_data_in_has_files:
            # need to pass "_input_files" keyword argument to script main function:
            input_files_str = dedent(
                """\
                inp_files = {}
                if args.inputs_json:
                    inp_files["json"] = Path(args.inputs_json)
                if args.inputs_hdf5:
                    inp_files["hdf5"] = Path(args.inputs_hdf5)
            """
            )
            input_files_arg_str = "_input_files=inp_files"
            func_kwargs_lst.append(input_files_arg_str)
        else:
            input_files_str = ""

        if self.script_data_out_has_files:
            # need to pass "_output_files" keyword argument to script main function:
            output_files_str = dedent(
                """\
                out_files = {}
                if args.outputs_json:
                    out_files["json"] = Path(args.outputs_json)
                if args.outputs_hdf5:
                    out_files["hdf5"] = Path(args.outputs_hdf5)
            """
            )
            output_files_arg_str = "_output_files=out_files"
            func_kwargs_lst.append(output_files_arg_str)

        else:
            output_files_str = ""

        script_main_func = Path(script_name).stem
        func_invoke_str = f"{script_main_func}({', '.join(func_kwargs_lst)})"
        if "direct" in self.script_data_out_grouped:
            py_main_block_invoke = f"outputs = {func_invoke_str}"
            py_main_block_outputs = dedent(
                """\
                outputs = {"outputs." + k: v for k, v in outputs.items()}
                for name_i, out_i in outputs.items():
                    wk.set_parameter_value(param_id=EAR.data_idx[name_i], value=out_i)
                """
            )
        else:
            py_main_block_invoke = func_invoke_str
            py_main_block_outputs = ""

        tab_indent = "    "
        py_main_block = dedent(
            """\
            if __name__ == "__main__":
            {py_imports}
            {wk_load}
            {direct_ins}
            {in_files}
            {out_files}
            {invoke}
            {outputs}
            """
        ).format(
            py_imports=indent(py_imports, tab_indent),
            wk_load=indent(py_main_block_workflow_load, tab_indent),
            direct_ins=indent(direct_ins_str, tab_indent),
            in_files=indent(input_files_str, tab_indent),
            out_files=indent(output_files_str, tab_indent),
            invoke=indent(py_main_block_invoke, tab_indent),
            outputs=indent(py_main_block_outputs, tab_indent),
        )

        out = dedent(
            """\
            {script_str}
            {main_block}
        """
        ).format(
            script_str=script_str,
            main_block=py_main_block,
        )

        return out

    def get_parameter_names(self, prefix: str) -> list[str]:
        """Get parameter types associated with a given prefix.

        For example, with the prefix "inputs", this would return `['p1', 'p2']` for an
        action that has input types `p1` and `p2`. For inputs, labels are ignored. For
        example, for an action that accepts two inputs of the same type `p1`, with labels
        `one` and `two`, this method would return (for the "inputs" prefix):
        `['p1[one]', 'p1[two]']`.

        This method is distinct from `TaskSchema.get_parameter_names` in that it
        returns action-level input/output/file types/labels, whereas
        `TaskSchema.get_parameter_names` returns schema-level inputs/outputs.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs", "input_files", "output_files".

        """
        if prefix == "inputs":
            single_lab_lookup = self.task_schema._get_single_label_lookup()
            out = list(single_lab_lookup.get(i, i) for i in self.get_input_types())
        elif prefix == "outputs":
            out = list(f"{i}" for i in self.get_output_types())
        elif prefix == "input_files":
            out = list(f"{i}" for i in self.get_input_file_labels())
        elif prefix == "output_files":
            out = list(f"{i}" for i in self.get_output_file_labels())
        return out
