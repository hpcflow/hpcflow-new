"""
Errors from the workflow system.
"""

from __future__ import annotations
import os
from collections.abc import Iterable


class InputValueDuplicateSequenceAddress(ValueError):
    """
    An InputValue has the same sequence address twice.
    """


class TaskTemplateMultipleSchemaObjectives(ValueError):
    """
    A TaskTemplate has multiple objectives.
    """


class TaskTemplateUnexpectedInput(ValueError):
    """
    A TaskTemplate was given unexpected input.
    """


class TaskTemplateUnexpectedSequenceInput(ValueError):
    """
    A TaskTemplate was given an unexpected sequence.
    """


class TaskTemplateMultipleInputValues(ValueError):
    """
    A TaskTemplate had multiple input values bound over each other.
    """


class InvalidIdentifier(ValueError):
    """
    A bad identifier name was given.
    """


class MissingInputs(Exception):
    """
    Inputs were missing.

    Parameters
    ----------
    message:
        The message of the exception.
    missing_inputs:
        The missing inputs.
    """

    # TODO: add links to doc pages for common user-exceptions?

    def __init__(self, message: str, missing_inputs: Iterable[str]) -> None:
        self.missing_inputs = tuple(missing_inputs)
        super().__init__(message)


class UnrequiredInputSources(ValueError):
    """
    Input sources were provided that were not required.

    Parameters
    ----------
    message:
        The message of the exception.
    unrequired_sources:
        The input sources that were not required.
    """

    def __init__(self, message: str, unrequired_sources: Iterable[str]) -> None:
        self.unrequired_sources = frozenset(unrequired_sources)
        for src in unrequired_sources:
            if src.startswith("inputs."):
                # reminder about how to specify input sources:
                message += (
                    f" Note that input source keys should not be specified with the "
                    f"'inputs.' prefix. Did you mean to specify {src[len('inputs.'):]!r} "
                    f"instead of {src!r}?"
                )
                break
        super().__init__(message)


class ExtraInputs(Exception):
    """
    Extra inputs were provided.

    Parameters
    ----------
    message:
        The message of the exception.
    extra_inputs:
        The extra inputs.
    """

    def __init__(self, message: str, extra_inputs: set[str]) -> None:
        self.extra_inputs = frozenset(extra_inputs)
        super().__init__(message)


class UnavailableInputSource(ValueError):
    """
    An input source was not available.
    """


class InapplicableInputSourceElementIters(ValueError):
    """
    An input source element iteration was inapplicable."""


class NoCoincidentInputSources(ValueError):
    """
    Could not line up input sources to make an actual valid execution.
    """


class TaskTemplateInvalidNesting(ValueError):
    """
    Invalid nesting in a task template.
    """


class TaskSchemaSpecValidationError(Exception):
    """
    A task schema failed to validate.
    """


class WorkflowSpecValidationError(Exception):
    """
    A workflow failed to validate.
    """


class InputSourceValidationError(Exception):
    """
    An input source failed to validate.
    """


class EnvironmentSpecValidationError(Exception):
    """
    An environment specification failed to validate.
    """


class ParameterSpecValidationError(Exception):
    """
    A parameter specification failed to validate.
    """


class FileSpecValidationError(Exception):
    """
    A file specification failed to validate.
    """


class DuplicateExecutableError(ValueError):
    """
    The same executable was present twice in an executable environment.
    """


class MissingCompatibleActionEnvironment(Exception):
    """
    Could not find a compatible action environment.
    """


class MissingActionEnvironment(Exception):
    """
    Could not find an action environment.
    """


class ActionEnvironmentMissingNameError(Exception):
    """
    An action environment was missing its name.
    """


class FromSpecMissingObjectError(Exception):
    """
    Missing object when deserialising from specification.
    """


class TaskSchemaMissingParameterError(Exception):
    """
    Parameter was missing from task schema.
    """


class ToJSONLikeChildReferenceError(Exception):
    """
    Failed to generate or reference a child object when converting to JSON.
    """


class InvalidInputSourceTaskReference(Exception):
    """
    Invalid input source in task reference.
    """


class WorkflowNotFoundError(Exception):
    """
    Could not find the workflow.
    """


class MalformedWorkflowError(Exception):
    """
    Workflow was a malformed document.
    """


class ValuesAlreadyPersistentError(Exception):
    """
    Trying to make a value persistent that already is so.
    """


class MalformedParameterPathError(ValueError):
    """
    The path to a parameter was ill-formed.
    """


class MalformedNestingOrderPath(ValueError):
    """
    A nesting order path was ill-formed.
    """


class UnknownResourceSpecItemError(ValueError):
    """
    A resource specification item was not found.
    """


class WorkflowParameterMissingError(AttributeError):
    """
    A parameter to a workflow was missing.
    """


class WorkflowBatchUpdateFailedError(Exception):
    """
    An update to a workflow failed.
    """


class WorkflowLimitsError(ValueError):
    """
    Workflow hit limits.
    """


class UnsetParameterDataError(Exception):
    """
    Tried to read from an unset parameter.
    """


class LoopAlreadyExistsError(Exception):
    """
    A particular loop (or its name) already exists.
    """


class LoopTaskSubsetError(ValueError):
    """
    Problem constructing a subset of a task for a loop.
    """


class SchedulerVersionsFailure(RuntimeError):
    """We couldn't get the scheduler and or shell versions."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class JobscriptSubmissionFailure(RuntimeError):
    """
    A job script could not be submitted to the scheduler.
    """

    def __init__(
        self,
        message: str,
        *,
        submit_cmd: list[str],
        js_idx: int,
        js_path: str,
        stdout: str | None = None,
        stderr: str | None = None,
        subprocess_exc: Exception | None = None,
        job_ID_parse_exc: Exception | None = None,
    ):
        self.message = message
        self.submit_cmd = submit_cmd
        self.js_idx = js_idx
        self.js_path = js_path
        self.stdout = stdout
        self.stderr = stderr
        self.subprocess_exc = subprocess_exc
        self.job_ID_parse_exc = job_ID_parse_exc
        super().__init__(message)


class SubmissionFailure(RuntimeError):
    """
    A job submission failed.
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class WorkflowSubmissionFailure(RuntimeError):
    """
    A workflow submission failed.
    """


class ResourceValidationError(ValueError):
    """An incompatible resource requested by the user."""


class UnsupportedOSError(ResourceValidationError):
    """This machine is not of the requested OS."""

    def __init__(self, os_name: str) -> None:
        message = (
            f"OS {os_name!r} is not compatible with this machine/instance with OS: "
            f"{os.name!r}."
        )
        super().__init__(message)
        self.os_name = os_name


class UnsupportedShellError(ResourceValidationError):
    """We don't support this shell on this OS."""

    def __init__(self, shell: str, supported: Iterable[str]) -> None:
        sup = set(supported)
        message = (
            f"Shell {shell!r} is not supported on this machine/instance. Supported "
            f"shells are: {sup!r}."
        )
        super().__init__(message)
        self.shell = shell
        self.supported = frozenset(sup)


class UnsupportedSchedulerError(ResourceValidationError):
    """This scheduler is not supported on this machine according to the config.

    This is also raised in config validation when attempting to add a scheduler that is
    not known for this OS.

    """

    def __init__(
        self,
        scheduler: str,
        supported: Iterable[str] | None = None,
        available: Iterable[str] | None = None,
    ) -> None:
        if supported is not None:
            message = (
                f"Scheduler {scheduler!r} is not supported on this machine/instance. "
                f"Supported schedulers according to the app configuration are: "
                f"{supported!r}."
            )
        elif available is not None:
            message = (
                f"Scheduler {scheduler!r} is not supported on this OS. Schedulers "
                f"compatible with this OS are: {available!r}."
            )
        super().__init__(message)
        self.scheduler = scheduler
        self.supported = tuple(supported) if supported is not None else None
        self.available = tuple(available) if available is not None else None


class UnknownSGEPEError(ResourceValidationError):
    """
    Miscellaneous error from SGE parallel environment.
    """


class IncompatibleSGEPEError(ResourceValidationError):
    """
    The SGE parallel environment selected is incompatible.
    """


class NoCompatibleSGEPEError(ResourceValidationError):
    """
    No SGE parallel environment is compatible with request.
    """


class IncompatibleParallelModeError(ResourceValidationError):
    """
    The parallel mode is incompatible.
    """


class UnknownSLURMPartitionError(ResourceValidationError):
    """
    The requested SLURM partition isn't known.
    """


class IncompatibleSLURMPartitionError(ResourceValidationError):
    """
    The requested SLURM partition is incompatible.
    """


class IncompatibleSLURMArgumentsError(ResourceValidationError):
    """
    The SLURM arguments are incompatible with each other.
    """


class _MissingStoreItemError(ValueError):
    def __init__(self, id_lst: Iterable[int], item_type: str) -> None:
        message = (
            f"Store {item_type}s with the following IDs do not all exist: {id_lst!r}"
        )
        super().__init__(message)
        self.id_lst = id_lst


class MissingStoreTaskError(_MissingStoreItemError):
    """Some task IDs do not exist."""

    _item_type = "task"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class MissingStoreElementError(_MissingStoreItemError):
    """Some element IDs do not exist."""

    _item_type = "element"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class MissingStoreElementIterationError(_MissingStoreItemError):
    """Some element iteration IDs do not exist."""

    _item_type = "element iteration"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class MissingStoreEARError(_MissingStoreItemError):
    """Some EAR IDs do not exist."""

    _item_type = "EAR"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class MissingParameterData(_MissingStoreItemError):
    """Some parameter IDs do not exist"""

    _item_type = "parameter"

    def __init__(self, id_lst: Iterable[int]) -> None:
        super().__init__(id_lst, self._item_type)


class NotSubmitMachineError(RuntimeError):
    """
    The requested machine can't be submitted to.
    """


class RunNotAbortableError(ValueError):
    """
    Cannot abort the run.
    """


class NoCLIFormatMethodError(AttributeError):
    """
    Some CLI class lacks a format method
    """


class ContainerKeyError(KeyError):
    """
    A key could not be mapped in a container.

    Parameters
    ----------
    path:
        The path whose resolution failed.
    """

    def __init__(self, path: list[str]) -> None:
        self.path = path
        super().__init__()


class MayNeedObjectError(Exception):
    """
    An object is needed but not present.

    Parameters
    ----------
    path:
        The path whose resolution failed.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__()


class NoAvailableElementSetsError(Exception):
    """
    No element set is available.
    """


class OutputFileParserNoOutputError(ValueError):
    """
    There was no output for the output file parser to parse.
    """


class SubmissionEnvironmentError(ValueError):
    """
    Raised when submitting a workflow on a machine without a compatible environment.
    """


class MissingEnvironmentExecutableError(SubmissionEnvironmentError):
    """
    The environment does not have the requested executable at all.
    """


class MissingEnvironmentExecutableInstanceError(SubmissionEnvironmentError):
    """
    The environment does not have a suitable instance of the requested executable.
    """


class MissingEnvironmentError(SubmissionEnvironmentError):
    """
    There is no environment with that name.
    """


class UnsupportedScriptDataFormat(ValueError):
    """
    That format of script data is not supported.
    """


class UnknownScriptDataParameter(ValueError):
    """
    Unknown parameter in script data.
    """


class UnknownScriptDataKey(ValueError):
    """
    Unknown key in script data.
    """


class MissingVariableSubstitutionError(KeyError):
    """
    No definition available of a variable being substituted.
    """


class EnvironmentPresetUnknownEnvironmentError(ValueError):
    """
    An environment preset could not be resolved to an execution environment.
    """


class UnknownEnvironmentPresetError(ValueError):
    """
    An execution environment was unknown.
    """


class MultipleEnvironmentsError(ValueError):
    """
    Multiple applicable execution environments exist.
    """


class MissingElementGroup(ValueError):
    """
    An element group should exist but doesn't.
    """
