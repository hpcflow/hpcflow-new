class InputValueDuplicateSequenceAddress(ValueError):
    pass


class TaskTemplateMultipleSchemaObjectives(ValueError):
    pass


class TaskTemplateUnexpectedInput(ValueError):
    pass


class TaskTemplateUnexpectedSequenceInput(ValueError):
    pass


class TaskTemplateMultipleInputValues(ValueError):
    pass


class InvalidIdentifier(ValueError):
    pass


class MissingInputs(Exception):
    # TODO: add links to doc pages for common user-exceptions?

    def __init__(self, message, missing_inputs) -> None:
        self.missing_inputs = missing_inputs
        super().__init__(message)


class UnrequiredInputSources(ValueError):
    def __init__(self, message, unrequired_sources) -> None:
        self.unrequired_sources = unrequired_sources
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
    def __init__(self, message, extra_inputs) -> None:
        self.extra_inputs = extra_inputs
        super().__init__(message)


class TaskTemplateInvalidNesting(ValueError):
    pass


class TaskSchemaSpecValidationError(Exception):
    pass


class WorkflowSpecValidationError(Exception):
    pass


class InputSourceValidationError(Exception):
    pass


class EnvironmentSpecValidationError(Exception):
    pass


class ParameterSpecValidationError(Exception):
    pass


class FileSpecValidationError(Exception):
    pass


class DuplicateExecutableError(ValueError):
    pass


class MissingCompatibleActionEnvironment(Exception):
    pass


class MissingActionEnvironment(Exception):
    pass


class FromSpecMissingObjectError(Exception):
    pass


class TaskSchemaMissingParameterError(Exception):
    pass


class ToJSONLikeChildReferenceError(Exception):
    pass


class InvalidInputSourceTaskReference(Exception):
    pass


class WorkflowNotFoundError(Exception):
    pass


class ValuesAlreadyPersistentError(Exception):
    pass


class MalformedParameterPathError(ValueError):
    pass


class UnknownResourceSpecItemError(ValueError):
    pass


class WorkflowParameterMissingError(AttributeError):
    pass


class WorkflowBatchUpdateFailedError(Exception):
    pass


class WorkflowLimitsError(ValueError):
    pass


class UnsetParameterDataError(Exception):
    pass


class LoopAlreadyExistsError(Exception):
    pass


class SchedulerVersionsFailure(RuntimeError):
    """We couldn't get the scheduler and or shell versions."""

    def __init__(self, message):
        self.message = message
        super().__init__(message)


class JobscriptSubmissionFailure(RuntimeError):
    def __init__(
        self,
        message,
        submit_cmd,
        js_idx,
        js_path,
        stdout,
        stderr,
        subprocess_exc,
        job_ID_parse_exc,
    ) -> None:
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
    def __init__(self, message) -> None:
        self.message = message
        super().__init__(message)


class WorkflowSubmissionFailure(RuntimeError):
    pass


class UnsupportedShellError(ValueError):
    """We don't support this shell on this OS."""

    pass
