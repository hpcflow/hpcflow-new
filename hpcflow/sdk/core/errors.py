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
