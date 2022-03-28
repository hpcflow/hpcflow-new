from multiprocessing.sharedctypes import Value


class InputValueDuplicateSequenceAddress(ValueError):
    pass


class TaskTemplateMultipleSchemaObjectives(ValueError):
    pass


class TaskTemplateUnexpectedInput(ValueError):
    pass


class TaskTemplateMultipleInputValues(ValueError):
    pass


class InvalidIdentifier(ValueError):
    pass


class MissingInputs(Exception):
    pass


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


class DuplicateExecutableError(ValueError):
    pass


class MissingActionsError(ValueError):
    pass


class MissingCompatibleActionEnvironment(Exception):
    pass


class MissingActionEnvironment(Exception):
    pass


class ConfigError(Exception):
    """Raised when a valid configuration can not be associated with the current
    invocation."""

    pass


class ConfigUnknownItemError(ConfigError):
    pass


class ConfigFileValidationError(ConfigError):
    pass


class ConfigFileInvocationIncompatibleError(ConfigError):
    """Raised when, given the run time information of the invocation, no compatible
    configuration can be found in the config file."""

    def __init__(
        self, message="No config could be found that matches the current invocation."
    ):
        self.message = message
        super().__init__(self.message)


class ConfigValidationError(ConfigError):
    """Raised when the matching config data is invalid."""

    def __init__(self, message, meta_data=None):
        self.meta_data = meta_data
        self.message = message + f"config {self.meta_data}\n"
        super().__init__(self.message)


class ConfigChangeInvalidError(ConfigError):
    """Raised when trying to set an invalid key in the Config."""

    def __init__(self, name, message=None):
        self.message = message or (
            f"Cannot modify value for invalid config item {name!r}. Use the `config list`"
            f" sub-command to list all configurable items."
        )
        super().__init__(self.message)


class ConfigChangeInvalidJSONError(ConfigError):
    """Raised when attempting to set a config key using an invalid JSON string."""

    def __init__(self, name, json_str, err, message=None):
        self.message = message or (
            f"The config file has not been modified. Invalid JSON string for config item "
            f"{name!r}. {json_str!r}\n\n{err!r}"
        )
        super().__init__(self.message)


class ConfigChangeValidationError(ConfigError):
    """Raised when a change to the configurable data would invalidate the config."""

    def __init__(self, name, validation_err, message=None):
        self.message = message or (
            f"The config file has not been modified. Requested modification to item "
            f"{name!r} would invalidate the config in the following way."
            f"\n\n{validation_err}"
        )
        super().__init__(self.message)


class ConfigChangeFileUpdateError(ConfigError):
    """Raised when the updating of the config YAML file fails."""

    def __init__(self, name, err, message=None):
        self.message = message or (
            f"Failed to update the config file for modification of config item {name!r}."
            f"\n\n{err!r}"
        )
        super().__init__(self.message)


class ConfigChangeTypeInvalidError(ConfigError):
    """Raised when trying to modify a config item using a list operation, when the config
    item is not a list."""

    def __init__(self, name, typ, message=None):
        self.message = message or (
            f"The config file has not been modified. The config item {name!r} has type "
            f"{typ!r} and so cannot be modified in that way."
        )
        super().__init__(self.message)


class ConfigChangePopIndexError(ConfigError):
    """Raised when trying to pop an item from a config item with an invalid index."""

    def __init__(self, name, length, index, message=None):
        self.message = message or (
            f"The config file has not been modified. The config item {name!r} has length "
            f"{length!r} and so cannot be popped with index {index}."
        )
        super().__init__(self.message)


class MissingTaskSchemaFileError(ConfigError):
    """Raised when a task schema file specified in the config file does not exist."""

    def __init__(self, file_name, err, message=None):
        self.message = message or (
            f"The task schema file {file_name!r} cannot be found. \n{err!s}"
        )
        super().__init__(self.message)


class MissingEnvironmentFileError(ConfigError):
    """Raised when an environment file specified in the config file does not exist."""

    def __init__(self, file_name, err, message=None):
        self.message = message or (
            f"The environment file {file_name!r} cannot be found. \n{err!s}"
        )
        super().__init__(self.message)
