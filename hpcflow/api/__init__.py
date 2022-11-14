from hpcflow import __version__
from hpcflow.sdk import ConfigOptions
from hpcflow.sdk.app import BaseApp

config_options = ConfigOptions(
    directory_env_var="HPCFLOW_CONFIG_DIR",
    default_directory="~/.hpcflow",
    sentry_DSN="https://2463b288fd1a40f4bada9f5ff53f6811@o1180430.ingest.sentry.io/6293231",
    sentry_traces_sample_rate=1.0,
    sentry_env="main" if "a" in __version__ else "develop",
)

hpcflow = BaseApp(
    name="hpcflow",
    version=__version__,
    description="Computational workflow management",
    config_options=config_options,
    pytest_args=[
        "--verbose",
        "--exitfirst",
    ],
)

load_config = hpcflow.load_config
reload_config = hpcflow.reload_config
make_workflow = hpcflow.make_workflow

# expose core classes that require access to the App instance:
Action = hpcflow.Action
ActionEnvironment = hpcflow.ActionEnvironment
ActionScope = hpcflow.ActionScope
ActionScopeType = hpcflow.ActionScopeType
Command = hpcflow.Command
Environment = hpcflow.Environment
Executable = hpcflow.Executable
ExecutableInstance = hpcflow.ExecutableInstance
ExecutablesList = hpcflow.ExecutablesList
FileSpec = hpcflow.FileSpec
InputFile = hpcflow.InputFile
InputFileGenerator = hpcflow.InputFileGenerator
InputSource = hpcflow.InputSource
InputSourceMode = hpcflow.InputSourceMode
InputSourceType = hpcflow.InputSourceType
InputValue = hpcflow.InputValue
Parameter = hpcflow.Parameter
ResourceList = hpcflow.ResourceList
ResourceSpec = hpcflow.ResourceSpec
SchemaInput = hpcflow.SchemaInput
SchemaOutput = hpcflow.SchemaOutput
Task = hpcflow.Task
TaskObjective = hpcflow.TaskObjective
TaskSchema = hpcflow.TaskSchema
TaskSourceType = hpcflow.TaskSourceType
ValueSequence = hpcflow.ValueSequence
Workflow = hpcflow.Workflow
WorkflowTask = hpcflow.WorkflowTask
WorkflowTemplate = hpcflow.WorkflowTemplate
ZarrEncodable = hpcflow.ZarrEncodable
