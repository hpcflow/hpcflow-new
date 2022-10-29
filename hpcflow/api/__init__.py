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
TaskSchema = hpcflow.TaskSchema
Task = hpcflow.Task
WorkflowTask = hpcflow.WorkflowTask
Workflow = hpcflow.Workflow
WorkflowTemplate = hpcflow.WorkflowTemplate
Action = hpcflow.Action
ActionScope = hpcflow.ActionScope
ActionScopeType = hpcflow.ActionScopeType
Environment = hpcflow.Environment
InputFile = hpcflow.InputFile
InputSource = hpcflow.InputSource
InputSourceType = hpcflow.InputSourceType
InputSourceMode = hpcflow.InputSourceMode
Command = hpcflow.Command
ActionEnvironment = hpcflow.ActionEnvironment
Parameter = hpcflow.Parameter
ValueSequence = hpcflow.ValueSequence
ZarrEncodable = hpcflow.ZarrEncodable
InputValue = hpcflow.InputValue
ResourceSpec = hpcflow.ResourceSpec
SchemaInput = hpcflow.SchemaInput
SchemaOutput = hpcflow.SchemaOutput
TaskSourceType = hpcflow.TaskSourceType
