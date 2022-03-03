"""Module containing logic for parsing workflow spec files/strings."""

from importlib import resources
from pathlib import Path

from ruamel.yaml import YAML
from valida import Schema
from hpcflow.command_files import FileSpec

from hpcflow.errors import (
    TaskSchemaSpecValidationError,
    WorkflowSpecValidationError,
    EnvironmentSpecValidationError,
)
from hpcflow.parameters import Parameter, SchemaInput, SchemaOutput
from hpcflow.task_schema import TaskSchema
from hpcflow.workflow import WorkflowTemplate
from hpcflow.environment import Executable, ExecutableInstance, Environment


def get_workflow_spec_schema():
    with resources.open_text("hpcflow.data", "workflow_spec_schema.yaml") as fh:
        schema_dat = fh.read()
    schema = Schema.from_yaml(schema_dat)
    return schema


def get_task_schema_spec_schema():
    with resources.open_text("hpcflow.data", "task_schema_spec_schema.yaml") as fh:
        schema_dat = fh.read()
    schema = Schema.from_yaml(schema_dat)
    return schema


def get_environment_spec_schema():
    with resources.open_text("hpcflow.data", "environments_spec_schema.yaml") as fh:
        schema_dat = fh.read()
    schema = Schema.from_yaml(schema_dat)
    return schema


def get_task_schemas_and_parameters():
    with resources.open_text("hpcflow.data", "task_schemas.yaml") as fh:
        yaml_str = fh.read()

    yaml = YAML(typ="safe")
    task_schemas_dat = yaml.load(yaml_str)

    task_schemas_spec_schema = get_task_schema_spec_schema()
    validated = task_schemas_spec_schema.validate(task_schemas_dat)
    if not validated.is_valid:
        raise TaskSchemaSpecValidationError(validated.get_failures_string())

    parameters = {}
    for i in task_schemas_dat["parameters"]:
        parameters.update({i["type"]: Parameter.from_spec(i)})

    cmd_files = []
    for i in task_schemas_dat["command_files"]:
        cmd_files.append(FileSpec.from_spec(i))

    envs = get_environments()

    task_schemas = {}
    for i in task_schemas_dat["task_schemas"]:
        key = (i["objective"], i.get("method"), i.get("implementation"))
        schema = TaskSchema.from_spec(i, parameters, envs, cmd_files)
        task_schemas.update({key: schema})

    return task_schemas, parameters, envs, cmd_files


def get_environments():
    with resources.open_text("hpcflow.data", "environments.yaml") as fh:
        yaml_str = fh.read()

    yaml = YAML(typ="safe")
    envs_dat = yaml.load(yaml_str)

    env_spec_schema = get_environment_spec_schema()
    validated = env_spec_schema.validate(envs_dat)
    if not validated.is_valid:
        raise EnvironmentSpecValidationError(validated.get_failures_string())

    envs = [Environment.from_spec(env_spec) for env_spec in envs_dat]
    return envs


def parse_YAML_spec_file(yaml_file):
    """Generate a WorkflowTemplate from a YAML string."""
    with Path(yaml_file).open("r") as fh:
        yaml_str = fh.read()
    return parse_YAML_spec_str(yaml_str)


def parse_YAML_spec_str(yaml_str):
    """Generate a WorkflowTemplate from a YAML string."""
    yaml = YAML(typ="safe")
    workflow_dat = yaml.load(yaml_str)
    validated = get_workflow_spec_schema().validate(workflow_dat)

    if not validated.is_valid:
        raise WorkflowSpecValidationError(validated.get_failures_string())

    task_schemas, parameters, envs, cmd_files = get_task_schemas_and_parameters()

    workflow = WorkflowTemplate.from_spec(workflow_dat, task_schemas, parameters)

    return workflow
