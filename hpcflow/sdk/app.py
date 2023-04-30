"""An hpcflow application."""

from functools import wraps
from importlib import resources, import_module
import time
from typing import Dict
import warnings

import click
from colorama import init as colorama_init
from termcolor import colored

from hpcflow import __version__
from hpcflow.sdk.persistence import ALL_STORE_FORMATS, DEFAULT_STORE_FORMAT
from hpcflow.sdk.submission.shells import ALL_SHELLS
from .core.json_like import JSONLike
from .core.utils import read_YAML, read_YAML_file
from . import api, SDK_logger
from .config import Config
from .config.cli import get_config_CLI
from .config.errors import ConfigError
from .core.actions import (
    Action,
    ActionScopeType,
    ElementAction,
    ElementAction,
    ElementActionRun,
)
from .core.element import (
    Element,
    ElementIteration,
    ElementInputFiles,
    ElementInputs,
    ElementOutputFiles,
    ElementOutputs,
    ElementResources,
    ElementParameter,
)
from .core.parameters import (
    InputSourceType,
    ParameterPropagationMode,
    TaskSourceType,
    ValueSequence,
    ParameterValue,
)
from .core.task import (
    ElementPropagation,
    Parameters,
    TaskInputParameters,
    TaskOutputParameters,
    WorkflowTask,
    ElementSet,
    Elements,
)
from .core.loop import Loop, WorkflowLoop
from .core.task_schema import TaskObjective
from .core.workflow import ALL_TEMPLATE_FORMATS, DEFAULT_TEMPLATE_FORMAT, Workflow
from .demo.cli import get_demo_software_CLI
from .helper.cli import get_helper_CLI
from .log import AppLog
from .runtime import RunTimeInfo

SDK_logger = SDK_logger.getChild(__name__)


class BaseApp:
    """Class to generate the base hpcflow application."""

    _template_component_types = (
        "parameters",
        "command_files",
        "environments",
        "task_schemas",
    )

    def __init__(
        self,
        name,
        version,
        description,
        config_options,
        template_components: Dict = None,
        pytest_args=None,
    ):
        SDK_logger.info(f"Generating {self.__class__.__name__} {name!r}.")

        self.name = name
        self.version = version
        self.description = description
        self.config_options = config_options
        self.pytest_args = pytest_args

        self.CLI = self._make_CLI()
        self.log = AppLog(self)
        self.run_time_info = RunTimeInfo(
            self.name, self.version, self.runtime_info_logger
        )

        self._builtin_template_components = template_components or {}

        self._config = None  # assigned on first access to `config` property

        # Set by `_load_template_components`:
        self._template_components = {}
        self._parameters = None
        self._command_files = None
        self._environments = None
        self._task_schemas = None
        self._scripts = None

        self._core_classes = self._assign_core_classes()

        # Add API functions as methods:
        SDK_logger.debug(f"Assigning API functions to the {self.__class__.__name__}.")

        def get_api_method(func):
            # this function avoids scope issues
            return lambda *args, **kwargs: func(self, *args, **kwargs)

        for func in (getattr(api, i) for i in api.__all__):

            if type(self) is BaseApp and func.__name__ == "run_hpcflow_tests":
                # this method provides the same functionality as the `run_tests` method
                continue

            SDK_logger.debug(f"Wrapping API callable: {func!r}")
            # allow sub-classes to override API functions:
            if not hasattr(self, func.__name__):
                api_method = get_api_method(func)
                api_method = wraps(func)(api_method)
                api_method.__doc__ = func.__doc__.format(app_name=name)
                setattr(self, func.__name__, api_method)

    @property
    def template_components(self):
        if not self.is_template_components_loaded:
            self._load_template_components()
        return self._template_components

    def _get_core_JSONLike_classes(self):
        """Get all JSONLike subclasses (recursively).

        If this is run after App initialisation, the returned list will include the
        app-injected sub-classes as well.

        """

        def all_subclasses(cls):
            return set(cls.__subclasses__()).union(
                [s for c in cls.__subclasses__() for s in all_subclasses(c)]
            )

        return all_subclasses(JSONLike)

    def inject_into(self, cls):
        SDK_logger.debug(f"Injecting app {self!r} into class {cls.__name__}")
        if cls.__doc__:
            cls.__doc__ = cls.__doc__.format(app_name=self.name)
        return type(cls.__name__, (cls,), {getattr(cls, "_app_attr"): self})

    def _assign_core_classes(self):

        # ensure classes defined in `object_list` are included in core classes:
        import_module("hpcflow.sdk.core.object_list")

        core_classes = list(self._get_core_JSONLike_classes())

        # Non-`JSONLike` classes:
        core_classes += [
            ActionScopeType,
            Element,
            ElementIteration,
            Elements,
            ElementInputs,
            ElementInputs,
            ElementOutputs,
            ElementResources,
            ElementInputFiles,
            ElementOutputFiles,
            ElementAction,
            ElementAction,
            ElementActionRun,
            ElementParameter,
            ElementPropagation,
            InputSourceType,
            Parameters,
            ParameterPropagationMode,
            TaskSourceType,
            TaskInputParameters,
            TaskOutputParameters,
            Workflow,
            WorkflowTask,
            WorkflowLoop,
            ParameterValue,
        ]
        for cls in core_classes:
            if hasattr(cls, "_app_attr"):
                setattr(self, cls.__name__, self.inject_into(cls))
            else:
                setattr(self, cls.__name__, cls)

        return tuple(
            sorted(
                set(core_classes),
                key=lambda x: f"{x.__module__}.{x.__qualname__}",
            )
        )

    def _ensure_template_components(self):
        if not self.is_template_components_loaded:
            self._load_template_components()

    def load_template_components(self, warn=True):
        if warn and self.is_template_components_loaded:
            warnings.warn("Template components already loaded; reloading now.")
        self._load_template_components()

    def reload_template_components(self, warn=True):
        if warn and not self.is_template_components_loaded:
            warnings.warn("Template components not loaded; loading now.")
        self._load_template_components()

    def _load_template_components(self):
        """Combine any builtin template components with user-defined template components
        and initialise list objects."""

        params = self._builtin_template_components.get("parameters", [])
        for path in self.config.parameter_sources:
            params.extend(read_YAML_file(path))

        cmd_files = self._builtin_template_components.get("command_files", [])
        for path in self.config.command_file_sources:
            cmd_files.extend(read_YAML_file(path))

        envs = self._builtin_template_components.get("environments", [])
        for path in self.config.environment_sources:
            envs.extend(read_YAML_file(path))

        schemas = self._builtin_template_components.get("task_schemas", [])
        for path in self.config.task_schema_sources:
            schemas.extend(read_YAML_file(path))

        self_tc = self._template_components
        self_tc["parameters"] = self.ParametersList.from_json_like(
            params, shared_data=self_tc
        )
        self_tc["command_files"] = self.CommandFilesList.from_json_like(
            cmd_files, shared_data=self_tc
        )
        self_tc["environments"] = self.EnvironmentsList.from_json_like(
            envs, shared_data=self_tc
        )
        self_tc["task_schemas"] = self.TaskSchemasList.from_json_like(
            schemas, shared_data=self_tc
        )

        self._parameters = self_tc["parameters"]
        self._command_files = self_tc["command_files"]
        self._environments = self_tc["environments"]
        self._task_schemas = self_tc["task_schemas"]

        self._scripts = self._load_scripts()

        self.logger.info("Template components loaded.")

    @classmethod
    def load_builtin_template_component_data(cls, package):
        components = {}
        for comp_type in cls._template_component_types:
            with resources.open_text(package, f"{comp_type}.yaml") as fh:
                comp_dat = fh.read()
                components[comp_type] = read_YAML(comp_dat)
        return components

    @property
    def parameters(self):
        self._ensure_template_components()
        return self._parameters

    @property
    def command_files(self):
        self._ensure_template_components()
        return self._command_files

    @property
    def envs(self):
        self._ensure_template_components()
        return self._environments

    @property
    def scripts(self):
        self._ensure_template_components()
        return self._scripts

    @property
    def task_schemas(self):
        self._ensure_template_components()
        return self._task_schemas

    @property
    def logger(self):
        return self.log.logger

    @property
    def API_logger(self):
        return self.logger.getChild("api")

    @property
    def CLI_logger(self):
        return self.logger.getChild("cli")

    @property
    def config_logger(self):
        return self.logger.getChild("config")

    @property
    def runtime_info_logger(self):
        return self.logger.getChild("runtime")

    @property
    def is_config_loaded(self):
        return bool(self._config)

    @property
    def is_template_components_loaded(self):
        return bool(self._parameters)

    @property
    def config(self):
        if not self.is_config_loaded:
            self.load_config()
        return self._config

    def _load_config(self, config_dir, config_invocation_key, **overrides):
        self.logger.debug("Loading configuration.")
        self._config = Config(
            app=self,
            options=self.config_options,
            config_dir=config_dir,
            config_invocation_key=config_invocation_key,
            logger=self.config_logger,
            **overrides,
        )
        self.logger.info(f"Configuration loaded from: {self.config.config_file_path}")

    def load_config(self, config_dir=None, config_invocation_key=None, **overrides):
        if self.is_config_loaded:
            warnings.warn("Configuration is already loaded; reloading.")
        self._load_config(config_dir, config_invocation_key, **overrides)

    def reload_config(self, config_dir=None, config_invocation_key=None, **overrides):
        if not self.is_config_loaded:
            warnings.warn("Configuration is not loaded; loading.")
        self._load_config(config_dir, config_invocation_key, **overrides)

    def _make_API_CLI(self):
        """Generate the CLI for the main functionality."""

        @click.command(name="make")
        @click.argument("template_file_or_str")
        @click.option(
            "--string",
            is_flag=True,
            default=False,
            help="Determines if passing a file path or a string.",
        )
        @click.option(
            "--format",
            type=click.Choice(ALL_TEMPLATE_FORMATS),
            default=DEFAULT_TEMPLATE_FORMAT,
            help=(
                'If specified, one of "json" or "yaml". This forces parsing from a '
                "particular format."
            ),
        )
        @click.option(
            "--path",
            type=click.Path(exists=True),
            help="The directory path into which the new workflow will be generated.",
        )
        @click.option(
            "--name",
            help=(
                "The name of the workflow. If specified, the workflow directory will be "
                "`path` joined with `name`. If not specified the workflow template name "
                "will be used, in combination with a date-timestamp."
            ),
        )
        @click.option(
            "--overwrite",
            is_flag=True,
            default=False,
            help=(
                "If True and the workflow directory (`path` + `name`) already exists, "
                "the existing directory will be overwritten."
            ),
        )
        @click.option(
            "--store",
            type=click.Choice(ALL_STORE_FORMATS),
            help="The persistent store type to use.",
            default=DEFAULT_STORE_FORMAT,
        )
        @click.option(
            "--ts-fmt",
            help=(
                "The datetime format to use for storing datetimes. Datetimes are always "
                "stored in UTC (because Numpy does not store time zone info), so this "
                "should not include a time zone name."
            ),
        )
        @click.option(
            "--ts-name-fmt",
            help=(
                "The datetime format to use when generating the workflow name, where it "
                "includes a timestamp."
            ),
        )
        def make_workflow(
            template_file_or_str,
            string,
            format,
            path,
            name,
            overwrite,
            store,
            ts_fmt=None,
            ts_name_fmt=None,
        ):
            """Generate a new {app_name} workflow.

            TEMPLATE_FILE_OR_STR is either a path to a template file in YAML or JSON
            format, or a YAML/JSON string.

            """
            wk = self.make_workflow(
                template_file_or_str=template_file_or_str,
                is_string=string,
                template_format=format,
                path=path,
                name=name,
                overwrite=overwrite,
                store=store,
                ts_fmt=ts_fmt,
                ts_name_fmt=ts_name_fmt,
            )
            click.echo(wk.path)

        @click.command(name="go")
        @click.argument("template_file_or_str")
        @click.option(
            "--string",
            is_flag=True,
            default=False,
            help="Determines if passing a file path or a string.",
        )
        @click.option(
            "--format",
            type=click.Choice(ALL_TEMPLATE_FORMATS),
            default=DEFAULT_TEMPLATE_FORMAT,
            help=(
                'If specified, one of "json" or "yaml". This forces parsing from a '
                "particular format."
            ),
        )
        @click.option(
            "--path",
            type=click.Path(exists=True),
            help="The directory path into which the new workflow will be generated.",
        )
        @click.option(
            "--name",
            help=(
                "The name of the workflow. If specified, the workflow directory will be "
                "`path` joined with `name`. If not specified the workflow template name "
                "will be used, in combination with a date-timestamp."
            ),
        )
        @click.option(
            "--overwrite",
            is_flag=True,
            default=False,
            help=(
                "If True and the workflow directory (`path` + `name`) already exists, "
                "the existing directory will be overwritten."
            ),
        )
        @click.option(
            "--store",
            type=click.Choice(ALL_STORE_FORMATS),
            help="The persistent store type to use.",
            default=DEFAULT_STORE_FORMAT,
        )
        @click.option(
            "--ts-fmt",
            help=(
                "The datetime format to use for storing datetimes. Datetimes are always "
                "stored in UTC (because Numpy does not store time zone info), so this "
                "should not include a time zone name."
            ),
        )
        @click.option(
            "--ts-name-fmt",
            help=(
                "The datetime format to use when generating the workflow name, where it "
                "includes a timestamp."
            ),
        )
        @click.option(
            "--js-parallelism",
            help=(
                "If True, allow multiple jobscripts to execute simultaneously. Raises if "
                "set to True but the store type does not support the "
                "`jobscript_parallelism` feature. If not set, jobscript parallelism will "
                "be used if the store type supports it."
            ),
            type=click.BOOL,
        )
        def make_and_submit_workflow(
            template_file_or_str,
            string,
            format,
            path,
            name,
            overwrite,
            store,
            ts_fmt=None,
            ts_name_fmt=None,
            js_parallelism=None,
        ):
            """Generate and submit a new {app_name} workflow.

            TEMPLATE_FILE_OR_STR is either a path to a template file in YAML or JSON
            format, or a YAML/JSON string.

            """
            self.make_and_submit_workflow(
                template_file_or_str=template_file_or_str,
                is_string=string,
                template_format=format,
                path=path,
                name=name,
                overwrite=overwrite,
                store=store,
                ts_fmt=ts_fmt,
                ts_name_fmt=ts_name_fmt,
                JS_parallelism=js_parallelism,
            )

        @click.command(context_settings={"ignore_unknown_options": True})
        @click.argument("py_test_args", nargs=-1, type=click.UNPROCESSED)
        @click.pass_context
        def test(ctx, py_test_args):
            """Run {app_name} test suite.

            PY_TEST_ARGS are arguments passed on to Pytest.

            """
            ctx.exit(self.run_tests(*py_test_args))

        @click.command(context_settings={"ignore_unknown_options": True})
        @click.argument("py_test_args", nargs=-1, type=click.UNPROCESSED)
        @click.pass_context
        def test_hpcflow(ctx, py_test_args):
            """Run hpcflow test suite.".

            PY_TEST_ARGS are arguments passed on to Pytest.

            """
            ctx.exit(self.run_hpcflow_tests(*py_test_args))

        commands = [
            make_workflow,
            make_and_submit_workflow,
            test,
        ]
        for cmd in commands:
            if cmd.help:
                cmd.help = cmd.help.format(app_name=self.name)

        if type(self) is not BaseApp:
            # `test_hpcflow` is the same as `test` for the BaseApp so no need to add both:
            commands.append(test_hpcflow)

        return commands

    def _make_workflow_submission_jobscript_CLI(self):
        """Generate the CLI for interacting with existing workflow submission
        jobscripts."""

        @click.group(name="js")
        @click.pass_context
        @click.argument("js_idx", type=click.INT)
        def jobscript(ctx, js_idx):
            """Interact with existing {app_name} workflow submission jobscripts.

            JS_IDX is the jobscript index within the submission object.

            """
            ctx.obj["jobscript"] = ctx.obj["submission"].jobscripts[js_idx]

        @jobscript.command(name="res")
        @click.pass_context
        def resources(ctx):
            """Get resources associated with this jobscript."""
            click.echo(ctx.obj["jobscript"].resources.__dict__)

        @jobscript.command(name="deps")
        @click.pass_context
        def dependencies(ctx):
            """Get jobscript dependencies."""
            click.echo(ctx.obj["jobscript"].dependencies)

        @jobscript.command()
        @click.pass_context
        def path(ctx):
            """Get the file path to the jobscript."""
            click.echo(ctx.obj["jobscript"].jobscript_path)

        @jobscript.command()
        @click.pass_context
        def show(ctx):
            """Show the jobscript file."""
            with ctx.obj["jobscript"].jobscript_path.open("rt") as fp:
                click.echo(fp.read())

        jobscript.help = jobscript.help.format(app_name=self.name)

        return jobscript

    def _make_workflow_submission_CLI(self):
        """Generate the CLI for interacting with existing workflow submissions."""

        @click.group(name="sub")
        @click.pass_context
        @click.argument("sub_idx", type=click.INT)
        def submission(ctx, sub_idx):
            """Interact with existing {app_name} workflow submissions.

            SUB_IDX is the submission index.

            """
            ctx.obj["submission"] = ctx.obj["workflow"].submissions[sub_idx]

        @submission.command("status")
        @click.pass_context
        def status(ctx):
            """Get the submission status."""
            click.echo(ctx.obj["submission"].status.name.lower())

        @submission.command("submitted-js")
        @click.pass_context
        def submitted_JS(ctx):
            """Get a list of jobscript indices that have been submitted."""
            click.echo(ctx.obj["submission"].submitted_jobscripts)

        @submission.command("outstanding-js")
        @click.pass_context
        def outstanding_JS(ctx):
            """Get a list of jobscript indices that have not yet been submitted."""
            click.echo(ctx.obj["submission"].outstanding_jobscripts)

        @submission.command("needs-submit")
        @click.pass_context
        def needs_submit(ctx):
            """Check if this submission needs submitting."""
            click.echo(ctx.obj["submission"].needs_submit)

        submission.help = submission.help.format(app_name=self.name)
        submission.add_command(self._make_workflow_submission_jobscript_CLI())

        return submission

    def _make_workflow_CLI(self):
        """Generate the CLI for interacting with existing workflows."""

        @click.group()
        @click.argument("workflow_path", type=click.Path(exists=True))
        @click.pass_context
        def workflow(ctx, workflow_path):
            """Interact with existing {app_name} workflows.

            WORKFLOW_PATH is the path to an existing workflow.

            """
            wk = self.Workflow(workflow_path)
            ctx.ensure_object(dict)
            ctx.obj["workflow"] = wk

        @workflow.command(name="submit")
        @click.option(
            "--js-parallelism",
            help=(
                "If True, allow multiple jobscripts to execute simultaneously. Raises if "
                "set to True but the store type does not support the "
                "`jobscript_parallelism` feature. If not set, jobscript parallelism will "
                "be used if the store type supports it."
            ),
            type=click.BOOL,
        )
        @click.pass_context
        def submit_workflow(ctx, js_parallelism=None):
            """Submit the workflow."""
            ctx.obj["workflow"].submit(JS_parallelism=js_parallelism)

        @workflow.command(name="get-param")
        @click.argument("index", type=click.INT)
        @click.pass_context
        def get_parameter(ctx, index):
            """Get a parameter value by data index."""
            click.echo(ctx.obj["workflow"].get_parameter_data(index))

        @workflow.command(name="get-param-source")
        @click.argument("index", type=click.INT)
        @click.pass_context
        def get_parameter_source(ctx, index):
            """Get a parameter source by data index."""
            click.echo(ctx.obj["workflow"].get_parameter_source(index))

        @workflow.command(name="get-all-params")
        @click.pass_context
        def get_all_parameters(ctx):
            """Get all parameter values."""
            click.echo(ctx.obj["workflow"].get_all_parameter_data())

        @workflow.command(name="is-param-set")
        @click.argument("index", type=click.INT)
        @click.pass_context
        def is_parameter_set(ctx, index):
            """Check if a parameter specified by data index is set."""
            click.echo(ctx.obj["workflow"].is_parameter_set(index))

        @workflow.command(name="show-all-status")
        @click.pass_context
        def show_all_EAR_statuses(ctx):
            """Show the submission status of all workflow EARs."""
            ctx.obj["workflow"].show_all_EAR_statuses()

        workflow.help = workflow.help.format(app_name=self.name)

        workflow.add_command(self._make_workflow_submission_CLI())

        return workflow

    def _make_submission_CLI(self):
        """Generate the CLI for submission related queries."""

        def OS_info_callback(ctx, param, value):
            if not value or ctx.resilient_parsing:
                return
            click.echo(self.get_OS_info())
            ctx.exit()

        @click.group()
        @click.option(
            "--os-info",
            help="Print information about the operating system.",
            is_flag=True,
            is_eager=True,
            expose_value=False,
            callback=OS_info_callback,
        )
        @click.pass_context
        def submission(ctx):
            """Submission-related queries."""

        @submission.command("shell-info")
        @click.argument("shell_name", type=click.Choice(ALL_SHELLS))
        @click.option("--exclude-os", is_flag=True, default=False)
        @click.pass_context
        def shell_info(ctx, shell_name, exclude_os):
            click.echo(self.get_shell_info(shell_name, exclude_os))
            ctx.exit()

        return submission

    def _make_internal_CLI(self):
        """Generate the CLI for internal use."""

        @click.group()
        def internal(help=True):  # TEMP
            """Internal CLI to be invoked by scripts generated by the app."""
            pass

        @internal.group()
        @click.argument("path", type=click.Path(exists=True))
        @click.pass_context
        def workflow(ctx, path):
            """"""
            wk = self.Workflow(path)
            ctx.ensure_object(dict)
            ctx.obj["workflow"] = wk

        @workflow.command()
        @click.pass_context
        @click.argument("submission_idx", type=click.INT)
        @click.argument("jobscript_idx", type=click.INT)
        @click.argument("js_element_idx", type=click.INT)
        @click.argument("js_action_idx", type=click.INT)
        def write_commands(
            ctx,
            submission_idx: int,
            jobscript_idx: int,
            js_element_idx: int,
            js_action_idx: int,
        ):
            ctx.exit(
                ctx.obj["workflow"].write_commands(
                    submission_idx,
                    jobscript_idx,
                    js_element_idx,
                    js_action_idx,
                )
            )

        @workflow.command()
        @click.pass_context
        @click.argument("name")
        @click.argument("value")
        @click.argument("submission_idx", type=click.INT)
        @click.argument("jobscript_idx", type=click.INT)
        @click.argument("js_element_idx", type=click.INT)
        @click.argument("js_action_idx", type=click.INT)
        def save_parameter(
            ctx,
            name: str,
            value: str,
            submission_idx: int,
            jobscript_idx: int,
            js_element_idx: int,
            js_action_idx: int,
        ):
            ctx.exit(
                ctx.obj["workflow"].save_parameter(
                    name,
                    value,
                    submission_idx,
                    jobscript_idx,
                    js_element_idx,
                    js_action_idx,
                )
            )

        @workflow.command()
        @click.pass_context
        @click.argument("submission_idx", type=click.INT)
        @click.argument("jobscript_idx", type=click.INT)
        @click.argument("js_element_idx", type=click.INT)
        @click.argument("js_action_idx", type=click.INT)
        def set_EAR_start(
            ctx,
            submission_idx: int,
            jobscript_idx: int,
            js_element_idx: int,
            js_action_idx: int,
        ):
            ctx.exit(
                ctx.obj["workflow"].set_EAR_start(
                    submission_idx,
                    jobscript_idx,
                    js_element_idx,
                    js_action_idx,
                )
            )

        @workflow.command()
        @click.pass_context
        @click.argument("submission_idx", type=click.INT)
        @click.argument("jobscript_idx", type=click.INT)
        @click.argument("js_element_idx", type=click.INT)
        @click.argument("js_action_idx", type=click.INT)
        def set_EAR_end(
            ctx,
            submission_idx: int,
            jobscript_idx: int,
            js_element_idx: int,
            js_action_idx: int,
        ):
            ctx.exit(
                ctx.obj["workflow"].set_EAR_end(
                    submission_idx,
                    jobscript_idx,
                    js_element_idx,
                    js_action_idx,
                )
            )

        # TODO: in general, maybe the workflow command group can expose the simple Workflow
        # properties; maybe use a decorator on the Workflow property object to signify
        # inclusion?

        return internal

    def _make_CLI(self):
        """Generate the root CLI for the app."""

        colorama_init(autoreset=True)

        def run_time_info_callback(ctx, param, value):
            if not value or ctx.resilient_parsing:
                return
            click.echo(str(self.run_time_info))
            ctx.exit()

        @click.group(name=self.name)
        @click.version_option(
            version=self.version,
            package_name=self.name,
            prog_name=self.name,
            help=f"Show the version of {self.name} and exit.",
        )
        @click.version_option(
            __version__,
            "--hpcflow-version",
            help="Show the version of hpcflow and exit.",
            package_name="hpcflow",
            prog_name="hpcflow",
        )
        @click.help_option()
        @click.option(
            "--run-time-info",
            help="Print run-time information!",
            is_flag=True,
            is_eager=True,
            expose_value=False,
            callback=run_time_info_callback,
        )
        @click.option("--config-dir", help="Set the configuration directory.")
        @click.option(
            "--config-invocation-key", help="Set the configuration invocation key."
        )
        @click.option(
            "--with-config",
            help="Override a config item in the config file",
            nargs=2,
            multiple=True,
        )
        @click.pass_context
        def new_CLI(ctx, config_dir, config_invocation_key, with_config):
            overrides = {kv[0]: kv[1] for kv in with_config}
            try:
                self.load_config(
                    config_dir=config_dir,
                    config_invocation_key=config_invocation_key,
                    **overrides,
                )
            except ConfigError as err:
                click.echo(f"{colored(err.__class__.__name__, 'red')}: {err}")
                ctx.exit(1)

        new_CLI.__doc__ = self.description
        new_CLI.add_command(get_config_CLI(self))
        new_CLI.add_command(get_demo_software_CLI(self))
        new_CLI.add_command(get_helper_CLI(self))
        new_CLI.add_command(self._make_workflow_CLI())
        new_CLI.add_command(self._make_submission_CLI())
        new_CLI.add_command(self._make_internal_CLI())
        for cli_cmd in self._make_API_CLI():
            new_CLI.add_command(cli_cmd)

        return new_CLI

    def _load_scripts(self):
        # TODO: load custom directories / custom functions (via decorator)
        pkg = "hpcflow.sdk.demo.scripts"
        script_names = (
            name
            for name in resources.contents(pkg)
            if name != "__init__.py" and resources.is_resource(pkg, name)
        )
        scripts = {}
        for i in script_names:
            scripts[i] = resources.path(pkg, i)
        return scripts

    def template_components_from_json_like(self, json_like):
        cls_lookup = {
            "parameters": self.ParametersList,
            "command_files": self.CommandFilesList,
            "environments": self.EnvironmentsList,
            "task_schemas": self.TaskSchemasList,
        }
        tc = {}
        for k, v in cls_lookup.items():
            tc_k = v.from_json_like(
                json_like.get(k, {}),
                shared_data=tc,
                is_hashed=True,
            )
            tc[k] = tc_k
        return tc

    def get_info(self) -> Dict:
        return {
            "name": self.name,
            "version": self.version,
            "python_version": self.run_time_info.python_version,
            "is_frozen": self.run_time_info.is_frozen,
        }


class App(BaseApp):
    """Class to generate an hpcflow application (e.g. MatFlow)"""

    pass
