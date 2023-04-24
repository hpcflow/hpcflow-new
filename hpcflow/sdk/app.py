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
from .core.task_schema import TaskObjective
from .core.workflow import Workflow
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
        self.config = None
        self.run_time_info = RunTimeInfo(
            self.name, self.version, self.runtime_info_logger
        )

        self._builtin_template_components = template_components or {}

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

        all_funcs = [func_i for func_i in api.__dict__.values() if callable(func_i)]
        for func in all_funcs:

            if type(self) is BaseApp and func.__name__ == "run_hpcflow_tests":
                # this method provides the same functionality as the `run_tests` method
                continue

            SDK_logger.debug(f"Wrapping API callable: {func!r}")
            # allow sub-classes to override API functions:
            if not hasattr(self, func.__name__):
                api_method = get_api_method(func)
                api_method = wraps(func)(api_method)
                api_method.__doc__ = func.__doc__.format(name=name)
                setattr(self, func.__name__, api_method)

    @property
    def _template_component_classes(self):
        return {
            "parameters": self.ParametersList,
            "command_files": self.CommandFilesList,
            "environments": self.EnvironmentsList,
            "task_schemas": self.TaskSchemasList,
        }

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
        return type(cls.__name__, (cls,), {getattr(cls, "_app_attr"): self})

    def _assign_core_classes(self):

        # ensure classes defined in `object_list` are included in core classes:
        import_module("hpcflow.sdk.core.object_list")

        core_classes = list(self._get_core_JSONLike_classes())

        # Non-`JSONLike` classes:
        core_classes += [
            ActionScopeType,
            Element,
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
        return bool(self.config)

    @property
    def is_template_components_loaded(self):
        return bool(self._parameters)

    def _load_config(self, config_dir, **overrides):
        self.logger.debug("Loading configuration.")
        self.config = Config(
            app=self,
            options=self.config_options,
            config_dir=config_dir,
            logger=self.config_logger,
            **overrides,
        )
        self.logger.info(f"Configuration loaded from: {self.config.config_file_path}")

    def load_config(self, config_dir=None, **overrides):
        if self.is_config_loaded:
            warnings.warn("Configuration is already loaded; reloading.")
        self._load_config(config_dir, **overrides)

    def reload_config(self, config_dir=None, **overrides):
        if not self.is_config_loaded:
            warnings.warn("Configuration is not loaded; loading.")
        self._load_config(config_dir, **overrides)

    def _make_API_CLI(self):
        """Generate the CLI for the main functionality."""

        @click.command(help=f"Generate a new {self.name} workflow")
        @click.argument("template_file")
        @click.argument("directory")
        def make_workflow(template_file, directory):
            return self.make_workflow(template_file, directory)

        @click.command(help=f"Generate and submit a new {self.name} workflow")
        @click.argument("template_file")
        @click.argument("directory")
        def submit_workflow(template_file, directory):
            return self.submit_workflow(template_file, directory)

        @click.command(help=f"Run {self.name} test suite.")
        def test():
            self.run_tests()
            time.sleep(10)

        @click.command(help=f"Run hpcflow test suite.")
        def test_hpcflow():
            self.run_hpcflow_tests()

        commands = [
            make_workflow,
            submit_workflow,
            test,
        ]

        if type(self) is not BaseApp:
            # `test_hpcflow` is the same as `test` for the BaseApp so no need to add both:
            commands.append(test_hpcflow)

        return commands

    def _make_workflow_CLI(self):
        """Generate the CLI for interacting with existing workflows."""

        @click.group()
        @click.argument("path", type=click.Path(exists=True))
        @click.pass_context
        def workflow(ctx, path):
            """"""
            wk = self.Workflow(path)
            ctx.ensure_object(dict)
            ctx.obj["workflow"] = wk

        return workflow

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
            "--with-config",
            help="Override a config item in the config file",
            nargs=2,
            multiple=True,
        )
        @click.pass_context
        def new_CLI(ctx, config_dir, with_config):
            overrides = {kv[0]: kv[1] for kv in with_config}
            try:
                self.load_config(config_dir=config_dir, **overrides)
            except ConfigError as err:
                click.echo(f"{colored(err.__class__.__name__, 'red')}: {err}")
                ctx.exit(1)

        new_CLI.__doc__ = self.description
        new_CLI.add_command(get_config_CLI(self))
        new_CLI.add_command(get_demo_software_CLI(self))
        new_CLI.add_command(get_helper_CLI(self))
        new_CLI.add_command(self._make_workflow_CLI())
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


class App(BaseApp):
    """Class to generate an hpcflow application (e.g. MatFlow)"""

    pass
