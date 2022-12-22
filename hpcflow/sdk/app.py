"""An hpcflow application."""

from functools import wraps
from importlib import resources
import time
import warnings

import click
from colorama import init as colorama_init
from termcolor import colored

from hpcflow import __version__
from .core.json_like import JSONLike
from .core.utils import read_YAML_file
from . import api, SDK_logger
from .config import Config
from .config.cli import get_config_CLI
from .config.errors import ConfigError
from .core.actions import Action, ActionScopeType, ElementAction
from .core.element import Element
from .core.environment import Executable, NumCores
from .core.zarr_io import ZarrEncodable
from .core.parameters import (
    InputSourceMode,
    InputSourceType,
    ParameterPropagationMode,
    TaskSourceType,
    ValueSequence,
)
from .core.task import WorkflowTask
from .core.task_schema import TaskObjective
from .core.workflow import Workflow
from .demo.cli import get_demo_software_CLI
from .helper.cli import get_helper_CLI
from .log import AppLog
from .runtime import RunTimeInfo

SDK_logger = SDK_logger.getChild(__name__)


class BaseApp:
    """Class to generate the base hpcflow application."""

    def __init__(
        self,
        name,
        version,
        description,
        config_options,
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

        # Set by `_load_data_files`:
        self._parameters = None
        self._command_files = None
        self._envs = None
        self._task_schemas = None
        self_scripts = None
        self._app_data = {}

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

    def _get_core_JSONLike_classes(self):
        """Get all JSONLike subclasses (recursively)."""

        def all_subclasses(cls):
            return set(cls.__subclasses__()).union(
                [s for c in cls.__subclasses__() for s in all_subclasses(c)]
            )

        return all_subclasses(JSONLike)

    def inject_into(self, cls):
        SDK_logger.debug(f"Injecting app {self!r} into class {cls.__name__}")
        return type(cls.__name__, (cls,), {getattr(cls, "_app_attr"): self})

    def _assign_core_classes(self):

        core_classes = list(self._get_core_JSONLike_classes())

        # Non-`JSONLike` classes:
        core_classes += [
            ActionScopeType,
            Action,
            Executable,
            Element,
            ElementAction,
            InputSourceMode,
            InputSourceType,
            NumCores,
            ParameterPropagationMode,
            TaskObjective,
            TaskSourceType,
            ValueSequence,
            Workflow,
            WorkflowTask,
            ZarrEncodable,
        ]
        for cls in core_classes:
            if hasattr(cls, "_app_attr"):
                setattr(self, cls.__name__, self.inject_into(cls))
            else:
                setattr(self, cls.__name__, cls)

        return tuple(
            sorted(
                core_classes,
                key=lambda x: f"{x.__module__}.{x.__qualname__}",
            )
        )

    def _ensure_data_files(self):
        if not self.is_data_files_loaded:
            self._load_data_files()

    def _load_data_files(self):
        if not self.is_config_loaded:
            self.load_config()

        self._parameters = self._load_parameters()
        self._app_data["parameters"] = self._parameters

        self._command_files = self._load_command_files()
        self._app_data["command_files"] = self._command_files

        self._envs = self._load_environments()
        self._app_data["envs"] = self._envs

        self._task_schemas = self._load_task_schemas()
        self._app_data["task_schemas"] = self._task_schemas

        self._scripts = self._load_scripts()
        self._app_data["scripts"] = self._scripts

        self.logger.info("Data files loaded.")

    def load_data_files(self):
        if self.is_data_files_loaded:
            warnings.warn("Data files already loaded; reloading.")
        self._load_data_files()

    def reload_data_files(self):
        if not self.is_data_files_loaded:
            warnings.warn("Data files not loaded; loading.")
        self._load_data_files()

    @property
    def app_data(self):
        return self._app_data

    @property
    def task_schemas(self):
        self._ensure_data_files()
        return self._task_schemas

    @property
    def parameters(self):
        self._ensure_data_files()
        return self._parameters

    @property
    def envs(self):
        self._ensure_data_files()
        return self._envs

    @property
    def command_files(self):
        self._ensure_data_files()
        return self._command_files

    @property
    def scripts(self):
        self._ensure_data_files()
        return self._scripts

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
    def is_data_files_loaded(self):
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
            # `test_hpcflow` is the same as `test` for the BaseApp
            commands.append(test_hpcflow)

        return commands

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
            version=self.version, package_name=self.name, prog_name=self.name
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
        for cli_cmd in self._make_API_CLI():
            new_CLI.add_command(cli_cmd)

        return new_CLI

    def _load_environments(self):

        all_envs = []
        for path in self.config.environment_files:
            all_envs.extend(read_YAML_file(path))

        return self.EnvironmentsList.from_json_like(all_envs)

    def _load_command_files(self):

        all_files = []
        for path in self.config.file_files:
            all_files.extend(read_YAML_file(path))

        return self.CommandFilesList.from_json_like(all_files)

    def _load_parameters(self):

        all_params = []
        for path in self.config.parameter_files:
            all_params.extend(read_YAML_file(path))

        return self.ParametersList.from_json_like(all_params)

    def _load_task_schemas(self):

        all_ts = []
        for path in self.config.task_schema_files:
            all_ts.extend(read_YAML_file(path))

        return self.TaskSchemasList.from_json_like(all_ts)

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

    def shared_data_from_json_like(self, json_like):
        cls_lookup = {
            "command_files": self.CommandFilesList,
            "envs": self.EnvironmentsList,
            "parameters": self.ParametersList,
            "task_schemas": self.TaskSchemasList,
        }
        shared_data = {}
        for k, v in cls_lookup.items():
            shared_data[k] = v.from_json_like(json_like.get(k, {}), is_hashed=True)

        return shared_data


class App(BaseApp):
    """Class to generate an hpcflow application (e.g. MatFlow)"""

    pass
