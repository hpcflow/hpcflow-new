"""An hpcflow application."""
from __future__ import annotations

import enum
from functools import partial, wraps
from importlib import resources, import_module
import importlib
from logging import Logger
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type, Union
import warnings

from setuptools import find_packages

from hpcflow import __version__
from hpcflow.sdk.core.object_list import ObjectList
from hpcflow.sdk.core.utils import read_YAML, read_YAML_file
from hpcflow.sdk import sdk_objs, sdk_classes, sdk_funcs, get_SDK_logger
from hpcflow.sdk.config import Config
from hpcflow.sdk.core.workflow import ALL_TEMPLATE_FORMATS, DEFAULT_TEMPLATE_FORMAT
from hpcflow.sdk.log import AppLog
from hpcflow.sdk.persistence import DEFAULT_STORE_FORMAT
from hpcflow.sdk.runtime import RunTimeInfo
from hpcflow.sdk.cli import make_cli
from hpcflow.sdk.submission.shells import get_shell
from hpcflow.sdk.submission.shells.os_version import (
    get_OS_info_POSIX,
    get_OS_info_windows,
)
from hpcflow.sdk.typing import PathLike

SDK_logger = get_SDK_logger(__name__)


def __getattr__(name):
    """Allow access to core classes and API functions (useful for type annotations)."""
    try:
        return get_app_attribute(name)
    except AttributeError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}.")


def get_app_attribute(name):
    """A function to assign to an app module `__getattr__` to access app attributes."""
    try:
        app_obj = App.get_instance()
    except RuntimeError:
        app_obj = BaseApp.get_instance()
    try:
        return getattr(app_obj, name)
    except AttributeError:
        raise AttributeError(f"module {app_obj.module!r} has no attribute {name!r}.")


def get_app_module_all():
    return ["app"] + list(sdk_objs.keys())


def get_app_module_dir():
    return lambda: sorted(get_app_module_all())


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        SDK_logger.info(f"App metaclass __call__ with {args=} {kwargs=}")
        if cls not in cls._instances:
            SDK_logger.info(f"App metaclass initialising new object {kwargs['name']!r}.")
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    def get_instance(cls):
        """Retrieve the instance of the singleton class if initialised."""
        try:
            return cls._instances[cls]
        except KeyError:
            raise RuntimeError(f"{cls.__name__!r} object has not be instantiated!")


class BaseApp(metaclass=Singleton):
    """Class to generate the hpcflow application.

    Parameters
    ----------
    module:
        The module name in which the app object is defined.

    """

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
        module,
        description,
        config_options,
        scripts_dir,
        template_components: Dict = None,
        pytest_args=None,
        package_name=None,
    ):
        SDK_logger.info(f"Generating {self.__class__.__name__} {name!r}.")

        self.name = name
        self.package_name = package_name or name.lower()
        self.version = version
        self.module = module
        self.description = description
        self.config_options = config_options
        self.pytest_args = pytest_args
        self.scripts_dir = scripts_dir

        self.cli = make_cli(self)

        self._log = AppLog(self)
        self._run_time_info: RunTimeInfo = RunTimeInfo(
            self.name,
            self.package_name,
            self.version,
            self.runtime_info_logger,
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

        self._app_attr_cache = {}

    def __getattr__(self, name):
        if name in sdk_classes:
            return self._get_app_core_class(name)
        elif name in sdk_funcs:
            return self._get_app_func(name)
        else:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}.")

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name!r}, version={self.version!r})"

    def _get_app_attribute(self, name: str) -> Type:
        obj_mod = import_module(sdk_objs[name])
        return getattr(obj_mod, name)

    def _get_app_core_class(self, name: str) -> Type:
        if name not in self._app_attr_cache:
            cls = self._get_app_attribute(name)
            if issubclass(cls, enum.Enum):
                sub_cls = cls
            else:
                dct = {}
                if hasattr(cls, "_app_attr"):
                    dct = {getattr(cls, "_app_attr"): self}
                sub_cls = type(cls.__name__, (cls,), dct)
                if cls.__doc__:
                    sub_cls.__doc__ = cls.__doc__.format(app_name=self.name)
            sub_cls.__module__ = self.module
            self._app_attr_cache[name] = sub_cls

        return self._app_attr_cache[name]

    def _get_app_func(self, name) -> Callable:
        if name not in self._app_attr_cache:

            def wrap_func(func):
                # this function avoids scope issues
                return lambda *args, **kwargs: func(*args, **kwargs)

            # retrieve the "private" function:
            sdk_func = getattr(self, f"_{name}")

            func = wrap_func(sdk_func)
            func = wraps(sdk_func)(func)
            if func.__doc__:
                func.__doc__ = func.__doc__.format(app_name=self.name)
            func.__module__ = self.module
            self._app_attr_cache[name] = func

        return self._app_attr_cache[name]

    @property
    def run_time_info(self) -> RunTimeInfo:
        return self._run_time_info

    @property
    def log(self) -> AppLog:
        return self._log

    @property
    def template_components(self) -> Dict[str, ObjectList]:
        if not self.is_template_components_loaded:
            self._load_template_components()
        return self._template_components

    def _ensure_template_components(self) -> None:
        if not self.is_template_components_loaded:
            self._load_template_components()

    def load_template_components(self, warn=True) -> None:
        if warn and self.is_template_components_loaded:
            warnings.warn("Template components already loaded; reloading now.")
        self._load_template_components()

    def reload_template_components(self, warn=True) -> None:
        if warn and not self.is_template_components_loaded:
            warnings.warn("Template components not loaded; loading now.")
        self._load_template_components()

    def _load_template_components(self) -> None:
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
        self_tc["scripts"] = self._load_scripts()

        self._parameters = self_tc["parameters"]
        self._command_files = self_tc["command_files"]
        self._environments = self_tc["environments"]
        self._task_schemas = self_tc["task_schemas"]
        self._scripts = self_tc["scripts"]

        self.logger.info("Template components loaded.")

    @classmethod
    def load_builtin_template_component_data(
        cls, package
    ) -> Dict[str, Union[List, Dict]]:
        SDK_logger.info(
            f"Loading built-in template component data for package: {package!r}."
        )
        components = {}
        for comp_type in cls._template_component_types:
            with resources.open_text(package, f"{comp_type}.yaml") as fh:
                SDK_logger.info(f"Parsing file as YAML: {fh.name!r}")
                comp_dat = fh.read()
                components[comp_type] = read_YAML(comp_dat)
        return components

    @property
    def parameters(self) -> get_app_attribute("ParametersList"):
        self._ensure_template_components()
        return self._parameters

    @property
    def command_files(self) -> get_app_attribute("CommandFilesList"):
        self._ensure_template_components()
        return self._command_files

    @property
    def envs(self) -> get_app_attribute("EnvironmentsList"):
        self._ensure_template_components()
        return self._environments

    @property
    def scripts(self):
        self._ensure_template_components()
        return self._scripts

    @property
    def task_schemas(self) -> get_app_attribute("TaskSchemasList"):
        self._ensure_template_components()
        return self._task_schemas

    @property
    def logger(self) -> Logger:
        return self.log.logger

    @property
    def API_logger(self) -> Logger:
        return self.logger.getChild("api")

    @property
    def CLI_logger(self) -> Logger:
        return self.logger.getChild("cli")

    @property
    def config_logger(self) -> Logger:
        return self.logger.getChild("config")

    @property
    def runtime_info_logger(self) -> Logger:
        return self.logger.getChild("runtime")

    @property
    def is_config_loaded(self) -> bool:
        return bool(self._config)

    @property
    def is_template_components_loaded(self) -> bool:
        return bool(self._parameters)

    @property
    def config(self) -> Config:
        if not self.is_config_loaded:
            self.load_config()
        return self._config

    def _load_config(self, config_dir, config_invocation_key, **overrides) -> None:
        self.logger.info("Loading configuration.")
        self._config = Config(
            app=self,
            options=self.config_options,
            config_dir=config_dir,
            config_invocation_key=config_invocation_key,
            logger=self.config_logger,
            variables={"app_name": self.name, "app_version": self.version},
            **overrides,
        )
        self.log.update_console_level(self.config.get("log_console_level"))
        self.log.add_file_logger(
            path=self.config.get("log_file_path"),
            level=self.config.get("log_file_level"),
        )
        self.logger.info(f"Configuration loaded from: {self.config.config_file_path}")

    def load_config(
        self,
        config_dir=None,
        config_invocation_key=None,
        **overrides,
    ) -> None:
        if self.is_config_loaded:
            warnings.warn("Configuration is already loaded; reloading.")
        self._load_config(config_dir, config_invocation_key, **overrides)

    def reload_config(
        self,
        config_dir=None,
        config_invocation_key=None,
        **overrides,
    ) -> None:
        if not self.is_config_loaded:
            warnings.warn("Configuration is not loaded; loading.")
        self._load_config(config_dir, config_invocation_key, **overrides)

    def _load_scripts(self):
        # TODO: load custom directories / custom functions (via decorator)

        app_module = import_module(self.package_name)
        root_scripts_dir = self.scripts_dir

        packages = find_packages(
            where=str(Path(app_module.__path__[0], *root_scripts_dir.split(".")))
        )
        packages = [root_scripts_dir] + [root_scripts_dir + "." + i for i in packages]
        packages = [self.package_name + "." + i for i in packages]
        num_root_dirs = len(root_scripts_dir.split(".")) + 1

        scripts = {}
        for pkg in packages:
            script_names = (
                name
                for name in resources.contents(pkg)
                if name != "__init__.py" and resources.is_resource(pkg, name)
            )
            for i in script_names:
                script_key = "/".join(pkg.split(".")[num_root_dirs:] + [i])
                scripts[script_key] = resources.path(pkg, i)

        return scripts

    def template_components_from_json_like(self, json_like) -> None:
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

    def get_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "python_version": self.run_time_info.python_version,
            "is_frozen": self.run_time_info.is_frozen,
        }

    def _make_workflow(
        self,
        template_file_or_str: Union[PathLike, str],
        is_string: Optional[bool] = False,
        template_format: Optional[str] = DEFAULT_TEMPLATE_FORMAT,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> get_app_attribute("Workflow"):
        """Generate a new {app_name} workflow from a file or string containing a workflow
        template parametrisation.

        Parameters
        ----------
        template_path_or_str
            Either a path to a template file in YAML or JSON format, or a YAML/JSON string.
        is_string
            Determines if passing a file path or a string.
        template_format
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the workflow template name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store type to use.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        """

        self.API_logger.info("make_workflow called")

        common = {
            "path": path,
            "name": name,
            "overwrite": overwrite,
            "store": store,
            "ts_fmt": ts_fmt,
            "ts_name_fmt": ts_name_fmt,
        }

        if not is_string:
            wk = self.Workflow.from_file(
                template_path=template_file_or_str,
                template_format=template_format,
                **common,
            )

        elif template_format == "json":
            wk = self.Workflow.from_JSON_string(JSON_str=template_file_or_str, **common)

        elif template_format == "yaml":
            wk = self.Workflow.from_YAML_string(YAML_str=template_file_or_str, **common)

        else:
            raise ValueError(
                f"Template format {template_format} not understood. Available template "
                f"formats are {ALL_TEMPLATE_FORMATS!r}."
            )
        return wk

    def _make_and_submit_workflow(
        self,
        template_file_or_str: Union[PathLike, str],
        is_string: Optional[bool] = False,
        template_format: Optional[str] = DEFAULT_TEMPLATE_FORMAT,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        JS_parallelism: Optional[bool] = None,
    ) -> Dict[int, int]:
        """Generate and submit a new {app_name} workflow from a file or string containing a
        workflow template parametrisation.

        Parameters
        ----------

        template_path_or_str
            Either a path to a template file in YAML or JSON format, or a YAML/JSON string.
        is_string
            Determines whether `template_path_or_str` is a string or a file.
        template_format
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        JS_parallelism
            If True, allow multiple jobscripts to execute simultaneously. Raises if set to
            True but the store type does not support the `jobscript_parallelism` feature. If
            not set, jobscript parallelism will be used if the store type supports it.
        """

        self.API_logger.info("make_and_submit_workflow called")

        wk = self.make_workflow(
            template_file_or_str=template_file_or_str,
            is_string=is_string,
            template_format=template_format,
            path=path,
            name=name,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
        )
        return wk.submit(JS_parallelism=JS_parallelism)

    def _submit_workflow(
        self, workflow_path: PathLike, JS_parallelism: Optional[bool] = None
    ) -> Dict[int, int]:
        """Submit an existing {app_name} workflow.

        Parameters
        ----------
        workflow_path
            Path to an existing workflow
        JS_parallelism
            If True, allow multiple jobscripts to execute simultaneously. Raises if set to
            True but the store type does not support the `jobscript_parallelism` feature. If
            not set, jobscript parallelism will be used if the store type supports it.
        """

        self.API_logger.info("submit_workflow called")
        wk = self.Workflow(workflow_path)
        return wk.submit(JS_parallelism=JS_parallelism)

    def _run_hpcflow_tests(self, *args):
        """Run hpcflow test suite. This function is only available from derived apps."""

        from hpcflow import app as hf

        return hf.app.run_tests(*args)

    def _run_tests(self, *args):
        """Run {app_name} test suite."""

        try:
            import pytest
        except ModuleNotFoundError:
            raise RuntimeError(
                f"{self.name} has not been built with testing dependencies."
            )

        test_args = (self.pytest_args or []) + list(args)
        if self.run_time_info.is_frozen:
            with importlib.resources.path(self.package_name, "tests") as test_dir:
                return pytest.main([str(test_dir)] + test_args)
        else:
            ret_code = pytest.main(["--pyargs", f"{self.package_name}"] + test_args)
            if ret_code is not pytest.ExitCode.OK:
                raise RuntimeError(f"Tests failed with exit code: {str(ret_code)}")
            else:
                return ret_code

    def _get_OS_info(self) -> Dict:
        """Get information about the operating system."""
        os_name = os.name
        if os_name == "posix":
            return get_OS_info_POSIX(
                linux_release_file=self.config.get("linux_release_file")
            )
        elif os_name == "nt":
            return get_OS_info_windows()

    def _get_shell_info(
        self,
        shell_name: str,
        exclude_os: Optional[bool] = False,
    ) -> Dict:
        """Get information about a given shell and the operating system.

        Parameters
        ----------
        shell_name
            One of the supported shell names.
        exclude_os
            If True, exclude operating system information.
        """
        shell = get_shell(
            shell_name=shell_name,
            os_args={"linux_release_file": self.config.linux_release_file},
        )
        return shell.get_version_info(exclude_os)


class App(BaseApp):
    """Class from which to instantiate downstream app objects (e.g. MatFlow)."""

    pass
