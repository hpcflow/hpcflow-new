"""An hpcflow application."""

import enum
from functools import wraps
from importlib import resources, import_module
from pathlib import Path
from typing import Callable, Dict, Type
import warnings

from setuptools import find_packages

from hpcflow import __version__
from hpcflow.sdk.core.utils import read_YAML, read_YAML_file
from hpcflow.sdk import sdk_classes, sdk_funcs, get_SDK_logger
from hpcflow.sdk.config import Config
from hpcflow.sdk.log import AppLog
from hpcflow.sdk.runtime import RunTimeInfo
from hpcflow.sdk.cli import make_cli

SDK_logger = get_SDK_logger(__name__)

_sdk_objs = {**sdk_classes, **sdk_funcs}


def __getattr__(name):
    """Allow access to core classes and API functions (useful for type annotations)."""
    try:
        return get_app_attribute(name)
    except AttributeError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}.")


def get_app_attribute(name):
    """A function to assign to an app module `__getattr__` to access app attributes."""
    app_obj = App.get_instance()
    try:
        return getattr(app_obj, name)
    except AttributeError:
        raise AttributeError(f"module {app_obj.module!r} has no attribute {name!r}.")


def get_app_module_all():
    return ["app"] + list(_sdk_objs.keys())


def get_app_module_dir():
    return lambda: sorted(get_app_module_all())


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    def get_instance(cls):
        """Retrieve the instance of the singleton class if initialised."""
        try:
            return cls._instances[cls]
        except KeyError:
            raise RuntimeError(f"{cls.__name__!r} object has not be instantiated!")


class App(metaclass=Singleton):
    """Class to generate the hpcflow application.

    Parameters
    ----------
    module:
        The module name in which the app object is defined.

    """

    _app_attr_cache = {}
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

        self.log = AppLog(self)
        self.run_time_info = RunTimeInfo(
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

    def __getattr__(self, name):
        if name in sdk_classes:
            return self._get_app_core_class(name)
        elif name in sdk_funcs:
            return self._get_app_func(name)
        else:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}.")

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, version={self.version})"

    def _get_app_attribute(self, name: str) -> Type:
        obj_mod = import_module(_sdk_objs[name])
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
        def wrap_func(func):
            # this function avoids scope issues
            return lambda *args, **kwargs: func(*args, **kwargs)

        if name not in self._app_attr_cache:
            sdk_func = self._get_app_attribute(name)
            func = wrap_func(sdk_func)
            func = wraps(sdk_func)(func)
            if func.__doc__:
                func.__doc__ = func.__doc__.format(app_name=self.name)
            func.__module__ = self.module
            self._app_attr_cache[name] = func
        return self._app_attr_cache[name]

    @property
    def template_components(self):
        if not self.is_template_components_loaded:
            self._load_template_components()
        return self._template_components

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
        self_tc["scripts"] = self._load_scripts()

        self._parameters = self_tc["parameters"]
        self._command_files = self_tc["command_files"]
        self._environments = self_tc["environments"]
        self._task_schemas = self_tc["task_schemas"]
        self._scripts = self_tc["scripts"]

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

    def load_config(self, config_dir=None, config_invocation_key=None, **overrides):
        if self.is_config_loaded:
            warnings.warn("Configuration is already loaded; reloading.")
        self._load_config(config_dir, config_invocation_key, **overrides)

    def reload_config(self, config_dir=None, config_invocation_key=None, **overrides):
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
