"""An hpcflow application."""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timezone
import enum
from functools import wraps
from importlib import resources, import_module
from logging import Logger
import os
from pathlib import Path
import socket
from typing import Any, Callable, Dict, List, Optional, Type, Union
import warnings
from platformdirs import user_data_dir
from reretry import retry
from rich.console import Console
from rich.table import Table, box
from rich.text import Text
from rich.padding import Padding

from setuptools import find_packages

from hpcflow import __version__
from hpcflow.sdk.core.errors import WorkflowNotFoundError
from hpcflow.sdk.core.object_list import ObjectList
from hpcflow.sdk.core.utils import read_YAML, read_YAML_file
from hpcflow.sdk import sdk_objs, sdk_classes, sdk_funcs, get_SDK_logger
from hpcflow.sdk.config import Config
from hpcflow.sdk.core import (
    ALL_TEMPLATE_FORMATS,
    DEFAULT_TEMPLATE_FORMAT,
)
from hpcflow.sdk.log import AppLog
from hpcflow.sdk.persistence import DEFAULT_STORE_FORMAT
from hpcflow.sdk.persistence.base import TEMPLATE_COMP_TYPES
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
    docs_import_conv:
        The convention for the app alias used in import statements in the documentation.
        E.g. for the `hpcflow` base app, this is `hf`. This is combined with `module` to
        form the complete import statement. E.g. for the `hpcflow` base app, the complete
        import statement is: `import hpcflow.app as hf`, where `hpcflow.app` is the
        `module` argument and `hf` is the `docs_import_conv` argument.

    """

    _known_subs_file_name = "known_submissions.txt"
    _known_subs_file_sep = "::"
    _submission_ts_fmt = r"%Y-%m-%d %H:%M:%S.%f"

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
        docs_import_conv=None,
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
        self.docs_import_conv = docs_import_conv

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
        for comp_type in TEMPLATE_COMP_TYPES:
            resource = f"{comp_type}.yaml"
            try:
                fh = resources.files(package).joinpath(resource).open("rt")
            except AttributeError:
                # < python 3.8; `resource.open_text` deprecated since 3.11
                fh = resources.open_text(package, resource)
            SDK_logger.info(f"Parsing file as YAML: {fh.name!r}")
            comp_dat = fh.read()
            components[comp_type] = read_YAML(comp_dat)
            fh.close()

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
    def persistence_logger(self) -> Logger:
        return self.logger.getChild("persistence")

    @property
    def submission_logger(self) -> Logger:
        return self.logger.getChild("submission")

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

    def perm_error_retry(self):
        """Return a decorator for retrying functions on permission and OS errors that
        might be associated with cloud-storage desktop sync. engine operations."""
        return retry(
            (PermissionError, OSError),
            tries=10,
            delay=1,
            backoff=2,
            logger=self.persistence_logger,
        )

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

        # TODO: setuptools.find_packages takes a long time to import
        packages = find_packages(
            where=str(Path(app_module.__path__[0], *root_scripts_dir.split(".")))
        )
        packages = [root_scripts_dir] + [root_scripts_dir + "." + i for i in packages]
        packages = [self.package_name + "." + i for i in packages]
        num_root_dirs = len(root_scripts_dir.split(".")) + 1

        scripts = {}
        for pkg in packages:
            try:
                contents = (
                    resource.name
                    for resource in resources.files(pkg).iterdir()
                    if resource.is_file()
                )
                _is_rsrc = lambda pkg, name: resources.files(pkg).joinpath(name).is_file()

            except AttributeError:
                # < python 3.8; `resource.contents` deprecated since 3.11
                contents = resources.contents(pkg)
                _is_rsrc = lambda pkg, name: resources.is_resource(pkg, name)

            script_names = (
                name for name in contents if name != "__init__.py" and _is_rsrc(pkg, name)
            )

            for i in script_names:
                script_key = "/".join(pkg.split(".")[num_root_dirs:] + [i])
                try:
                    script_ctx = resources.as_file(resources.files(pkg).joinpath(i))
                except AttributeError:
                    # < python 3.8; `resource.path` deprecated since 3.11
                    script_ctx = resources.path(pkg, i)

                with script_ctx as script:
                    scripts[script_key] = script

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

    def get_parameter_task_schema_map(self) -> Dict[str, List[List]]:
        """Get a dict mapping parameter types to task schemas that input/output each
        parameter."""

        param_map = {}
        for ts in self.task_schemas:
            for inp in ts.inputs:
                if inp.parameter.typ not in param_map:
                    param_map[inp.parameter.typ] = [[], []]
                param_map[inp.parameter.typ][0].append(ts.objective.name)
            for out in ts.outputs:
                if out.parameter.typ not in param_map:
                    param_map[out.parameter.typ] = [[], []]
                param_map[out.parameter.typ][1].append(ts.objective.name)

        return param_map

    def get_info(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "python_version": self.run_time_info.python_version,
            "is_frozen": self.run_time_info.is_frozen,
        }

    def get_user_data_dir(self):
        """We segregate by hostname to account for the case where multiple machines might
        use the same shared file system"""

        # This might need to cover e.g. multiple login nodes, as described in the
        # config file:
        host_dir_name = self.config.get("machine")
        return Path(user_data_dir(appname=self.package_name)).joinpath(host_dir_name)

    @property
    def known_subs_file_name(self):
        return self._known_subs_file_name

    @property
    def known_subs_file_path(self):
        return self.get_user_data_dir() / self.known_subs_file_name

    def _format_known_submissions_line(
        self, local_id, workflow_id, submit_time, sub_idx, is_active, wk_path
    ):
        line = [
            str(local_id),
            workflow_id,
            str(int(is_active)),
            str(sub_idx),
            submit_time,
            str(wk_path),
        ]
        return self._known_subs_file_sep.join(line) + "\n"

    def _parse_known_submissions_line(self, line: str) -> Dict:
        local_id, workflow_id, is_active, sub_idx, submit_time, path_i = line.split(
            self._known_subs_file_sep, maxsplit=5
        )
        item = {
            "local_id": int(local_id),
            "workflow_id": workflow_id,
            "is_active": bool(int(is_active)),
            "submit_time": submit_time,
            "sub_idx": int(sub_idx),
            "path": path_i.strip(),
        }
        return item

    def read_known_submissions_file(self) -> List[Dict]:
        """Retrieve existing workflows that *might* be running."""
        known = []
        with self.known_subs_file_path.open("rt", newline="\n") as fh:
            for ln in fh.readlines():
                known.append(self._parse_known_submissions_line(ln))
        return known

    def _add_to_known_submissions(
        self,
        wk_path: PathLike,
        wk_id: str,
        sub_idx: int,
        sub_time: str,
    ) -> int:
        """Ensure a the specified workflow submission is in the known-submissions file and
        return the associated local ID."""

        try:
            known = self.read_known_submissions_file()
        except FileNotFoundError:
            known = []

        wk_path = str(wk_path)
        all_ids = []
        for i in known:
            all_ids.append(i["local_id"])
            if (
                wk_path == i["path"]
                and sub_idx == i["sub_idx"]
                and sub_time == i["submit_time"]
            ):
                # workflow submission part already present
                return i["local_id"]

        # get the next available local ID:
        if all_ids:
            avail = set(range(0, max(all_ids) + 1)).difference(all_ids)
            next_id = min(avail) if avail else max(all_ids) + 1
        else:
            next_id = 0

        run_line = self._format_known_submissions_line(
            local_id=next_id,
            workflow_id=wk_id,
            is_active=True,
            submit_time=sub_time,
            sub_idx=sub_idx,
            wk_path=wk_path,
        )
        with self.known_subs_file_path.open("at", newline="\n") as fh:
            # TODO: check wk_path is an absolute path? what about if a remote fsspec path?
            self.submission_logger.info(
                f"adding to known-submissions file workflow path: {wk_path}"
            )
            fh.write(run_line)

        return next_id

    def set_inactive_in_known_subs_file(self, inactive_IDs: List[int]):
        """Set workflows in the known-submissions file to the non-running state.

        Note we aim for atomicity to help with the scenario where a new workflow
        submission is adding itself to the file at the same time as we have decided an
        existing workflow should no longer be part of this file. Ideally, such a scenario
        should not arise because both operations should only ever be interactively
        initiated by the single user (`Workflow.submit` and `App.get_known_submissions`). If this
        operation is atomic, then at least the known-submissions file should be left in a
        usable (but inaccurate) state.

        Returns
        -------
        removed_IDs
            List of local IDs removed from the known-submissions file due to the maximum
            number of recent workflows to store being exceeded.

        """

        self.submission_logger.info(
            f"setting these local IDs to inactive in known-submissions file: "
            f"{inactive_IDs}"
        )

        max_inactive = 10

        # keys are line indices of non-running submissions, values are submission
        # date-times:
        line_date = {}

        removed_IDs = []  # which submissions we completely remove from the file

        new_lines = []
        line_IDs = []
        for ln_idx, line in enumerate(self.known_subs_file_path.read_text().split("\n")):
            if not line.strip():
                continue
            item = self._parse_known_submissions_line(line)
            line_IDs.append(item["local_id"])
            is_active = item["is_active"]

            if item["local_id"] in inactive_IDs and is_active:
                # need to modify to set as inactive:
                non_run_line = self._format_known_submissions_line(
                    local_id=item["local_id"],
                    workflow_id=item["workflow_id"],
                    is_active=False,
                    submit_time=item["submit_time"],
                    sub_idx=item["sub_idx"],
                    wk_path=item["path"],
                )
                new_lines.append(non_run_line)
                is_active = False
                self.submission_logger.debug(
                    f"will set the following (workflow, submission) from the "
                    f"known-submissions file to inactive: "
                    f"({item['path']}, {item['sub_idx']})"
                )
            else:
                # leave this one alone:
                new_lines.append(line + "\n")

            if not is_active:
                line_date[ln_idx] = item["submit_time"]

        ld_srt_idx = list(dict(sorted(line_date.items(), key=lambda i: i[1])).keys())

        if len(line_date) > max_inactive:
            # remove oldest inactive submissions:
            num_remove = len(line_date) - max_inactive
            self.submission_logger.debug(
                f"will remove {num_remove} inactive workflow submissions from the "
                f"known-submissions file because the maximum number of stored inactive "
                f"workflows ({max_inactive}) has been exceeded."
            )

            # sort in reverse so we can remove indices from new_lines:
            oldest_idx = sorted(ld_srt_idx[:num_remove], reverse=True)
            for i in oldest_idx:
                new_lines.pop(i)
                removed_IDs.append(line_IDs.pop(i))

        # write the temp file:
        tmp_file = self.known_subs_file_path.with_suffix(
            self.known_subs_file_path.suffix + ".tmp"
        )
        with tmp_file.open("wt", newline="\n") as fh:
            fh.writelines(new_lines + [])

        # hopefully atomic rename:
        os.replace(src=tmp_file, dst=self.known_subs_file_path)
        self.submission_logger.debug("known-submissions file updated")

        return removed_IDs

    def clear_known_submissions_file(self):
        """Clear the known-submissions file of all submissions. This shouldn't be needed
        normally."""
        self.submission_logger.warning(
            f"clearing the known-submissions file at {self.known_subs_file_path}"
        )
        with self.known_subs_file_path.open("wt", newline="\n"):
            pass

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
        wait: Optional[bool] = False,
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
        wait
            If True, this command will block until the workflow execution is complete.
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
        return wk.submit(JS_parallelism=JS_parallelism, wait=wait)

    def _submit_workflow(
        self,
        workflow_path: PathLike,
        JS_parallelism: Optional[bool] = None,
        wait: Optional[bool] = False,
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
        return wk.submit(JS_parallelism=JS_parallelism, wait=wait)

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
            pkg = self.package_name
            res = "tests"
            try:
                test_dir_ctx = resources.as_file(resources.files(pkg).joinpath(res))
            except AttributeError:
                # < python 3.8; `resource.path` deprecated since 3.11
                test_dir_ctx = resources.path(pkg, res)

            with test_dir_ctx as test_dir:
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

    def _get_known_submissions(self, max_recent: int = 3, no_update: bool = False):
        """Retrieve information about active and recently inactive finished {app_name}
        workflows.

        This method removes workflows from the known-submissions file that are found to be
        inactive on this machine (according to the scheduler/process ID).

        Parameters
        ----------
        max_recent
            Maximum number of inactive workflows to retrieve.
        no_update
            If True, do not update the known-submissions file to set submissions that are
            now inactive.

        """

        out = []
        inactive_IDs = []

        try:
            known_subs = self.read_known_submissions_file()
        except FileNotFoundError:
            known_subs = []

        active_jobscripts = {}  # keys are (workflow path, submission index)
        loaded_workflows = {}  # keys are workflow path

        # loop in reverse so we process more-recent submissions first:
        for file_dat_i in known_subs[::-1]:
            submit_time_str = file_dat_i["submit_time"]
            out_item = {
                "local_id": file_dat_i["local_id"],
                "workflow_id": file_dat_i["workflow_id"],
                "workflow_path": file_dat_i["path"],
                "submit_time": submit_time_str,
                "sub_idx": file_dat_i["sub_idx"],
                "jobscripts": [],
                "active_jobscripts": {},
                "deleted": False,
            }
            if file_dat_i["path"] in loaded_workflows:
                wk_i = loaded_workflows[file_dat_i["path"]]
            else:
                try:
                    wk_i = self.Workflow(file_dat_i["path"])
                except WorkflowNotFoundError:
                    # might have been moved/archived/deleted
                    wk_i = None
                    out_item["deleted"] = True
                    if file_dat_i["is_active"]:
                        inactive_IDs.append(file_dat_i["local_id"])
                        file_dat_i["is_active"] = False
                else:
                    # cache:
                    loaded_workflows[file_dat_i["path"]] = wk_i

            if wk_i:
                if wk_i.id_ != file_dat_i["workflow_id"]:
                    # overwritten with a new workflow
                    if file_dat_i["is_active"]:
                        inactive_IDs.append(file_dat_i["local_id"])
                    out_item["deleted"] = True

                else:
                    sub = wk_i.submissions[file_dat_i["sub_idx"]]

                    all_jobscripts = sub._submission_parts[submit_time_str]
                    out_item.update(
                        {
                            "jobscripts": all_jobscripts,
                            "submission": sub,
                        }
                    )
                    if file_dat_i["is_active"]:
                        # check it really is active:
                        run_key = (file_dat_i["path"], file_dat_i["sub_idx"])
                        if run_key in active_jobscripts:
                            act_i_js = active_jobscripts[run_key]
                        else:
                            act_i_js = sub.get_active_jobscripts()
                            active_jobscripts[run_key] = act_i_js

                        out_item["active_jobscripts"] = {
                            k: v for k, v in act_i_js.items() if k in all_jobscripts
                        }
                        if not act_i_js and file_dat_i["is_active"]:
                            inactive_IDs.append(file_dat_i["local_id"])

            out.append(out_item)

        if inactive_IDs and not no_update:
            removed_IDs = self.set_inactive_in_known_subs_file(inactive_IDs)
            # remove these from the output, to avoid confusion (if kept, they would not
            # appear in the next invocation of this method):
            out = [i for i in out if i["local_id"] not in removed_IDs]

        # sort inactive by most-recently finished, then deleted:
        out_inactive = [i for i in out if not i["active_jobscripts"]]
        out_deleted = [i for i in out_inactive if i["deleted"]]
        out_not_deleted = [i for i in out_inactive if not i["deleted"]]

        # if an inactive submission has no end time, it was prematurely stopped, so put
        # these ones on top:
        out_not_deleted = sorted(
            out_not_deleted,
            key=lambda i: i["submission"].end_time or datetime.now().astimezone(),
            reverse=True,
        )
        out_inactive = (out_not_deleted + out_deleted)[:max_recent]

        out_active = [i for i in out if i["active_jobscripts"]]

        # show active submissions first:
        out = out_active + out_inactive

        return out

    def _show(
        self,
        max_recent: int = 3,
        full: bool = False,
        no_update: bool = False,
        columns=None,
    ):
        """Show information about running {app_name} workflows.

        Parameters
        ----------
        max_recent
            Maximum number of inactive workflows to show.
        full
            If True, provide more information; output may spans multiple lines for each
            workflow submission.
        no_update
            If True, do not update the known-submissions file to remove workflows that are
            no longer running.
        """

        # TODO: add --json to show, just returning this but without submissions?
        # TODO: add --legend to show to show what the EAR colours mean

        allowed_cols = {
            "id": "ID",
            "name": "Name",
            "status": "Status",
            "submit_time": "Submit",
            "start_time": "Start",
            "end_time": "End",
            "times": "Times",
            "actions": "Actions",
            "actions_compact": "Actions",
        }

        if full:
            columns = ("id", "name", "status", "times", "actions")

        else:
            columns = (
                "id",
                "name",
                "status",
                "submit_time",
                "start_time",
                "end_time",
                "actions_compact",
            )

        unknown_cols = set(columns) - set(allowed_cols.keys())
        if unknown_cols:
            raise ValueError(
                f"Unknown column names: {unknown_cols!r}. Allowed columns are "
                f"{list(allowed_cols.keys())!r}."
            )

        # TODO: add --filter option to filter by ID or name
        # TODO: add --sort option to sort by ID/name/start/end

        ts_fmt = r"%Y-%m-%d %H:%M:%S"
        ts_fmt_part = r"%H:%M:%S"

        console = Console()
        status = console.status("Retrieving data...")
        status.start()

        run_dat = self._get_known_submissions(
            max_recent=max_recent,
            no_update=no_update,
        )
        if not run_dat:
            status.stop()
            return

        status.update("Formatting...")
        table = Table(box=box.SQUARE, expand=False)
        for col_name in columns:
            table.add_column(allowed_cols[col_name])

        row_pad = 1 if full else 0

        for dat_i in run_dat:
            deleted = dat_i["deleted"]
            act_js = dat_i["active_jobscripts"]
            style = "grey42" if (deleted or not act_js) else ""
            style_wk_name = "grey42 strike" if deleted else style
            style_it = "italic grey42" if (deleted or not act_js) else "italic"

            all_cells = {}
            if "status" in columns:
                if act_js:
                    act_js_states = set([j for i in act_js.values() for j in i.values()])
                    status_text = "/".join(
                        f"[{i.colour}]{i.symbol}[/{i.colour}]" for i in act_js_states
                    )
                else:
                    status_text = Text(
                        "deleted" if deleted else "inactive", style=style_it
                    )
                all_cells["status"] = status_text

            if "id" in columns:
                all_cells["id"] = Text(str(dat_i["local_id"]), style=style)

            if "name" in columns:
                all_cells["name"] = Text(
                    Path(dat_i["workflow_path"]).name, style=style_wk_name
                )

            start_time, end_time = None, None
            if not deleted:
                start_time = dat_i["submission"].start_time
                end_time = dat_i["submission"].end_time if not act_js else None

            if "actions" in columns:
                if not deleted:
                    task_tab = Table(box=None, show_header=False)
                    task_tab.add_column()
                    task_tab.add_column()

                    for task_idx, elements in dat_i[
                        "submission"
                    ].EARs_by_elements.items():
                        task = dat_i["submission"].workflow.tasks[task_idx]

                        # inner table for elements/actions:
                        elem_tab_i = Table(box=None, show_header=False)
                        elem_tab_i.add_column()
                        for elem_idx, EARs in elements.items():
                            elem_status = Text(f"{elem_idx} | ", style=style)
                            for i in EARs:
                                elem_status.append("■", style=i.status.value)
                            elem_tab_i.add_row(elem_status)
                        task_tab.add_row(task.unique_name, elem_tab_i, style=style)
                else:
                    task_tab = ""

                all_cells["actions"] = Padding(task_tab, (0, 0, row_pad, 0))

            if "actions_compact" in columns:
                if not deleted:
                    EAR_stat_count = defaultdict(int)
                    for _, elements in dat_i["submission"].EARs_by_elements.items():
                        for elem_idx, EARs in elements.items():
                            for i in EARs:
                                EAR_stat_count[i.status.value] += 1
                    all_cells["actions_compact"] = " | ".join(
                        f"[{k}]■[/{k}]:{v}" for k, v in EAR_stat_count.items()
                    )
                else:
                    all_cells["actions_compact"] = ""

            if "submit_time" in columns or "times" in columns:
                submit_time = (
                    datetime.strptime(dat_i["submit_time"], self._submission_ts_fmt)
                    .replace(tzinfo=timezone.utc)
                    .astimezone()
                )
                submit_time_full = submit_time.strftime(ts_fmt)

            if "start_time" in columns or "times" in columns:
                start_time_full = start_time.strftime(ts_fmt) if start_time else "-"
                start_time_part = start_time_full
                if start_time and start_time.date() == submit_time.date():
                    start_time_part = start_time.strftime(ts_fmt_part)

            if "end_time" in columns or "times" in columns:
                end_time_full = end_time.strftime(ts_fmt) if end_time else "-"
                end_time_part = end_time_full
                if end_time and end_time.date() == start_time.date():
                    end_time_part = end_time.strftime(ts_fmt_part)

            if "submit_time" in columns:
                all_cells["submit_time"] = Padding(
                    Text(submit_time_full, style=style), (0, 0, row_pad, 0)
                )

            if "start_time" in columns:
                all_cells["start_time"] = Padding(
                    Text(start_time_part, style=style), (0, 0, row_pad, 0)
                )

            if "end_time" in columns:
                all_cells["end_time"] = Padding(
                    Text(end_time_part, style=style), (0, 0, row_pad, 0)
                )

            if "times" in columns:
                # submit/start/end on separate lines:
                times_tab = Table(box=None, show_header=False)
                times_tab.add_column()
                times_tab.add_column(justify="right")

                times_tab.add_row(
                    Text("sb.", style=style_it), Text(submit_time_full, style=style)
                )

                if start_time:
                    times_tab.add_row(
                        Text("st.", style=style_it), Text(start_time_part, style=style)
                    )
                if end_time:
                    times_tab.add_row(
                        Text("en.", style=style_it), Text(end_time_part, style=style)
                    )

                all_cells["times"] = Padding(times_tab, (0, 0, row_pad, 0))

            table.add_row(*[all_cells[i] for i in columns])

        status.stop()
        if table.row_count:
            console.print(table)

    def _get_workflow_path_from_local_ID(self, local_ID: int) -> Path:
        try:
            known_subs = self.read_known_submissions_file()
        except FileNotFoundError:
            known_subs = []

        path = None
        for i in known_subs:
            if i["local_id"] == local_ID:
                path = Path(i["path"])
                break
        if not path:
            raise ValueError(f"Specified local ID is not valid: {local_ID}.")

        return path

    def _resolve_workflow_reference(
        self, workflow_ref, ref_type: Union[str, None]
    ) -> Path:
        path = None
        if ref_type == "path":
            path = Path(workflow_ref)

        elif ref_type == "id":
            local_ID = int(workflow_ref)
            path = self._get_workflow_path_from_local_ID(local_ID)

        elif ref_type in ("assume-id", None):
            # see if reference is a valid local ID:
            is_local_ID = True
            try:
                local_ID = int(workflow_ref)
            except ValueError:
                is_local_ID = False
            else:
                try:
                    path = self._get_workflow_path_from_local_ID(local_ID)
                except ValueError:
                    pass

        if path is None:
            # see if reference is a valid path:
            is_path = True
            path = Path(workflow_ref)
            if not path.exists():
                is_path = False

            if is_path and is_local_ID:
                raise ValueError(
                    "Workflow reference appears to be both a valid path and a valid "
                    "local ID; set `ref_is_path` to True or False to disambiguate: "
                    f"{workflow_ref}."
                )
            elif not is_path and not is_local_ID:
                raise ValueError(
                    "Workflow reference appears to be neither a valid path or a valid "
                    f"local ID: {workflow_ref}."
                )
        return path.resolve()

    def _cancel(self, workflow_ref: Union[int, str, PathLike], ref_is_path=None):
        """Cancel the execution of a workflow submission.

        Parameters
        ----------
        ref_is_path
            One of "id", "path" or "assume-id" (the default)
        """
        path = self._resolve_workflow_reference(workflow_ref, ref_is_path)
        self.Workflow(path).cancel()


class App(BaseApp):
    """Class from which to instantiate downstream app objects (e.g. MatFlow)."""

    pass
