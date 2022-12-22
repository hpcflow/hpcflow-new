from __future__ import annotations

import copy
import functools
import json
import logging
import socket
import uuid
from dataclasses import dataclass, field
from hashlib import new
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from fsspec.registry import known_implementations as fsspec_protocols
from platformdirs import user_data_dir
from valida.schema import Schema

from hpcflow.sdk.core.validation import get_schema
from hpcflow.sdk.log import AppLog

from .callbacks import (
    callback_bool,
    callback_file_paths,
    set_callback_file_paths,
    check_load_data_files,
)
from .config_file import ConfigFile
from .errors import (
    ConfigChangeInvalidJSONError,
    ConfigChangePopIndexError,
    ConfigChangeTypeInvalidError,
    ConfigChangeValidationError,
    ConfigItemAlreadyUnsetError,
    ConfigItemCallbackError,
    ConfigNonConfigurableError,
    ConfigUnknownItemError,
    ConfigUnknownOverrideError,
    ConfigValidationError,
)

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE = {
    "configs": {
        "default": {
            "invocation": {"environment_setup": None, "match": {}},
            "config": {
                "machine": socket.gethostname(),
                "telemetry": True,
                "log_file_path": "logs/app.log",
                "environment_files": [],
                "task_schema_files": [],
                "file_files": [],
                "parameter_files": [],
            },
        }
    }
}


@dataclass
class ConfigOptions:
    """Application-level options for configuration"""

    default_directory: Union[Path, str]
    directory_env_var: str
    sentry_DSN: str
    sentry_traces_sample_rate: float
    sentry_env: str
    default_config: Optional[Dict] = field(
        default_factory=lambda: copy.deepcopy(DEFAULT_CONFIG_FILE)
    )
    extra_schemas: Optional[List[Schema]] = field(default_factory=lambda: [])


class Config:
    """Application configuration as defined in one or more config files.

    Methods
    -------
    to_string : get a formatted string
    get_configurable : list all configurable items
    get : get a configuration item
    set_value : set a configuration item
    unset_value : unset a configuration item
    append_value : append a value to a list configuration item
    prepend_value : prepend a value to a list configuration item
    pop_value : pop a value from a list configuration item

    Notes
    -----
    On modifying/setting existing values, modifications are not automatically copied
    to the configuration file; use `save()` to save to the file. Items in `overrides`
    are not saved into the file.

    """

    def __init__(
        self,
        app,
        options: ConfigOptions,
        config_dir,
        logger,
        uid=None,
        callbacks=None,
        **overrides,
    ):

        self._app = app
        self._options = options
        self._overrides = overrides
        self._logger = logger

        # Callbacks are run on get:
        self._get_callbacks = {
            "task_schema_files": (callback_file_paths,),
            "environment_files": (callback_file_paths,),
            "parameter_files": (callback_file_paths,),
            "file_files": (callback_file_paths,),
            "log_file_path": (callback_file_paths,),
            "telemetry": (callback_bool,),
            **(callbacks or {}),
        }

        # Set callbacks are run on set:
        self._set_callbacks = {
            "task_schema_files": (set_callback_file_paths, check_load_data_files),
            "environment_files": (set_callback_file_paths, check_load_data_files),
            "parameter_files": (set_callback_file_paths, check_load_data_files),
            "file_files": (set_callback_file_paths, check_load_data_files),
            "log_file_path": (set_callback_file_paths,),
        }

        cfg_schemas, cfg_keys = self._init_schemas()
        self._schemas = cfg_schemas

        self._file = ConfigFile(self, config_dir)

        self._configurable_keys = cfg_keys
        self._modified_keys = {}
        self._unset_keys = []

        for name in overrides:
            if name not in self._configurable_keys:
                raise ConfigUnknownOverrideError(name=name)

        host_uid, host_uid_file_path = self._get_user_id()

        self._meta_data = {
            "config_directory": self._file.directory,
            "config_file_name": self._file.path.name,
            "config_file_path": self._file.path,
            "config_file_contents": self._file.contents,
            "config_invocation_key": self._file.invoc_key,
            "config_schemas": cfg_schemas,
            "invoking_user_id": uid or host_uid,
            "host_user_id": host_uid,
            "host_user_id_file_path": host_uid_file_path,
        }

        self._validate()

    def __str__(self):
        return self.to_string(exclude=["config_file_contents"])

    def __dir__(self):
        return super().__dir__() + self._all_keys

    def __getattr__(self, name):
        return self.get(name)

    def __setattr__(self, name, value):
        if (
            "_configurable_keys" in self.__dict__
            and name in self.__dict__["_configurable_keys"]
        ):
            self.set(name, value)
        else:
            super().__setattr__(name, value)

    def _validate(self, data=None, raise_with_metadata=True):
        """Validate configuration items of the loaded invocation."""

        self._logger.debug("Validating configuration...")
        if data is None:
            validated_data = self.get_all(include_overrides=True)
        else:
            validated_data = data

        for cfg_schema in self._schemas:
            cfg_validated = cfg_schema.validate(validated_data)
            if not cfg_validated.is_valid:
                meta_data = None
                if raise_with_metadata:
                    meta_data = self.to_string(
                        exclude=["config_file_contents", "config_schemas"], just_meta=True
                    )
                raise ConfigValidationError(
                    message=cfg_validated.get_failures_string(),
                    meta_data=meta_data,
                )
            validated_data = cfg_validated.cast_data

        self._logger.debug("Configuration is valid.")
        return validated_data

    def to_string(self, exclude: Optional[List] = None, just_meta=False):
        """Format the instance in a string, optionally exclude some keys.

        Parameters
        ----------
        exclude
            List of keys to exclude. Optional.
        just_meta
            If True, just return a str of the meta-data. This is useful to show during
            initialisation, in the case where the configuration is otherwise invalid.

        """
        exclude = exclude or []
        lines = []
        blocks = {"meta-data": self._meta_data}
        if not just_meta:
            blocks.update({"configuration": self.get_all(as_str=True)})
        for title, dat in blocks.items():
            lines.append(f"{title}:")
            for key, val in dat.items():
                if key in exclude:
                    continue
                if isinstance(val, list):
                    if val:
                        val = "\n    " + "\n    ".join(str(i) for i in val)
                    else:
                        val = "[]"
                lines.append(f"  {key}: {val}")
        return "\n".join(lines)

    def _resolve_path(self, path):
        """Resolve a file path, but leave fsspec protocols alone."""
        if not any(str(path).startswith(i + ":") for i in fsspec_protocols):
            path = Path(path)
            path = path.expanduser()
            if not path.is_absolute():
                path = self._meta_data["config_directory"].joinpath(path)
        else:
            self._logger.debug(
                f"Not resolving path {path!r} because it looks like an `fsspec` URL."
            )

        return path

    def register_config_get_callback(self, name):
        """Decorator to register a function as a configuration callback for a specified
        configuration item name, to be invoked on `get` of the item."""

        def decorator(func):
            if name in self._get_callbacks:
                self._get_callbacks[name] = tuple(
                    list(self._get_callbacks[name]) + [func]
                )
            else:
                self._get_callbacks[name] = (func,)

            @functools.wraps(func)
            def wrap(value):
                return func(value)

            return wrap

        return decorator

    def register_config_set_callback(self, name):
        """Decorator to register a function as a configuration callback for a specified
        configuration item name, to be invoked on `set` of the item."""

        def decorator(func):
            if name in self._set_callbacks:
                self._set_callbacks[name] = tuple(
                    list(self._set_callbacks[name]) + [func]
                )
            else:
                self._set_callbacks[name] = (func,)

            @functools.wraps(func)
            def wrap(value):
                return func(value)

            return wrap

        return decorator

    @property
    def _all_keys(self):
        return self._configurable_keys + list(self._meta_data.keys())

    def get_all(self, include_overrides=True, as_str=False):
        """Get all configurable items."""
        items = {}
        for key in self._configurable_keys:
            if key in self._unset_keys:
                continue
            else:
                try:
                    val = self.get(
                        name=key,
                        include_overrides=include_overrides,
                        raise_on_missing=True,
                        as_str=as_str,
                    )
                except ValueError:
                    continue
                items.update({key: val})
        return items

    def _get_callback_value(self, name, value):
        if name in self._get_callbacks and value is not None:
            for cb in self._get_callbacks.get(name, []):
                self._logger.debug(
                    f"Invoking `config.get` callback ({cb.__name__!r}) for item {name!r}={value!r}"
                )
                value = cb(self, value)
        return value

    def get(
        self,
        name,
        include_overrides=True,
        raise_on_missing=False,
        as_str=False,
        callback=True,
    ):
        """Get a configuration item.

        Notes
        -----

        """
        if name not in self._all_keys:
            raise ConfigUnknownItemError(name=name)

        elif name in self._meta_data:
            val = self._meta_data[name]

        elif include_overrides and name in self._overrides:
            val = self._overrides[name]

        elif name in self._unset_keys:
            if raise_on_missing:
                raise ValueError("Not set.")
            val = None

        elif name in self._modified_keys:
            val = self._modified_keys[name]

        elif name in self._configurable_keys:
            val = self._file.get_config_item(name, raise_on_missing)

        if callback:
            try:
                val = self._get_callback_value(name, val)
            except Exception as err:
                raise ConfigItemCallbackError(name, err)

        if as_str:
            if isinstance(val, (list, tuple, set)):
                val = [str(i) for i in val]
            else:
                val = str(val)

        return val

    def _parse_JSON(self, name, value):
        try:
            value = json.loads(value)
        except json.decoder.JSONDecodeError as err:
            raise ConfigChangeInvalidJSONError(name=name, json_str=value, err=err)
        return value

    def set(self, name, value, is_json=False, callback=True):

        self._logger.debug(f"Attempting to set config item {name!r} to {value!r}.")

        if name not in self._configurable_keys:
            raise ConfigNonConfigurableError(name=name)
        else:
            if is_json:
                value = self._parse_JSON(name, value)
            current_val = self.get(name)
            callback_val = self._get_callback_value(name, value)
            file_val_raw = self._file.get_config_item(name)
            file_val = self._get_callback_value(name, file_val_raw)

            if callback_val != current_val:

                was_in_modified = False
                was_in_unset = False
                prev_modified_val = None
                modified_updated = False

                if name in self._modified_keys:
                    was_in_modified = True
                    prev_modified_val = self._modified_keys[name]

                if name in self._unset_keys:
                    was_in_unset = True
                    idx = self._unset_keys.index(name)
                    self._unset_keys.pop(idx)

                if callback_val != file_val:
                    self._modified_keys[name] = value
                    modified_updated = True

                try:
                    self._validate()

                    if callback:
                        for cb in self._set_callbacks.get(name, []):
                            self._logger.debug(
                                f"Invoking `config.set` callback for item {name!r}: {cb.__name__!r}"
                            )
                            cb(self, callback_val)

                except ConfigValidationError as err:

                    # revert:
                    if modified_updated:
                        if was_in_modified:
                            self._modified_keys[name] = prev_modified_val
                        else:
                            del self._modified_keys[name]
                    if was_in_unset:
                        self._unset_keys.append(name)

                    raise ConfigChangeValidationError(name, validation_err=err) from None

                self._logger.debug(
                    f"Successfully set config item {name!r} to {callback_val!r}."
                )
            else:
                print(f"value is already: {callback_val!r}")

    def unset(self, name):

        if name not in self._configurable_keys:
            raise ConfigNonConfigurableError(name=name)
        if name in self._unset_keys or not self._file.is_item_set(name):
            raise ConfigItemAlreadyUnsetError(name=name)

        self._unset_keys.append(name)
        try:
            self._validate()
        except ConfigValidationError as err:
            self._unset_keys.pop()
            raise ConfigChangeValidationError(name, validation_err=err) from None

    def append(self, name, value, is_json=False):
        existing = self.get(name, callback=False)
        if is_json:
            value = self._parse_JSON(name, value)
        try:
            new = existing + [value]
        except TypeError:
            raise ConfigChangeTypeInvalidError(name, typ=type(existing)) from None

        self.set(name, new)

    def prepend(self, name, value, is_json=False):
        existing = self.get(name, callback=False)
        if is_json:
            value = self._parse_JSON(name, value)
        try:
            new = [value] + existing
        except TypeError:
            raise ConfigChangeTypeInvalidError(name, typ=type(existing)) from None

        self.set(name, new)

    def pop(self, name, index):
        existing = self.get(name, callback=False)
        try:
            new = copy.deepcopy(existing)
            new.pop(index)
        except AttributeError:
            raise ConfigChangeTypeInvalidError(name, typ=type(existing)) from None
        except IndexError:
            raise ConfigChangePopIndexError(
                name, length=len(existing), index=index
            ) from None

        self.set(name, new)

    def save(self):
        """Save any modified/unset configuration items into the file."""
        if not self._modified_keys and not self._unset_keys:
            print("No modifications to save!")
        else:
            self._file.save()

    def get_configurable(self):
        """Get a list of all configurable keys."""
        return self._configurable_keys

    def _get_user_id(self):
        """Retrieve (and set if non-existent) a unique user ID that is independent of the
        config directory."""

        uid_file_dir = Path(user_data_dir(appname=self._app.name))
        uid_file_path = uid_file_dir.joinpath("user_id.txt")
        if not uid_file_path.exists():
            uid_file_dir.mkdir(exist_ok=True, parents=True)
            uid = str(uuid.uuid4())
            with uid_file_path.open("wt") as fh:
                fh.write(uid)
        else:
            with uid_file_path.open("rt") as fh:
                uid = fh.read().strip()

        # Generate sub-dir for this hostname (used by helper process):
        hostname_dir = uid_file_dir.joinpath(socket.gethostname())
        hostname_dir.mkdir(exist_ok=True)

        return uid, uid_file_path

    def _init_schemas(self):
        # Get allowed configurable keys from config schemas:
        cfg_schemas = [get_schema("config_schema.yaml")] + self._options.extra_schemas
        cfg_keys = []
        for cfg_schema in cfg_schemas:
            for rule in cfg_schema.rules:
                if not rule.path and rule.condition.callable.name == "allowed_keys":
                    cfg_keys.extend(rule.condition.callable.args)

        return (cfg_schemas, cfg_keys)
