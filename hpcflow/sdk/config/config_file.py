from __future__ import annotations

import copy
import io
import logging
import os
from pathlib import Path
from tkinter import E
from typing import Optional, Union

from ruamel.yaml import YAML

from hpcflow.sdk.core.validation import get_schema

from .errors import (
    ConfigChangeFileUpdateError,
    ConfigDefaultValidationError,
    ConfigFileInvocationIncompatibleError,
    ConfigFileValidationError,
    ConfigValidationError,
)


class ConfigFile:
    """Configuration file."""

    def __init__(self, config, directory):

        self.config = config
        self.logger = self.config._logger
        self.directory = self._resolve_config_dir(directory)

        # set by _load_file_data:
        self.path = None
        self.contents = None
        self.data = None
        self.data_rt = None

        self._load_file_data()
        self._validate(self.data)

        # select appropriate config data for this invocation using run-time info:
        invoc_key = None
        for c_name_i, c_dat_i in self.data["configs"].items():
            is_match = True
            for match_k, match_v in c_dat_i["invocation"]["match"].items():
                if getattr(self.config._app.run_time_info, match_k) != match_v:
                    is_match = False
                    break
            if is_match:
                invoc_key = c_name_i
                self.logger.debug(
                    f"Found matching config ({invoc_key!r}) for this invocation."
                )
                break
        if not is_match:
            raise ConfigFileInvocationIncompatibleError()

        self.invoc_key = invoc_key

    def _validate(self, data):
        file_schema = get_schema("config_file_schema.yaml")
        file_validated = file_schema.validate(data)
        if not file_validated.is_valid:
            raise ConfigFileValidationError(file_validated.get_failures_string())

    @property
    def invoc_data(self):
        return self.data["configs"][self.invoc_key]

    def save(self):

        new_data = copy.deepcopy(self.data)
        new_data_rt = copy.deepcopy(self.data_rt)
        new_contents = ""

        modified_names = list(self.config._modified_keys.keys()) + self.config._unset_keys
        for k, v in self.config._modified_keys.items():
            new_data["configs"][self.invoc_key]["config"][k] = v
            new_data_rt["configs"][self.invoc_key]["config"][k] = v

        for k in self.config._unset_keys:
            del new_data["configs"][self.invoc_key]["config"][k]
            del new_data_rt["configs"][self.invoc_key]["config"][k]

        try:
            # write a new temporary config file
            cfg_file_path = self.config.get("config_file_path")
            cfg_tmp_file = cfg_file_path.with_suffix(cfg_file_path.suffix + ".tmp")
            self.logger.debug(f"Creating temporary config file: {cfg_tmp_file!r}.")
            yaml = YAML(typ="rt")
            with cfg_tmp_file.open("wt", newline="\n") as fh:
                yaml.dump(new_data_rt, fh)

            # atomic rename, overwriting original:
            self.logger.debug("Replacing original config file with temporary file.")
            os.replace(src=cfg_tmp_file, dst=cfg_file_path)

            buff = io.BytesIO()
            yaml.dump(new_data_rt, buff)
            new_contents = buff.getvalue()

        except Exception as err:
            raise ConfigChangeFileUpdateError(names=modified_names, err=err) from None

        self.data = new_data
        self.data_rt = new_data_rt
        self.contents = new_contents

        self.config._unset_keys = []
        self.config._modified_keys = {}

    def _resolve_config_dir(self, directory: Optional[Union[str, Path]] = None) -> Path:
        """Find the directory in which to locate the configuration file.

        If no configuration directory is specified, look first for an environment variable
        (given by config option `directory_env_var`), and then in the default
        configuration directory (given by config option `default_directory`).

        The configuration directory will be created if it does not exist.

        Parameters
        ----------
        directory
            Directory in which to find the configuration file. Optional.

        Returns
        -------
        directory : Path
            Absolute path to the configuration directory.

        """

        if not directory:
            directory = Path(
                os.getenv(
                    self.config._options.directory_env_var,
                    self.config._options.default_directory,
                )
            ).expanduser()
        else:
            directory = Path(directory)

        if not directory.is_dir():
            self.logger.debug(
                f"Configuration directory does not exist. Generating here: {str(directory)!r}."
            )
            directory.mkdir()
        else:
            self.logger.debug(f"Using configuration directory: {str(directory)!r}.")

        return directory.resolve()

    def _setup_default_config(self, config_path):
        self.config._logger.info(
            "No config.yaml found in the configuration directory. Generating "
            "a config.yaml file."
        )

        # validate the default:
        default_config = self.config._options.default_config
        try:
            # validate default config "file" structure:
            self._validate(data=default_config)

            # validate default config items for each defined invocation:
            for val in default_config["configs"].values():
                self.config._validate(data=val["config"], raise_with_metadata=False)

        except (ConfigFileValidationError, ConfigValidationError) as err:
            raise ConfigDefaultValidationError(err) from None

        yaml = YAML(typ="rt")
        with config_path.open("w") as handle:
            yaml.dump(default_config, handle)

    def _load_file_data(self):
        """Load data from the configuration file (config.yaml or config.yml)."""

        # Try both ".yml" and ".yaml" extensions:
        path = self.directory.joinpath("config.yaml")
        if not path.is_file():
            path = self.directory.joinpath("config.yml")

        if not path.is_file():
            self._setup_default_config(config_path=path)

        yaml = YAML(typ="safe")
        yaml_rt = YAML(typ="rt")
        with path.open() as handle:
            contents = handle.read()
            handle.seek(0)
            data = yaml.load(handle)
            handle.seek(0)
            data_rt = yaml_rt.load(handle)

        self.path = path
        self.contents = contents
        self.data = data
        self.data_rt = data_rt

    def get_config_item(self, name, raise_on_missing=False):
        if raise_on_missing and name not in self.invoc_data["config"]:
            raise ValueError(f"missing from file: {name!r}")
        return self.invoc_data["config"].get(name)

    def is_item_set(self, name):
        try:
            self.get_config_item(name, raise_on_missing=True)
        except ValueError:
            return False
        return True
