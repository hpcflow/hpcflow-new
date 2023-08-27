from __future__ import annotations

import copy
import fnmatch
import io
import logging
import os
from pathlib import Path
import random
import string
from typing import Dict, Optional, Union

from ruamel.yaml import YAML

from hpcflow.sdk.core.validation import get_schema

from .errors import (
    ConfigChangeFileUpdateError,
    ConfigDefaultValidationError,
    ConfigFileInvocationIncompatibleError,
    ConfigFileInvocationUnknownMatchKey,
    ConfigFileValidationError,
    ConfigInvocationKeyNotFoundError,
    ConfigValidationError,
)


class ConfigFile:
    """Configuration file."""

    def __init__(self, directory, logger, config_options):
        self.logger = logger
        self.directory = self._resolve_config_dir(
            config_opt=config_options,
            logger=self.logger,
            directory=directory,
        )

        self._configs = []

        # set by _load_file_data:
        self.path = None
        self.contents = None
        self.data = None
        self.data_rt = None

        self._load_file_data(config_options)
        self.file_schema = self._validate(self.data)

    @staticmethod
    def select_invocation(
        configs: Dict,
        run_time_info: Dict,
        path: Path,
        config_key: Union[str, None] = None,
    ) -> str:
        """Select a matching configuration for this invocation using run-time info."""
        if not config_key:
            all_matches = {}  # keys are config keys; values are lengths of match dict
            for c_name_i, c_dat_i in configs.items():
                # for a config to "match", each "match key" must match the relevant run
                # time info attribute. If a "match key" has multiple values, at least
                # one value must match the run time info attribute:
                is_match = True
                for match_k, match_v in c_dat_i["invocation"]["match"].items():
                    # test for a matching glob pattern (where multiple may be specified):
                    if not isinstance(match_v, list):
                        match_v = [match_v]

                    try:
                        k_value = run_time_info[match_k]
                    except KeyError:
                        raise ConfigFileInvocationUnknownMatchKey(match_k)

                    is_match_i = False
                    for match_i in match_v:
                        if fnmatch.filter(names=[k_value], pat=match_i):
                            is_match_i = True
                            break

                    if not is_match_i:
                        is_match = False
                        break

                if is_match:
                    all_matches[c_name_i] = len(c_dat_i["invocation"]["match"])

            if all_matches:
                # for multiple matches select the more specific one:
                all_sorted = sorted(all_matches.items(), key=lambda x: x[1], reverse=True)
                config_key = all_sorted[0][0]
            else:
                raise ConfigFileInvocationIncompatibleError(config_key)

        elif config_key not in configs:
            raise ConfigInvocationKeyNotFoundError(config_key, path, list(configs.keys()))

        return config_key

    def _validate(self, data):
        file_schema = get_schema("config_file_schema.yaml")
        file_validated = file_schema.validate(data)
        if not file_validated.is_valid:
            raise ConfigFileValidationError(file_validated.get_failures_string())
        return file_schema

    def get_invoc_data(self, config_key):
        return self.data["configs"][config_key]

    def get_invocation(self, config_key):
        return self.get_invoc_data(config_key)["invocation"]

    def save(self):
        new_data = copy.deepcopy(self.data)
        new_data_rt = copy.deepcopy(self.data_rt)
        new_contents = ""

        modified_names = []
        for config in self._configs:
            modified_names += list(config._modified_keys.keys()) + config._unset_keys
            for k, v in config._modified_keys.items():
                new_data["configs"][config._config_key]["config"][k] = v
                new_data_rt["configs"][config._config_key]["config"][k] = v

            for k in config._unset_keys:
                del new_data["configs"][config._config_key]["config"][k]
                del new_data_rt["configs"][config._config_key]["config"][k]

        try:
            new_contents = self._dump(new_data_rt)
        except Exception as err:
            raise ConfigChangeFileUpdateError(names=modified_names, err=err) from None

        self.data = new_data
        self.data_rt = new_data_rt
        self.contents = new_contents

        for config in self._configs:
            config._unset_keys = []
            config._modified_keys = {}

    @staticmethod
    def _resolve_config_dir(
        config_opt, logger, directory: Optional[Union[str, Path]] = None
    ) -> Path:
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
                os.getenv(config_opt.directory_env_var, config_opt.default_directory)
            ).expanduser()
        else:
            directory = Path(directory)

        if not directory.is_dir():
            logger.debug(
                f"Configuration directory does not exist. Generating here: {str(directory)!r}."
            )
            directory.mkdir()
        else:
            logger.debug(f"Using configuration directory: {str(directory)!r}.")

        return directory.resolve()

    def _dump(self, config_data: Dict, path: Optional[Path] = None) -> str:
        """Dump the specified config data to the specified config file path.

        Parameters
        ----------
        config_data
            New configuration file data that will be dumped using the "round-trip" dumper.
        path
            Path to dump the config file data to. If not specified the `path` instance
            attribute will be used. If the file already exists, an "atomic-ish" overwrite
            will be used, where we firstly create a temporary file, which then replaces
            the existing file.

        Returns
        -------
        new_contents
            String contents of the new file.

        """

        if path is None:
            path = self.path

        yaml = YAML(typ="rt")
        if path.exists():
            # write a new temporary config file
            cfg_tmp_file = path.with_suffix(path.suffix + ".tmp")
            self.logger.debug(f"Creating temporary config file: {cfg_tmp_file!r}.")
            with cfg_tmp_file.open("wt", newline="\n") as fh:
                yaml.dump(config_data, fh)

            # atomic rename, overwriting original:
            self.logger.debug("Replacing original config file with temporary file.")
            os.replace(src=cfg_tmp_file, dst=path)

        else:
            with path.open("w", newline="\n") as handle:
                yaml.dump(config_data, handle)

        buff = io.BytesIO()
        yaml.dump(config_data, buff)
        new_contents = str(buff.getvalue())

        return new_contents

    def add_default_config(self, config_options, name=None) -> str:
        """Add a new default config to the config file, and create the file if it doesn't
        exist."""

        is_new_file = False
        if not self.path.exists():
            is_new_file = True
            new_data = {"configs": {}}
            new_data_rt = {"configs": {}}
        else:
            new_data = copy.deepcopy(self.data)
            new_data_rt = copy.deepcopy(self.data_rt)

        if not name:
            chars = string.ascii_letters
            name = "".join(random.choices(chars, k=6))

        def_config = copy.deepcopy(config_options.default_config)
        new_config = {name: def_config}

        new_data["configs"].update(new_config)
        new_data_rt["configs"].update(new_config)

        try:
            if is_new_file:
                # validate default config "file" structure:
                self._validate(data=new_data)

            # validate default config items for the newly added default config:
            config_options.validate(
                data=def_config["config"],
                logger=self.logger,
                raise_with_metadata=False,
            )

        except (ConfigFileValidationError, ConfigValidationError) as err:
            raise ConfigDefaultValidationError(err) from None

        self.data_rt = new_data_rt
        self.data = new_data
        self.contents = self._dump(new_data_rt)

        return name

    @staticmethod
    def get_config_file_path(directory):
        # Try both ".yml" and ".yaml" extensions:
        path_yaml = directory.joinpath("config.yaml")
        if path_yaml.is_file():
            return path_yaml
        path_yml = directory.joinpath("config.yml")
        if path_yml.is_file():
            return path_yml
        return path_yaml

    def _load_file_data(self, config_options):
        """Load data from the configuration file (config.yaml or config.yml)."""

        self.path = self.get_config_file_path(self.directory)
        if not self.path.is_file():
            self.logger.info(
                "No config.yaml found in the configuration directory. Generating "
                "a config.yaml file."
            )
            self.add_default_config(name="default", config_options=config_options)

        yaml = YAML(typ="safe")
        yaml_rt = YAML(typ="rt")
        with self.path.open() as handle:
            contents = handle.read()
            handle.seek(0)
            data = yaml.load(handle)
            handle.seek(0)
            data_rt = yaml_rt.load(handle)

        self.contents = contents
        self.data = data
        self.data_rt = data_rt

    def get_config_item(
        self, config_key, name, raise_on_missing=False, default_value=None
    ):
        if raise_on_missing and name not in self.get_invoc_data(config_key)["config"]:
            raise ValueError(f"missing from file: {name!r}")
        return self.get_invoc_data(config_key)["config"].get(name, default_value)

    def is_item_set(self, config_key, name):
        try:
            self.get_config_item(config_key, name, raise_on_missing=True)
        except ValueError:
            return False
        return True

    def rename_config_key(self, config_key: str, new_config_key: str):
        """Change the config key of the loaded config."""

        new_data = copy.deepcopy(self.data)
        new_data_rt = copy.deepcopy(self.data_rt)

        new_data["configs"][new_config_key] = new_data["configs"].pop(config_key)
        new_data_rt["configs"][new_config_key] = new_data_rt["configs"].pop(config_key)

        for config in self._configs:
            if config._config_key == config_key:
                config._meta_data["config_key"] = new_config_key
                config._config_key = new_config_key

        self.data_rt = new_data_rt
        self.data = new_data
        self.contents = self._dump(new_data_rt)

    def update_invocation(
        self,
        config_key: str,
        environment_setup: Optional[str] = None,
        match: Optional[Dict] = None,
    ):
        """Modify the invocation parameters of the loaded config."""

        new_data = copy.deepcopy(self.data)
        new_data_rt = copy.deepcopy(self.data_rt)

        for dat in (new_data, new_data_rt):
            invoc = dat["configs"][config_key]["invocation"]
            if environment_setup:
                invoc["environment_setup"] = environment_setup
            if match:
                invoc["match"].update(match)

        self.data_rt = new_data_rt
        self.data = new_data
        self.contents = self._dump(new_data_rt)
