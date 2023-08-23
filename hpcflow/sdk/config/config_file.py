from __future__ import annotations

import copy
import fnmatch
import io
import logging
import os
from pathlib import Path
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

    def __init__(self, config, directory, invoc_key=None):
        self.config = config
        self.logger = self.config._logger
        self.directory = self._resolve_config_dir(
            config_opt=self.config._options,
            logger=self.logger,
            directory=directory,
        )

        # set by _load_file_data:
        self.path = None
        self.contents = None
        self.data = None
        self.data_rt = None

        self._load_file_data()
        self.file_schema = self._validate(self.data)

        self.invoc_key = self.select_invocation(
            invoc_key=invoc_key,
            configs=self.data["configs"],
            run_time_info=self.config._app.run_time_info.to_dict(),
            path=self.path,
        )

    @staticmethod
    def select_invocation(
        configs: Dict,
        run_time_info: Dict,
        path: Path,
        invoc_key: Union[str, None] = None,
    ) -> str:
        """Select a matching configuration for this invocation using run-time info."""
        if not invoc_key:
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

            if is_match:
                # for multiple matches select the more specific one:
                all_sorted = sorted(all_matches.items(), key=lambda x: x[1], reverse=True)
                invoc_key = all_sorted[0][0]
            else:
                raise ConfigFileInvocationIncompatibleError(invoc_key)

        elif invoc_key not in configs:
            raise ConfigInvocationKeyNotFoundError(invoc_key, path, list(configs.keys()))

        return invoc_key

    def _validate(self, data):
        file_schema = get_schema("config_file_schema.yaml")
        file_validated = file_schema.validate(data)
        if not file_validated.is_valid:
            raise ConfigFileValidationError(file_validated.get_failures_string())
        return file_schema

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
            new_contents = self._dump(new_data_rt)
        except Exception as err:
            raise ConfigChangeFileUpdateError(names=modified_names, err=err) from None

        self.data = new_data
        self.data_rt = new_data_rt
        self.contents = new_contents

        self.config._unset_keys = []
        self.config._modified_keys = {}

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

    def _setup_default_config(self):
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

        self._dump(default_config)

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

    def _load_file_data(self):
        """Load data from the configuration file (config.yaml or config.yml)."""

        self.path = self.get_config_file_path(self.directory)
        if not self.path.is_file():
            self.config._logger.info(
                "No config.yaml found in the configuration directory. Generating "
                "a config.yaml file."
            )
            self._setup_default_config()

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

    def get_config_item(self, name, raise_on_missing=False, default_value=None):
        if raise_on_missing and name not in self.invoc_data["config"]:
            raise ValueError(f"missing from file: {name!r}")
        return self.invoc_data["config"].get(name, default_value)

    def is_item_set(self, name):
        try:
            self.get_config_item(name, raise_on_missing=True)
        except ValueError:
            return False
        return True

    def modify_invocation(self):
        # TODO
        pass

        # _dump(modified, self.path)
