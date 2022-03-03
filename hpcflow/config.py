from pathlib import Path
from ruamel.yaml import YAML, safe_load


class Config:

    __conf = {}

    @staticmethod
    def get_config_file(config_dir):

        config_file = config_dir.joinpath("config.yaml")
        with config_file.open() as handle:
            config_dat = safe_load(handle)

        return config_dat, config_file

    @staticmethod
    def resolve_config_dir(config_dir=None):

        if not config_dir:
            config_dir = Path("../hpcflow/")
        else:
            config_dir = Path(config_dir)

        if not config_dir.is_dir():
            print("Configuration directory does not exist. Generating.")
            config_dir.mkdir()

        return config_dir

    @staticmethod
    def set_config(config_dir=None):
        """Load configuration from a YAML file."""

        config_dir = Config.resolve_config_dir(config_dir)
        config_dat, _ = Config.get_config_file(config_dir)

        Config.__conf.update(**config_dat)
