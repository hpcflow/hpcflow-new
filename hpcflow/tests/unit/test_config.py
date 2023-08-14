import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.config.errors import ConfigFileValidationError


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


def test_reset_config(null_config):
    cfg_dir = hf.config.get("config_directory")
    machine_name = hf.config.get("machine")
    new_machine_name = machine_name + "123"
    hf.config.set("machine", new_machine_name)
    assert hf.config.get("machine") == new_machine_name
    hf.reset_config(config_dir=cfg_dir)
    assert hf.config.get("machine") == machine_name


def test_raise_on_invalid_config_file(null_config):
    # make an invalid config file:
    cfg_path = hf.config.get("config_file_path")
    with cfg_path.open("at+") as f:
        f.write("something_invalid: 1\n")

    # try to load the invalid file:
    cfg_dir = hf.config.get("config_directory")
    with pytest.raises(ConfigFileValidationError):
        hf.reload_config(config_dir=cfg_dir)


def test_reset_invalid_config(null_config):
    # make an invalid config file:
    cfg_path = hf.config.get("config_file_path")
    with cfg_path.open("at+") as f:
        f.write("something_invalid: 1\n")

    # check we can reset the invalid file:
    cfg_dir = hf.config.get("config_directory")
    hf.reset_config(config_dir=cfg_dir)
