import pytest

from hpcflow.app import app as hf


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


def test_reset_config(null_config, tmp_path):
    machine_name = hf.config.get("machine")
    new_machine_name = machine_name + "123"
    hf.config.set("machine", new_machine_name)
    assert hf.config.get("machine") == new_machine_name
    hf.config.reset()
    assert hf.config.get("machine") == machine_name
