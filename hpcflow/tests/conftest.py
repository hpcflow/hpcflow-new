import pytest
import hpcflow.app as hf


def pytest_configure(config):
    hf.run_time_info.in_pytest = True


def pytest_unconfigure(config):
    hf.run_time_info.in_pytest = False


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)
    hf.run_time_info.in_pytest = True
