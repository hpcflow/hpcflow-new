import pytest
import hpcflow.app as hf


def pytest_addoption(parser):
    parser.addoption(
        "--slurm",
        action="store_true",
        default=False,
        help="run slurm tests",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slurm: mark test as slurm to run")
    hf.run_time_info.in_pytest = True


def pytest_collection_modifyitems(config, items):
    if config.getoption("--slurm"):
        # --slurm given in cli: do not skip slurm tests
        return
    skip_slurm = pytest.mark.skip(reason="need --slurm option to run")
    for item in items:
        if "slurm" in item.keywords:
            item.add_marker(skip_slurm)


def pytest_unconfigure(config):
    hf.run_time_info.in_pytest = False


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)
    hf.run_time_info.in_pytest = True
