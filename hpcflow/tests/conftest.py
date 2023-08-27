import pytest
import hpcflow.app as hf


def pytest_addoption(parser):
    parser.addoption(
        "--slurm",
        action="store_true",
        default=False,
        help="run slurm tests",
    )
    parser.addoption(
        "--wsl",
        action="store_true",
        default=False,
        help="run Windows Subsystem for Linux tests",
    )
    parser.addoption(
        "--direct-linux",
        action="store_true",
        default=False,
        help="run direct-linux submission tests",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slurm: mark test as slurm to run")
    config.addinivalue_line("markers", "wsl: mark test as wsl to run")
    config.addinivalue_line(
        "markers", "direct_linux: mark test as a direct-linux submission test to run"
    )
    hf.run_time_info.in_pytest = True


def pytest_collection_modifyitems(config, items):
    if config.getoption("--slurm"):
        # --slurm given in cli: only run slurm tests
        for item in items:
            if "slurm" not in item.keywords:
                item.add_marker(pytest.mark.skip(reason="need no --slurm option to run"))
    elif config.getoption("--wsl"):
        # --wsl given in CLI: only run wsl tests
        for item in items:
            if "wsl" not in item.keywords:
                item.add_marker(pytest.mark.skip(reason="need no --wsl option to run"))
    elif config.getoption("--direct-linux"):
        # --direct-linux in CLI: only run these tests
        for item in items:
            if "direct_linux" not in item.keywords:
                item.add_marker(
                    pytest.mark.skip(reason="remove --direct-linux option to run")
                )
    else:
        # --slurm not given in cli: skip slurm tests and do not skip other tests
        for item in items:
            if "slurm" in item.keywords:
                item.add_marker(pytest.mark.skip(reason="need --slurm option to run"))
            elif "wsl" in item.keywords:
                item.add_marker(pytest.mark.skip(reason="need --wsl option to run"))
            elif "direct_linux" in item.keywords:
                item.add_marker(
                    pytest.mark.skip(reason="add --direct_linux option to run")
                )


def pytest_unconfigure(config):
    hf.run_time_info.in_pytest = False


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)
    hf.run_time_info.in_pytest = True


@pytest.fixture
def new_null_config(tmp_path):
    hf.load_config(config_dir=tmp_path)
    hf.run_time_info.in_pytest = True
