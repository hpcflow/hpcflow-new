import os
import time
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.config.errors import (
    ConfigFileValidationError,
    ConfigItemCallbackError,
    ConfigReadOnlyError,
)


def test_reset_config(new_null_config):
    cfg_dir = hf.config.get("config_directory")
    machine_name = hf.config.get("machine")
    new_machine_name = machine_name + "123"
    hf.config._set("machine", new_machine_name)
    assert hf.config.get("machine") == new_machine_name
    hf.reset_config(config_dir=cfg_dir)
    assert hf.config.get("machine") == machine_name


def test_raise_on_invalid_config_file(new_null_config):
    # make an invalid config file:
    cfg_path = hf.config.get("config_file_path")
    with cfg_path.open("at+") as f:
        f.write("something_invalid: 1\n")

    # try to load the invalid file:
    cfg_dir = hf.config.get("config_directory")
    with pytest.raises(ConfigFileValidationError):
        hf.reload_config(config_dir=cfg_dir, warn=False)
    hf.reset_config(config_dir=cfg_dir, warn=False)
    hf.unload_config()


def test_reset_invalid_config(new_null_config):
    # make an invalid config file:
    cfg_path = hf.config.get("config_file_path")
    with cfg_path.open("at+") as f:
        f.write("something_invalid: 1\n")

    # check we can reset the invalid file:
    cfg_dir = hf.config.get("config_directory")
    hf.reset_config(config_dir=cfg_dir, warn=False)
    hf.unload_config()


def test_raise_on_set_default_scheduler_not_in_schedulers_list_invalid_name(null_config):
    new_default = "invalid-scheduler"
    with pytest.raises(ConfigItemCallbackError):
        hf.config.default_scheduler = new_default


def test_raise_on_set_default_scheduler_not_in_schedulers_list_valid_name(null_config):
    new_default = "slurm"  # valid but unsupported (by default) scheduler
    with pytest.raises(ConfigItemCallbackError):
        hf.config.default_scheduler = new_default


def test_without_callbacks_ctx_manager(null_config):
    # set a new shell that would raise an error in the `callback_supported_shells`:
    new_default = "bash" if os.name == "nt" else "powershell"

    with hf.config._without_callbacks("callback_supported_shells"):
        hf.config.default_shell = new_default
        assert hf.config.default_shell == new_default

    # outside the context manager, the callback is reinstated, which should raise:
    with pytest.raises(ConfigItemCallbackError):
        hf.config.default_shell

    # unload the modified config so it's not reused by other tests
    hf.unload_config()


@pytest.mark.xfail(reason="Might occasionally fail.")
def test_cache_faster_than_no_cache(null_config):
    n = 10_000
    tic = time.perf_counter()
    for _ in range(n):
        _ = hf.config.machine
    toc = time.perf_counter()
    elapsed_no_cache = toc - tic

    with hf.config.cached_config():
        tic = time.perf_counter()
        for _ in range(n):
            _ = hf.config.machine
        toc = time.perf_counter()
    elapsed_cache = toc - tic

    assert elapsed_cache < elapsed_no_cache


def test_cache_read_only(new_null_config):
    """Check we cannot modify the config when using the cache"""

    # check we can set an item first:
    hf.machine = "abc"
    assert hf.machine == "abc"

    with pytest.raises(ConfigReadOnlyError):
        with hf.config.cached_config():
            hf.config.set("machine", "123")

    with pytest.raises(ConfigReadOnlyError):
        with hf.config.cached_config():
            hf.config.machine = "456"
