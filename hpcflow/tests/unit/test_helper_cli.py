import pytest
from datetime import datetime, timedelta
import time
from pathlib import Path

from click.testing import CliRunner

from hpcflow.sdk.helper import helper
from tempfile import gettempdir
from hpcflow.api import hpcflow, load_config


@pytest.fixture
def app():
    load_config(config_dir=gettempdir())
    return hpcflow


# TODO: def test_start
# TODO: def test_modify
# TODO: def test_stop
# TODO: def test_run
# TODO: def test_restart
# TODO: def test_pid
# TODO: def test_pid_file
# TODO: def test_clear
# TODO: def test_uptime_running
# TODO: def test_uptime_not_running
# TODO: def test_log_path
# TODO: def test_watch_list_path
# TODO: def test_watch_list


# TODO: The test below is actually a functional test... move to another folder?
def test_modify_helper_cli(app):
    start_t = datetime.now()
    time.sleep(0.2)
    r = CliRunner()

    so = cli(r, args="helper clear")
    so = cli(
        r, args="helper start --timeout 60 --timeout-check-interval 1 --watch-interval 3"
    )
    assert "Helper started successfully." in so
    so = cli(
        args="helper modify --timeout 60 --timeout-check-interval 1 --watch-interval 3"
    )
    assert so == "Helper parameters already met."

    so = cli(
        r, args="helper modify --timeout 60 --timeout-check-interval 2 --watch-interval 1"
    )
    assert so == ""
    time.sleep(3.5)
    so = cli(
        r, args="helper modify --timeout 60 --timeout-check-interval 2 --watch-interval 1"
    )
    assert so == "Helper parameters already met."

    so = cli(
        r, args="helper modify --timeout 10 --timeout-check-interval 2 --watch-interval 1"
    )
    assert so == ""
    time.sleep(3)
    so = cli(
        r, args="helper modify --timeout 10 --timeout-check-interval 2 --watch-interval 1"
    )
    assert so == "Helper parameters already met."

    time.sleep(5)
    so = cli(r, args="helper pid")
    assert so == "Helper not running!"

    read_logs = helper.read_helper_log(app, start_t)
    mod_count = 0
    update_count = 0
    timeout = 0
    for line in read_logs:
        if " - INFO - " in line:
            (t, m) = line.split(" - INFO - ")
            if "Modifying" in m:
                mod_count = mod_count + 1
            elif "Updated" in m:
                update_count = update_count + 1
            elif "Helper exiting due to timeout" in m:
                timeout = timeout + 1
    assert timeout == 1
    assert update_count == 3
    assert mod_count == 2
    so = cli(r, args="helper clear")


def cli(r=CliRunner(), args=""):
    so = r.invoke(hpcflow.CLI, args)
    return so.output.strip()
