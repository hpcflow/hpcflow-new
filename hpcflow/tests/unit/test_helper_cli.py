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


@pytest.mark.parametrize(
    "flags", ["", " --timeout=3", " --timeout-check-interval=2", " --watch-interval=1"]
)
def test_start(mocker, flags):
    t, tci, wi = run_flags(flags, 3, 2, 1)
    start_spy = mocker.patch("hpcflow.sdk.helper.cli.start_helper")
    cli(args=f"helper start{flags}")
    start_spy.assert_called_once_with(mocker.ANY, t, tci, wi)


@pytest.mark.parametrize(
    "flags", ["", " --timeout=3", " --timeout-check-interval=2", " --watch-interval=1"]
)
def test_modify(mocker, flags):
    t, tci, wi = run_flags(flags, 3, 2, 1)
    modify_spy = mocker.patch("hpcflow.sdk.helper.cli.modify_helper")
    cli(args=f"helper modify{flags}")
    modify_spy.assert_called_once_with(mocker.ANY, t, tci, wi)


def test_stop(mocker):
    stop_spy = mocker.patch("hpcflow.sdk.helper.cli.clear_helper")
    cli(args="helper clear")
    stop_spy.assert_called_once


@pytest.mark.parametrize(
    "flags", ["", " --timeout=3", " --timeout-check-interval=2", " --watch-interval=1"]
)
def test_run(mocker, flags):
    t, tci, wi = run_flags(flags, 3, 2, 1)
    run_spy = mocker.patch("hpcflow.sdk.helper.cli.run_helper")
    cli(args=f"helper run{flags}")
    run_spy.assert_called_once_with(mocker.ANY, t, tci, wi)


@pytest.mark.parametrize(
    "flags", ["", " --timeout=3", " --timeout-check-interval=2", " --watch-interval=1"]
)
def test_restart(mocker, flags):
    t, tci, wi = run_flags(flags, 3, 2, 1)
    restart_spy = mocker.patch("hpcflow.sdk.helper.cli.restart_helper")
    cli(args=f"helper restart{flags}")
    restart_spy.assert_called_once_with(mocker.ANY, t, tci, wi)


def test_pid(app):
    helper.clear_helper(app)
    pid_fp = helper.get_PID_file_path(app)
    with pid_fp.open("wt") as fp:
        fp.write(f"pid = {12345}\n")
    so = cli(args="helper pid")
    assert so == "12345"


def test_pid_file(app):
    helper.clear_helper(app)
    pid_fp = helper.get_PID_file_path(app)
    with pid_fp.open("wt") as fp:
        fp.write(f"pid = {12345}\n")
    so = cli(args="helper pid -f")
    assert so == str(f"12345 ({str(pid_fp)})")


def test_clear(mocker):
    clear_spy = mocker.patch("hpcflow.sdk.helper.cli.clear_helper")
    cli(args="helper clear")
    clear_spy.assert_called_once


def test_uptime_running(app, mocker):
    mocker.patch(
        "hpcflow.sdk.helper.cli.get_helper_uptime",
        return_value=timedelta(seconds=3661),
    )
    so = cli(args="helper uptime")
    assert so == "1:01:01"


def test_uptime_not_running(app):
    helper.clear_helper(app)
    so = cli(args="helper uptime")
    assert so == "Helper not running!"


def test_log_path(app):
    log_path = helper.get_helper_log_path(app)
    so = cli(args="helper log-path")
    assert so == str(log_path)


def test_watch_list_path(app):
    watcher_path = helper.get_watcher_file_path(app)
    so = cli(args="helper watch-list-path")
    assert so == str(watcher_path)


def test_watch_list(app, mocker):
    mocker.patch(
        "hpcflow.sdk.helper.cli.get_helper_watch_list",
        return_value=[{"path": Path("mockpath1")}, {"path": Path("mockpath2")}],
    )
    so = cli(args="helper watch-list")
    assert so == "mockpath1\nmockpath2"


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


def cli(r=None, args="", config_dir=None):
    if r is None:
        r = CliRunner()
    if not config_dir:
        config_dir = gettempdir()
    args = f"--config-dir {str(config_dir)} " + args
    so = r.invoke(hpcflow.CLI, args)
    return so.output.strip()


def run_flags(flags, t, tci, wi):
    t = float(t) if "--timeout=" in flags else float(helper.DEFAULT_TIMEOUT)
    tci = (
        float(tci)
        if "--timeout-check-interval=" in flags
        else float(helper.DEFAULT_TIMEOUT_CHECK)
    )
    wi = (
        float(wi)
        if "--watch-interval=" in flags
        else float(helper.DEFAULT_WATCH_INTERVAL)
    )
    return t, tci, wi
