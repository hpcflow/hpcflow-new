import pytest
from datetime import datetime, timedelta
import time

import io
import sys

from click.testing import CliRunner

from hpcflow.sdk.helper import helper
from tempfile import gettempdir
from hpcflow.api import hpcflow, load_config


@pytest.fixture
def app():
    load_config(config_dir=gettempdir())
    return hpcflow


# TODO: test_get_user_data_dir
# TODO: test_get_PID_file_path


def test_get_helper_PID(app):
    pid_fp = helper.get_PID_file_path(app)
    with pid_fp.open("wt") as fp:
        fp.write(f"pid = {12345}\n")
    (pid, file) = helper.get_helper_PID(app)
    assert 12345 == pid
    assert file == pid_fp


def test_get_helper_PID_no_file(app):
    pid_fp = helper.get_PID_file_path(app)
    if pid_fp.is_file():
        pid_fp.unlink()
    ghpid = helper.get_helper_PID(app)
    assert None == ghpid


# TODO: test_get_helper_log_path
# TODO: test_get_helper_logger
# TODO: test_logger.info
# TODO: test_logger.error
# TODO: test_get_watcher_file_path
# TODO: test_get_helper_watch_list
# TODO: test_clear_helper
# TODO: test_kill_proc_tree

# TODO: test_get_helper_uptime


def test_write_and_read_helper_args(app):
    helper.write_helper_args(app, 123, 4, 5, 6)
    read_helper_args = helper.read_helper_args(app)
    assert {
        "pid": 123,
        "timeout": 4,
        "timeout_check_interval": 5.0,
        "watch_interval": 6.0,
    } == read_helper_args
    pid_file = helper.get_PID_file_path(app)
    pid_file.unlink()


def test_read_helper_log_with_start_t(app):
    oldlogs = [
        "2023-02-27 8:00:00,000 - hpcflow.sdk.helper.helper - INFO - log 1 before start",
        "2023-02-27 8:00:01,000 - hpcflow.sdk.helper.helper - INFO - log 2 before start",
    ]
    start_t = datetime(2023, 2, 27, 8, 0, 2, 0)
    newlogs = [
        "2023-02-27 8:00:03,000 - hpcflow.sdk.helper.helper - INFO - log 1 after start",
        "2023-02-27 8:00:04,000 - hpcflow.sdk.helper.helper - INFO - log 2 after start",
    ]
    log_file = helper.get_helper_log_path(app)
    with log_file.open("wt") as f:
        for line in oldlogs + newlogs:
            f.write(line + "\n")
    read_logs = helper.read_helper_log(app, start_t)
    assert newlogs == read_logs


def test_read_helper_log_with_uptime(app, mocker):
    helper.clear_helper(app)
    logger = helper.get_helper_logger(app)
    logger.info("log 1 before start")
    logger.info("log 2 before start")
    time.sleep(0.01)
    start_t = datetime.now()
    time.sleep(0.01)
    mocker.patch(
        "hpcflow.sdk.helper.helper.get_helper_uptime",
        # get_helper_uptime calls are bypassed with this return value
        return_value=datetime.now() - start_t,
    )
    logger.info("log 1 after start")
    logger.info("log 2 after start")
    read_logs = helper.read_helper_log(app)
    assert len(read_logs) == 2
    assert "log 1 after start" in read_logs[-2]
    assert "log 2 after start" in read_logs[-1]


def test_start_and_stop_default(app):
    helper.clear_helper(app)
    pytest_stdout = sys.stdout
    so = io.StringIO()  # Create StringIO object
    sys.stdout = so  # Redirect stdout.
    try:
        helper.start_helper(app)
        assert so.getvalue().splitlines()[-1] == "Helper started successfully."
    finally:
        try:
            solen = len(so.getvalue().splitlines())
            helper.stop_helper(app)
            assert solen == len(so.getvalue().splitlines())
        finally:
            sys.stdout = pytest_stdout  # Reset stdout.
            helper.clear_helper(app)


def test_start_and_stop_params(app):
    helper.clear_helper(app)
    pytest_stdout = sys.stdout
    so = io.StringIO()  # Create StringIO object
    sys.stdout = so  # Redirect stdout.
    try:
        helper.start_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)
        assert so.getvalue().splitlines()[-1] == "Helper started successfully."
        helper_args = helper.read_helper_args(app)
        assert {
            "pid": helper_args["pid"],
            "timeout": 60,
            "timeout_check_interval": 1.0,
            "watch_interval": 3.0,
        } == helper_args
    finally:
        try:
            solen = len(so.getvalue().splitlines())
            helper.stop_helper(app)
            assert solen == len(so.getvalue().splitlines())
        finally:
            sys.stdout = pytest_stdout  # Reset stdout.
            helper.clear_helper(app)


def test_modify_helper_detects_repeated_values(app):
    helper.clear_helper(app)
    pytest_stdout = sys.stdout
    so = io.StringIO()  # Create StringIO object
    sys.stdout = so  # Redirect stdout.
    try:
        helper.start_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)
        helper.modify_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)
        assert so.getvalue().splitlines()[-1] == "Helper parameters already met."
    finally:
        sys.stdout = pytest_stdout  # Reset stdout.
        helper.clear_helper(app)


def test_modify_helper_writes_parameters_to_PID_file(app):
    helper.clear_helper(app)
    try:
        helper.start_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)
        pid = helper.get_helper_PID(app)[0]
        helper.modify_helper(app, timeout=40, timeout_check_interval=2, watch_interval=1)
        helper_args = helper.read_helper_args(app)

        assert {
            "pid": pid,
            "timeout": 40,
            "timeout_check_interval": 2.0,
            "watch_interval": 1.0,
        } == helper_args
    finally:
        helper.clear_helper(app)


def test_modify_helper_writes_modification_to_logs(app):
    helper.clear_helper(app)
    t_start = datetime.now()
    try:
        helper.start_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)
        helper.modify_helper(app, timeout=40, timeout_check_interval=2, watch_interval=1)
        pid = helper.get_helper_PID(app)[0]
        xlog = f"Modifying helper with pid={pid} to: timeout=40, timeout_check_interval=2 and watch_interval=1."
        log_lines = helper.read_helper_log(app, t_start)
        assert xlog in log_lines[-1]
    finally:
        helper.clear_helper(app)


# TODO: test_restart_helper
# TODO: test_helper_timeout
# TODO: def test_run_helper_timeouts_when_it_should(app):


def test_run_helper_writes_start_signal_to_log(app):
    found = False
    t_start = datetime.now()
    time.sleep(0.2)
    try:
        helper.run_helper(app, 1, 2, 3)
    except SystemExit:
        xlog = "Helper started with timeout=1, timeout_check_interval=2 and watch_interval=3."
        log_lines = helper.read_helper_log(app, t_start)
        for line in log_lines:
            if xlog in line:
                found = True
                break
        assert found


def test_run_helper_uses_params_over_pid_file_values(app):
    helper.write_helper_args(app, 123, 4, 5, 6)
    found = False
    t_start = datetime.now()
    time.sleep(0.2)
    try:
        helper.run_helper(app, 1, 2, 3)
    except SystemExit:
        xlog = "Helper started with timeout=1, timeout_check_interval=2 and watch_interval=3."
        log_lines = helper.read_helper_log(app, t_start)
        for line in log_lines:
            if xlog in line:
                found = True
                break
        assert found


def test_run_helper_detects_parameter_changes(app):
    helper.write_helper_args(app, 456, 1, 2, 3)
    t_start = datetime.now()
    time.sleep(0.2)
    try:
        helper.run_helper(app, 10, 1, 1)
    except SystemExit:
        xlog = [
            "Updated timeout parameter from 10 to 1.",
            "Updated timeout_check_interval parameter from 1 to 2.",
            "Updated watch_interval parameter from 1 to 3.",
        ]
        log_lines = helper.read_helper_log(app, t_start)
        updates = 0
        for xline in xlog:
            for line in log_lines:
                if xline in line:
                    updates = updates + 1
        assert updates == 3


# TODO: test_helper_cli (or should this be a separate test file?)


# TODO: The tests below are actually functional tests... move them to another folder?
def test_modify_helper(app):
    helper.clear_helper(app)
    start_t = datetime.now()
    time.sleep(0.2)

    helper.start_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)

    # This checks that parameters already in the file are being compared to new inputs
    pytest_stdout = sys.stdout
    so = io.StringIO()  # Create StringIO object
    sys.stdout = so  # Redirect stdout.
    helper.modify_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)
    assert so.getvalue().splitlines()[-1] == "Helper parameters already met."
    sys.stdout = pytest_stdout  # Reset stdout.

    helper.modify_helper(app, timeout=60, timeout_check_interval=2, watch_interval=1)
    # This checks if the file was written with new variables
    args = helper.read_helper_args(app)
    assert args["timeout"] == 60
    assert args["timeout_check_interval"] == 2
    assert args["watch_interval"] == 1
    time.sleep(3.5)

    helper.modify_helper(app, timeout=5, timeout_check_interval=2, watch_interval=1)
    time.sleep(5)
    # If the parameters have been loaded correctly, then it should have timed out by now.
    pid = helper.get_helper_PID(app)
    assert pid == None

    # This checks the logs were updated correctly and without repetition.
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
    helper.clear_helper(app)


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
