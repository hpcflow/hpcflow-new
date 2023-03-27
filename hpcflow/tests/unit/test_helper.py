import pytest
from datetime import datetime, timedelta
import time
from pathlib import Path

import os
import io
import sys
import subprocess
from multiprocessing import Process, Queue
import psutil

from hpcflow.sdk.helper import helper
from hpcflow.api import hpcflow, load_config


def get_sleep_shell_command(seconds):
    if os.name == "posix":
        return ["sleep", str(seconds)]
    elif os.name == "nt":
        return ["powershell", "sleep", str(seconds)]


@pytest.fixture
def app(tmp_path):
    load_config(config_dir=tmp_path)
    return hpcflow


# TODO: test_get_user_data_dir


def test_get_PID_file_path(app, mocker):
    mocker.patch(
        "hpcflow.sdk.helper.helper.get_user_data_dir",
        return_value=Path("/user_data_dir"),
    )
    pid_fp = helper.get_PID_file_path(app)
    assert Path("/user_data_dir/pid.txt") == pid_fp


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


def test_get_helper_log_path(app, mocker):
    mocker.patch(
        "hpcflow.sdk.helper.helper.get_user_data_dir",
        return_value=Path("/user_data_dir"),
    )
    log_fp = helper.get_helper_log_path(app)
    assert Path("/user_data_dir/helper.log") == log_fp


def test_helper_logger(app):
    log_fp = helper.get_helper_log_path(app)
    if log_fp.is_file():
        log_fp.unlink()
    logger = helper.get_helper_logger(app)
    assert len(logger.handlers) == 1
    logger.info("***Test info log.")
    logger.error("***Test error log.")
    logger2 = helper.get_helper_logger(app)
    logger2.info("***Test for duplicate logs.")
    assert len(logger.handlers) == 1
    with log_fp.open("rt") as logs:
        lines = logs.readlines()
        assert len(lines) == 3
        assert "INFO - ***Test info log." in lines[0]
        assert "ERROR - ***Test error log." in lines[1]
        assert "INFO - ***Test for duplicate logs." in lines[2]


def test_get_watcher_file_path(app, mocker):
    mocker.patch(
        "hpcflow.sdk.helper.helper.get_user_data_dir",
        return_value=Path("/user_data_dir"),
    )
    watcher_fp = helper.get_watcher_file_path(app)
    assert Path("/user_data_dir/watch_workflows.txt") == watcher_fp


def test_get_helper_watch_list(app, mocker):
    mocklist = [{"path": Path("mockpath1")}, {"path": Path("mockpath2")}]
    mocker.patch(
        "hpcflow.sdk.helper.helper.get_watcher_file_path",
        return_value=Path(os.getcwd()),
    )
    mocker.patch(
        "hpcflow.sdk.helper.watcher.MonitorController.parse_watch_workflows_file",
        return_value=mocklist,
    )
    helper_watch_list = helper.get_helper_watch_list(app)
    assert mocklist == helper_watch_list


def test_get_helper_watch_list_no_file(app, mocker):
    mocklist = [{"path": Path("mockpath1")}, {"path": Path("mockpath2")}]
    mocker.patch(
        "hpcflow.sdk.helper.helper.get_watcher_file_path",
        return_value=Path("/non_existent_watcher_file_path"),
    )
    mocker.patch(
        "hpcflow.sdk.helper.watcher.MonitorController.parse_watch_workflows_file",
        return_value=mocklist,
    )
    helper_watch_list = helper.get_helper_watch_list(app)
    assert None == helper_watch_list


def test_clear_helper_no_process(app):
    pid_fp = helper.get_PID_file_path(app)
    with pid_fp.open("wt") as fp:
        fp.write(f"pid = {12345}\n")
    helper.clear_helper(app)
    assert pid_fp.is_file() == False


@pytest.mark.skipif(  # Skip MacOs with python 3.7. Also skipping in CentOS at the gh action
    (sys.platform == "darwin") and (sys.version_info < (3, 8)),
    reason="Unknown failure. Illegal instruction",
)
def test_kill_proc_tree():
    queue = Queue()
    depth = 3
    parent = Process(
        target=sleeping_child,
        args=[queue, 10, depth],
    )
    parent.start()
    children = []
    while len(children) < depth:
        children.append(queue.get())
    children.append(parent.pid)
    try:
        g, a = helper.kill_proc_tree(parent.pid)
        assert len(g) == depth + 1
        assert len(a) == 0
    finally:
        sitll_running = 0
        for pid in children:
            try:
                proc = psutil.Process(pid)
                print(f"Process {proc.pid} still running!")
                sitll_running = sitll_running + 1
            except psutil.NoSuchProcess:
                pass
        assert sitll_running == 0, "Some processes were not killed"


def test_get_helper_uptime(app):
    # TODO: get_helper_uptime uses proc.create_time, which is known to be inacurate.
    # See https://github.com/giampaolo/psutil/issues/877
    # This is probably only an issue for tests, but uptime should not be trusted for
    # measurements under the "1 second" scale.
    # When this is solved, the "time.sleep(0.5)" lines can be removed.
    try:
        pid_fp = helper.get_PID_file_path(app)
        t_0 = datetime.now()
        time.sleep(0.5)
        proc = subprocess.Popen(get_sleep_shell_command(100))
        time.sleep(0.5)
        t_1 = datetime.now()
        with pid_fp.open("wt") as fp:
            fp.write(f"pid = {proc.pid}\n")
        t_2 = datetime.now()
        time.sleep(0.5)
        uptime = helper.get_helper_uptime(app)
        time.sleep(0.5)
        t_3 = datetime.now()
        out_t = t_3 - t_0
        in_t = t_2 - t_1
        assert out_t > uptime
        assert in_t < uptime
    finally:
        helper.kill_proc_tree(proc.pid)
        with pytest.raises(psutil.NoSuchProcess):
            psutil.Process(proc.pid)


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


def test_restart_helper(app, mocker):
    stop_spy = mocker.patch("hpcflow.sdk.helper.helper.stop_helper")
    start_spy = mocker.patch("hpcflow.sdk.helper.helper.start_helper")
    helper.restart_helper(app)
    assert stop_spy.call_count == 1
    assert start_spy.call_count == 1


def test_helper_timeout(app, mocker):
    timeout = timedelta(seconds=60)
    pid_fp = helper.get_PID_file_path(app)
    with pid_fp.open("wt") as fp:
        fp.write(f"pid = {12345}\n")
    pid_fp2 = Path(str(pid_fp)[:-4] + "2.txt")
    with pid_fp2.open("wt") as fp:
        fp.write(f"workflow_dirs_file\n")
    m_controller = mocker.Mock()
    m_controller.workflow_dirs_file_path = pid_fp2
    m_logger = mocker.Mock()
    try:
        helper.helper_timeout(app, timeout, m_controller, m_logger)
    except SystemExit:
        assert 4 == m_logger.info.call_count
        m_logger.info.assert_has_calls(
            [
                mocker.call(f"Helper exiting due to timeout ({timeout!r})."),
                mocker.call(f"Deleting PID file: {pid_fp!r}."),
                mocker.call("Stopping all watchers."),
                mocker.call(f"Deleting watcher file: {pid_fp2}"),
            ]
        )
        m_controller.stop.assert_called_once()
        m_controller.join.assert_called_once()
    finally:
        assert pid_fp.is_file() == False
        assert pid_fp2.is_file() == False


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


def test_run_helper_timeouts_when_it_should(app):
    helper.clear_helper(app)
    pid_fp = helper.get_PID_file_path(app)
    t_start = datetime.now()
    time.sleep(0.1)
    try:
        helper.run_helper(app, 1, 2, 3)
        time.sleep(5)
        assert False
    except SystemExit:
        assert pid_fp.is_file() == False
        log_lines = helper.read_helper_log(app, t_start)
        for line in log_lines:
            if "Helper started with timeout=" in line:
                (ts, m) = line.split(" - INFO - ")
                (t0, s) = ts.split(" - ")
                t0 = datetime.strptime(t0, "%Y-%m-%d %H:%M:%S,%f")
            elif "Helper exiting due to timeout" in line:
                (ts, m) = line.split(" - INFO - ")
                (tf, s) = ts.split(" - ")
                tf = datetime.strptime(tf, "%Y-%m-%d %H:%M:%S,%f")
                break
        lifespan = tf - t0
        assert lifespan.seconds == 1
    finally:
        helper.clear_helper(app)


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


# TODO: The test below is actually a functional test... move to another folder?
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


def sleeping_child(queue, t, depth):
    if depth > 1:
        child = Process(target=sleeping_child, args=[queue, t, depth - 1])
    else:
        child = Process(target=time.sleep, args=[t])
    child.start()
    queue.put(child.pid)
    child.join()
