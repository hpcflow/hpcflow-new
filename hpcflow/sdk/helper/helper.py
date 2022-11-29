from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from platformdirs import user_data_dir
import psutil

from .watcher import MonitorController


DEFAULT_TIMEOUT = 3600  # seconds


def kill_proc_tree(
    pid, sig=signal.SIGTERM, include_parent=True, timeout=None, on_terminate=None
):
    """Kill a process tree (including grandchildren) with signal
    "sig" and return a (gone, still_alive) tuple.
    "on_terminate", if specified, is a callback function which is
    called as soon as a child terminates.
    """
    assert pid != os.getpid(), "won't kill myself"
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    if include_parent:
        children.append(parent)
    for p in children:
        try:
            p.send_signal(sig)
        except psutil.NoSuchProcess:
            pass
    gone, alive = psutil.wait_procs(children, timeout=timeout, callback=on_terminate)
    return (gone, alive)


def get_user_data_dir(app):
    """We segregate by hostname to account for the case where multiple machines might use
    the same shared file system."""
    return Path(user_data_dir(appname=app.name)).joinpath(socket.gethostname())


def get_PID_file_path(app):
    """Get the path to the file containing the process ID of the helper, if running."""
    return get_user_data_dir(app) / "pid.txt"


def get_watcher_file_path(app):
    """Get the path to the watcher file, which contains a list of workflows to watch."""
    return get_user_data_dir(app) / "watch_workflows.txt"


def start_helper(app, polling_interval, timeout):
    PID_file = get_PID_file_path(app)
    if PID_file.is_file():
        print("Helper already running?")
        with PID_file.open("rt") as fp:
            helper_pid = int(fp.read().strip())
            print(f"{helper_pid=}")

    else:
        logger = get_helper_logger(app)
        logger.info(f"Starting helper with {polling_interval=!r} and {timeout=!r}.")
        kwargs = {}
        if os.name == "nt":
            kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW}

        if isinstance(timeout, timedelta):
            timeout = timeout.seconds

        args = app.run_time_info.get_invocation_command()
        args += [
            "--config-dir",
            str(app.config.config_directory),
            "helper",
            "run",
            "--timeout",
            str(timeout),
            str(polling_interval),
        ]

        proc = subprocess.Popen(
            args=args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            **kwargs,
        )

        logger.info(f"Writing process ID {proc.pid} to file.")
        try:
            with PID_file.open("wt") as fp:
                fp.write(f"{proc.pid}\n")
        except FileNotFoundError as err:
            logger.error(
                f"Could not write to the PID file {PID_file!r}; killing helper process. "
                f"Exception was: {err!r}"
            )
            proc.kill()
            sys.exit(1)


def get_helper_PID(app):

    PID_file = get_PID_file_path(app)
    if not PID_file.is_file():
        print("Helper not running!")
        return None
    else:
        with PID_file.open("rt") as fp:
            helper_pid = int(fp.read().strip())
        return helper_pid, PID_file


def stop_helper(app):
    logger = get_helper_logger(app)
    pid_info = get_helper_PID(app)
    if pid_info:
        logger.info(f"Stopping helper.")
        pid, pid_file = pid_info
        kill_proc_tree(pid=pid)
        pid_file.unlink()

        workflow_dirs_file_path = get_watcher_file_path(app)
        logger.info(f"Deleting watcher file: {str(workflow_dirs_file_path)}")
        workflow_dirs_file_path.unlink()


def get_helper_uptime(app):
    pid_info = get_helper_PID(app)
    if pid_info:
        proc = psutil.Process(pid_info[0])
        create_time = datetime.fromtimestamp(proc.create_time())
        uptime = datetime.now() - create_time
        return uptime


def get_helper_logger(app):

    log_path = get_watcher_file_path(app).parent / "watcher.log"
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    f_handler = RotatingFileHandler(log_path, maxBytes=(5 * 2**20), backupCount=3)
    f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    return logger


def helper_timeout(app, timeout, controller, logger):
    """Kill the helper due to running duration exceeding the timeout."""

    logger.info(f"Helper exiting due to timeout ({timeout!r}).")
    pid_info = get_helper_PID(app)
    if pid_info:
        pid_file = pid_info[1]
        logger.info(f"Deleting PID file: {pid_file!r}.")
        pid_file.unlink()

    logger.info(f"Stopping all watchers.")
    controller.stop()
    controller.join()

    logger.info(f"Deleting watcher file: {str(controller.workflow_dirs_file_path)}")
    controller.workflow_dirs_file_path.unlink()

    sys.exit(0)


def run_helper(app, polling_interval, timeout=DEFAULT_TIMEOUT):

    # TODO: when writing to watch_workflows from a workflow, copy, modify and then rename
    # this will be atomic - so there will be only one event fired.
    # Also return a local run ID (the position in the file) to be used in jobscript naming

    if not isinstance(timeout, timedelta):
        timeout = timedelta(seconds=timeout)

    start_time = datetime.now()
    logger = get_helper_logger(app)
    controller = MonitorController(get_watcher_file_path(app), logger)

    try:
        while True:
            dt = datetime.now() - start_time
            if dt >= timeout:
                helper_timeout(app, timeout, controller, logger)
            time.sleep(polling_interval)

    except KeyboardInterrupt:
        controller.stop()

    controller.join()  # wait for it to stop!
