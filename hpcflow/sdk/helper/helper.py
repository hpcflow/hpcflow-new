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
from inspect import getsourcefile
from hpcflow import cli

from platformdirs import user_data_dir
import psutil

from .watcher import MonitorController


DEFAULT_TIMEOUT = 3600  # seconds
DEFAULT_TIMEOUT_CHECK = 60  # seconds
DEFAULT_WATCH_INTERVAL = 10  # seconds


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


def get_helper_log_path(app):
    """Get the log file path for the helper."""
    return get_user_data_dir(app) / "helper.log"


def get_helper_watch_list(app):
    """Get the list of workflows currently being watched by the helper process."""
    logger = get_helper_logger(app)
    watch_file_path = get_watcher_file_path(app)
    if watch_file_path.exists():
        return MonitorController.parse_watch_workflows_file(watch_file_path, logger)


def start_helper(
    app,
    timeout=DEFAULT_TIMEOUT,
    timeout_check_interval=DEFAULT_TIMEOUT_CHECK,
    watch_interval=DEFAULT_WATCH_INTERVAL,
    logger=None,
):
    PID_file = get_PID_file_path(app)
    if PID_file.is_file():
        helper_pid = get_helper_PID(app)[0]
        print(f"Helper already running, with process ID: {helper_pid}")

    else:
        logger = logger or get_helper_logger(app)
        logger.info(
            f"Starting helper with timeout={timeout!r}, timeout_check_interval="
            f"{timeout_check_interval!r} and watch_interval={watch_interval!r}."
        )
        kwargs = {}
        if os.name == "nt":
            kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW}

        if isinstance(timeout, timedelta):
            timeout = timeout.total_seconds()
        if isinstance(timeout_check_interval, timedelta):
            timeout_check_interval = timeout_check_interval.total_seconds()
        if isinstance(watch_interval, timedelta):
            watch_interval = watch_interval.total_seconds()

        args = app.run_time_info.get_invocation_command()
        # TODO: This is not ideal, but works for the timebeing...
        logger.info(f"fhadb - Invocation command:\n\n{args[0]}\n{args[1]}\n")
        if "pytest/__main__.py" in args[-1]:
            args[-1] = os.path.dirname(getsourcefile(cli)) + "/cli.py"
            logger.info(f"fhadb - Modified invocation command:\n\n{args[0]}\n{args[1]}\n")
        elif "pytest\\__main__.py" in args[-1]:
            args[-1] = os.path.dirname(getsourcefile(cli)) + "\\cli.py"
            logger.info(f"fhadb - Modified invocation command:\n\n{args[0]}\n{args[1]}\n")
        args += [
            "--config-dir",
            str(app.config.config_directory),
            "helper",
            "run",
            "--timeout",
            str(timeout),
            "--timeout-check-interval",
            str(timeout_check_interval),
            "--watch-interval",
            str(watch_interval),
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
            write_helper_args(
                app, proc.pid, timeout, timeout_check_interval, watch_interval
            )
            # Make sure that the process is actually running.
            try:
                time.sleep(0.2)  # Sleep time is necessary for poll to work.
                pr = proc.poll()
                logger.info(f"fhadb - poll result: {pr}")
                procinfo = psutil.Process(proc.pid)
                logger.info(f"fhadb - proc info: {procinfo}")
                logger.info(f"Process {proc.pid} successfully running.")
            except psutil.NoSuchProcess:
                logger.error(f"Process {proc.pid} failed to start.")
                sys.exit(1)
        except FileNotFoundError as err:
            logger.error(f"Killing helper process. ")
            proc.kill()
            sys.exit(1)


def modify_helper(
    app,
    timeout=DEFAULT_TIMEOUT,
    timeout_check_interval=DEFAULT_TIMEOUT_CHECK,
    watch_interval=DEFAULT_WATCH_INTERVAL,
):
    PID_file = get_PID_file_path(app)
    if PID_file.is_file():
        helper_args = read_helper_args(app)
        if (
            helper_args["timeout"] != timeout
            or helper_args["timeout_check_interval"] != timeout_check_interval
            or helper_args["watch_interval"] != watch_interval
        ):
            logger = get_helper_logger(app)
            logger.info(
                f"Modifying helper with pid={helper_args['pid']}"
                f" to: timeout={timeout!r}, timeout_check_interval="
                f"{timeout_check_interval!r} and watch_interval={watch_interval!r}."
            )

            if isinstance(timeout, timedelta):
                timeout = timeout.total_seconds()
            if isinstance(timeout_check_interval, timedelta):
                timeout_check_interval = timeout_check_interval.total_seconds()
            if isinstance(watch_interval, timedelta):
                watch_interval = watch_interval.total_seconds()

            try:
                write_helper_args(
                    app,
                    helper_args["pid"],
                    timeout,
                    timeout_check_interval,
                    watch_interval,
                )
            except FileNotFoundError as err:
                sys.exit(1)
        else:
            print("Helper parameters already met.")
    else:
        print(f"Helper not running!")


def restart_helper(
    app,
    timeout=DEFAULT_TIMEOUT,
    timeout_check_interval=DEFAULT_TIMEOUT_CHECK,
    watch_interval=DEFAULT_WATCH_INTERVAL,
):
    logger = stop_helper(app, return_logger=True)
    start_helper(app, timeout, timeout_check_interval, watch_interval, logger=logger)


def write_helper_args(
    app,
    pid,
    timeout=DEFAULT_TIMEOUT,
    timeout_check_interval=DEFAULT_TIMEOUT_CHECK,
    watch_interval=DEFAULT_WATCH_INTERVAL,
):
    PID_file = get_PID_file_path(app)
    try:
        with PID_file.open("wt") as fp:
            fp.write(f"pid = {pid}\n")
            fp.write(f"timeout = {timeout}\n")
            fp.write(f"timeout_check_interval = {timeout_check_interval}\n")
            fp.write(f"watch_interval = {watch_interval}\n")
    except FileNotFoundError as err:
        logger = get_helper_logger(app)
        logger.error(
            f"Could not write to the PID file {PID_file!r};" f"Exception was: {err!r}"
        )
        print(err)


def read_helper_args(app):
    PID_file = get_PID_file_path(app)
    if not PID_file.is_file():
        print("Helper not running!")
        return None
    else:
        helper_args = {}
        with PID_file.open("rt") as fp:
            for line in fp:
                (key, val) = line.split(" = ")
                helper_args[key] = float(val)
        helper_args["pid"] = int(helper_args["pid"])
        return helper_args


def get_helper_PID(app):

    PID_file = get_PID_file_path(app)
    if not PID_file.is_file():
        print("Helper not running!")
        return None
    else:
        with PID_file.open("rt") as fp:
            helper_pid = int(fp.readline().strip("pid =\n"))
        return helper_pid, PID_file


def stop_helper(app, return_logger=False):
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

    if return_logger:
        return logger


def clear_helper(app):
    try:
        stop_helper(app)
    except psutil.NoSuchProcess:
        pid_info = get_helper_PID(app)
        if pid_info:
            pid_file = pid_info[1]
            print(f"Removing file {pid_file!r}")
            pid_file.unlink()


def get_helper_uptime(app):
    logger = get_helper_logger(app)
    logger.info(f"fhadb Process info with psutil:\n\n")
    logger.info(f"fhadb - uptime: {uptime}")
    logger.info(f"fhadb - exe:{proc.exe()}")
    logger.info(f"fhadb - cwd:{proc.cwd()}")
    logger.info(f"fhadb - command:{proc.cmdline()}")
    logger.info(f"fhadb - status:{proc.status()}")
    logger.info(f"fhadb\n\n")
    with open("fhadb.txt", "r") as f:
        logger.info(f"fhadb file:\n{f.read()}\n")
    pid_info = get_helper_PID(app)
    if pid_info:
        proc = psutil.Process(pid_info[0])
        create_time = datetime.fromtimestamp(proc.create_time())
        uptime = datetime.now() - create_time
        return uptime


def get_helper_logger(app):

    log_path = get_helper_log_path(app)
    logger = logging.getLogger(__name__)
    if not len(logger.handlers):
        logger.setLevel(logging.INFO)
        f_handler = RotatingFileHandler(log_path, maxBytes=(5 * 2**20), backupCount=3)
        f_format = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)

    return logger


def helper_timeout(app, timeout, controller, logger):
    """Kill the helper due to running duration exceeding the timeout."""

    with open("fhadb.txt", "a") as f:
        f.write(f"\nfhadb - Helper exiting due to timeout")
        logger.info(f"Helper exiting due to timeout ({timeout!r}).")
        pid_info = get_helper_PID(app)
        f.write(f"\nfhadb - pid_info {pid_info}")
        if pid_info:
            pid_file = pid_info[1]
            f.write(f"\nfhadb - pid_info[1] {pid_info[1]}")
            logger.info(f"Deleting PID file: {pid_file!r}.")
            pid_file.unlink()
            f.write(f"\nfhadb - pid_file unlinked... maybe...")

        f.write(f"\nfhadb - Stopping all watchers")
        logger.info(f"Stopping all watchers.")
        f.write(f"\nfhadb - stopping controller")
        controller.stop()
        f.write(f"\nfhadb - joining controller")
        controller.join()
        f.write(f"\nfhadb - joined... or is it joined?")

        f.write(f"\nfhadb - Deleting watcher file")
        logger.info(f"Deleting watcher file: {str(controller.workflow_dirs_file_path)}")
        f.write(f"\nfhadb - deleted")
        controller.workflow_dirs_file_path.unlink()
        f.write(f"\nfhadb - unlinked")

    sys.exit(0)


def run_helper(
    app,
    timeout=DEFAULT_TIMEOUT,
    timeout_check_interval=DEFAULT_TIMEOUT_CHECK,
    watch_interval=DEFAULT_WATCH_INTERVAL,
):
    with open("fhadb.txt", "w") as f:
        f.write(f"fhadb - Can I get the helper logger?")
        logger = get_helper_logger(app)
        f.write(f"\nfhadb - Seems like I just did!")
        f.write(f"\nfhadb - Now I'll write something to the log...")
        logger.info(f"fhadb - I am inside the run_helper function")
        f.write(f"\nfhadb - Done, log has been written to!... has it, really?")

    # TODO: when writing to watch_workflows from a workflow, copy, modify and then rename
    # this will be atomic - so there will be only one event fired.
    # Also return a local run ID (the position in the file) to be used in jobscript naming

    # TODO: we will want to set the timeout to be slightly more than the largest allowable
    # walltime in the case of scheduler submissions.

    if isinstance(timeout, timedelta):
        timeout_s = timeout.total_seconds()
    else:
        timeout_s = timeout
        timeout = timedelta(seconds=timeout_s)

    if isinstance(timeout_check_interval, timedelta):
        timeout_check_interval_s = timeout_check_interval.total_seconds()
    else:
        timeout_check_interval_s = timeout_check_interval
        timeout_check_interval = timedelta(seconds=timeout_check_interval_s)

    start_time = datetime.now()
    end_time = start_time + timeout
    # logger = get_helper_logger(app)
    controller = MonitorController(get_watcher_file_path(app), watch_interval, logger)
    helper_args = read_helper_args(app)
    with open("fhadb.txt", "a") as f:
        f.write(f"\nfhadb - I am about to enter the while True...")
    try:
        while True:
            time_left_s = (end_time - datetime.now()).total_seconds()
            with open("fhadb.txt", "a") as f:
                f.write(
                    f"\nfhadb - I am inside while True loop"
                    + f"\nTime left: {time_left_s}"
                    + f"\nTimeout: {timeout}"
                    + f"\nTimeout-check-interval: {timeout_check_interval_s}"
                )
            logger.info(
                f"fhadb - I am inside the while True loop."
                + f"\nTime left: {time_left_s}"
                + f"\nTimeout: {timeout}"
                + f"\nTimeout-check-interval: {timeout_check_interval_s}"
            )
            with open("fhadb.txt", "a") as f:
                f.write(f"\nfhadb - Just wrote to log again... or did I?")
                f.write(
                    f"\nfhadb - Coming up next time_left_s:{time_left_s}, condition: {(time_left_s <= 0)}"
                )
            if time_left_s <= 0:

                with open("fhadb.txt", "a") as f:
                    f.write(f"\nfhadb - Time's up! I'm stopping")
                helper_timeout(app, timeout, controller, logger)
            time.sleep(min(timeout_check_interval_s, time_left_s))
            # Reading args from PID file
            helper_args_new = read_helper_args(app)
            for name, new_val in helper_args_new.items():
                if new_val != helper_args[name]:
                    with open("fhadb.txt", "a") as f:
                        f.write(f"\nfhadb - I detected a change!")
                    change = f"{name} parameter from {helper_args[name]} to {new_val}."
                    helper_args[name] = new_val
                    if name in ["timeout", "timeout_check_interval"]:
                        t = helper_args[name]
                        if isinstance(t, timedelta):
                            t_s = t.total_seconds()
                        else:
                            t_s = t
                        if name == "timeout":
                            timeout = timedelta(seconds=t_s)
                            end_time = start_time + timeout
                        else:  # name == "timeout_check_interval"
                            timeout_check_interval_s = t_s
                    else:  # name == "watch_interval"
                        controller.stop()
                        # TODO: we might need to consider if a workflow change could be missed during this stop
                        controller = MonitorController(
                            get_watcher_file_path(app), helper_args[name], logger
                        )
                    with open("fhadb.txt", "a") as f:
                        f.write(f"\nfhadb - I just updated {change}")
                    logger.info(f"Updated {change}")
                    with open("fhadb.txt", "a") as f:
                        f.write(f"\nfhadb - And recorded it to the log... in theory...")

    except KeyboardInterrupt:
        controller.stop()

    controller.join()  # wait for it to stop!
