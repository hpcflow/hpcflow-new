from datetime import datetime
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import click
from platformdirs import user_data_dir
import psutil


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


def get_helper_CLI(app):
    """Generate the CLI to provide some server-like functionality."""

    @click.group()
    def helper():
        pass

    @helper.command()
    @click.argument("polling_interval", type=click.INT)
    def start(polling_interval):
        """Start the helper process."""
        start_helper(app, polling_interval)

    @helper.command()
    def stop():
        """Stop the helper process, if it is running."""
        stop_helper(app)

    @helper.command()
    @click.argument("polling_interval", type=click.INT)
    def run(polling_interval):
        """Run the helper functionality."""
        run_helper(polling_interval)

    @helper.command()
    @click.option("-f", "--file", is_flag=True)
    def pid(file):
        """Get the process ID of the running helper, if running."""
        pid_info = get_helper_PID(app)
        if pid_info:
            pid, pid_file = pid_info
            if file:
                click.echo(f"{pid} ({str(pid_file)})")
            else:
                click.echo(pid)

    @helper.command()
    def clear():
        """Remove the PID file (and kill the process if it exists). This should not
        normally be needed."""
        try:
            stop_helper(app)
        except psutil.NoSuchProcess:
            pid_info = get_helper_PID(app)
            if pid_info:
                pid_file = pid_info[1]
                print("Removing file {pid_file!r}")
                pid_file.unlink()

    @helper.command()
    def uptime():
        """Get the uptime of the helper process."""
        out = get_helper_uptime(app)
        if out:
            click.echo(out)

    return helper


def start_helper(app, polling_interval):

    PID_file = Path(user_data_dir(appname=app.name)).joinpath("pid.txt")
    if PID_file.is_file():
        print("Helper already running?")
        with PID_file.open("rt") as fp:
            helper_pid = int(fp.read().strip())
            print(f"{helper_pid=}")

    else:
        print("Starting helper")
        kwargs = {}
        if os.name == "nt":
            kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW}

        args = app.run_time_info.get_invocation_command()
        args += [
            "--config-dir",
            str(app.config.config_directory),
            "helper",
            "run",
            str(polling_interval),
        ]

        proc = subprocess.Popen(
            args=args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            **kwargs,
        )

        print(f"Writing process ID {proc.pid} to file.")
        with PID_file.open("wt") as fp:
            fp.write(f"{proc.pid}\n")


def get_helper_PID(app):

    PID_file = Path(user_data_dir(appname=app.name)).joinpath("pid.txt")
    if not PID_file.is_file():
        print("Helper not running!")
        return None
    else:
        with PID_file.open("rt") as fp:
            helper_pid = int(fp.read().strip())
        return helper_pid, PID_file


def stop_helper(app):
    pid_info = get_helper_PID(app)
    if pid_info:
        pid, pid_file = pid_info
        kill_proc_tree(pid=pid)
        pid_file.unlink()


def get_helper_uptime(app):
    pid_info = get_helper_PID(app)
    if pid_info:
        proc = psutil.Process(pid_info[0])
        create_time = datetime.fromtimestamp(proc.create_time())
        uptime = datetime.now() - create_time
        return uptime


def run_helper(polling_interval):
    while True:
        with Path("test_file.txt").open("a") as fp:
            fp.write(str(datetime.now()) + "\n")
        time.sleep(polling_interval)
