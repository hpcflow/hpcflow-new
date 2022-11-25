from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
import time

import click
from platformdirs import user_data_dir
import psutil


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

        # for i in os.get_exec_path():
        #     i_app = Path(i).joinpath(f"{app.name}.cmd")
        #     if i_app.is_file():
        #         break

        args = [
            str(sys.executable),
            "-m",
            "hpcflow.cli.cli",  # TODO: search for correct entry point , or store in run-time-info?
            "--config-dir",
            str(app.config.config_directory),
            "server",
            "run",
            str(polling_interval),
        ]
        print(f"{args=}")

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


def stop_helper(app):

    PID_file = Path(user_data_dir(appname=app.name)).joinpath("pid.txt")
    if not PID_file.is_file():
        print("Server not running!")
    else:
        with PID_file.open("rt") as fp:
            server_pid = int(fp.read().strip())
        proc = psutil.Process(server_pid)
        proc.kill()
        PID_file.unlink()


def run_helper(polling_interval):
    while True:
        with Path("test_file.txt").open("a") as fp:
            fp.write(str(datetime.now()) + "\n")
        time.sleep(polling_interval)
