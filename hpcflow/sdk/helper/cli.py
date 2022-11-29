from datetime import timedelta

import click
import psutil

from .helper import (
    DEFAULT_TIMEOUT,
    start_helper,
    stop_helper,
    run_helper,
    get_helper_PID,
    get_helper_uptime,
)


def get_helper_CLI(app):
    """Generate the CLI to provide some server-like functionality."""

    @click.group()
    def helper():
        pass

    @helper.command()
    @click.argument("polling_interval", type=click.INT)
    @click.option(
        "-t",
        "--timeout",
        type=click.FLOAT,
        default=DEFAULT_TIMEOUT,
        help="Timeout in seconds.",
    )
    def start(polling_interval, timeout):
        """Start the helper process."""
        start_helper(app, polling_interval, timeout)

    @helper.command()
    def stop():
        """Stop the helper process, if it is running."""
        stop_helper(app)

    @helper.command()
    @click.argument("polling_interval", type=click.INT)
    @click.option(
        "-t",
        "--timeout",
        type=click.FLOAT,
        default=DEFAULT_TIMEOUT,
        help="Timeout in seconds.",
    )
    def run(polling_interval, timeout):
        """Run the helper functionality."""
        timeout = timedelta(seconds=timeout)
        run_helper(app, polling_interval, timeout)

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
                print(f"Removing file {pid_file!r}")
                pid_file.unlink()

    @helper.command()
    def uptime():
        """Get the uptime of the helper process."""
        out = get_helper_uptime(app)
        if out:
            click.echo(out)

    return helper
