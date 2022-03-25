import click
from colorama import init as colorama_init
from termcolor import colored

import hpcflow.api
from hpcflow import __version__, log, RUN_TIME_INFO
from hpcflow.app_log import LOG_LEVELS
from hpcflow.cli.config import config as config_group
from hpcflow.config import ConfigLoader
from hpcflow.errors import ConfigError

from tkinter import _tkinter

print(f"_tkinter: {_tkinter}")

colorama_init(autoreset=True)


def run_time_info_callback(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(RUN_TIME_INFO)
    ctx.exit(0)


@click.group(name="hpcflow")
@click.version_option(version=__version__, package_name="hpcflow", prog_name="hpcflow")
@click.help_option()
@click.option(
    "--run-time-info",
    help="Print run-time information",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=run_time_info_callback,
)
@click.option(
    "--console-log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default="WARNING",
    help="Set console log level",
)
@click.option(
    "--file-log-level",
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    default="INFO",
    help="Set file log level",
)
@click.option("--file-log-path", help="Set file log path")
@click.option("--config-dir", help="Set the configuration directory.")
@click.pass_context
def cli(ctx, config_dir, console_log_level, file_log_level, file_log_path):
    """Computational workflow management."""
    log.update_handlers(console_log_level, file_log_level, file_log_path)
    try:
        ConfigLoader(config_dir=config_dir)
    except ConfigError as err:
        click.echo(f"{colored(err.__class__.__name__, 'red')}: {err}")
        ctx.exit(1)


@cli.command()
def make_workflow():
    """Example command on hpcflow"""
    hpcflow.api.make_workflow(dir=".")


cli.add_command(config_group)

if __name__ == "__main__":
    cli()
