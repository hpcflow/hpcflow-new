import click

import hpcflow.api
from hpcflow import __version__, log
from hpcflow.app_log import LOG_LEVELS


@click.group(name="hpcflow")
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
@click.version_option(version=__version__, package_name="hpcflow", prog_name="hpcflow")
def cli(console_log_level, file_log_level, file_log_path):
    """Computational workflow management."""
    log.update_handlers(console_log_level, file_log_level, file_log_path)


@cli.command()
def make_workflow():
    """Example command on hpcflow"""
    hpcflow.api.make_workflow(dir=".")


if __name__ == "__main__":
    cli()
