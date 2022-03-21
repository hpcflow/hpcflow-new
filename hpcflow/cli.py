import click

import hpcflow.api
from hpcflow import __version__, log, RUN_TIME_INFO
from hpcflow.app_log import LOG_LEVELS


def run_time_info_callback(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(RUN_TIME_INFO)
    ctx.exit()


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
def cli(console_log_level, file_log_level, file_log_path):
    """Computational workflow management."""
    log.update_handlers(console_log_level, file_log_level, file_log_path)


@cli.command()
def make_workflow():
    """Example command on hpcflow"""
    hpcflow.api.make_workflow(dir=".")


if __name__ == "__main__":
    cli()
