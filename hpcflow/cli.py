import click
from hpcflow import __version__
from hpcflow.runtime import RunTimeInfo


@click.group(name="hpcflow")
@click.pass_context
@click.option("--debug/--no-debug", default=False)
@click.version_option(version=__version__, package_name="hpcflow", prog_name="hpcflow")
def cli(ctx, debug):
    ctx.obj = RunTimeInfo(name="hpcflow", debug=debug)
    if debug:
        click.echo("Debug mode is ON.")
        click.echo(f"run_time_info is: {ctx.obj!r}.")


@cli.command()
def make_workflow():
    """Example command on hpcflow"""
    pass


if __name__ == "__main__":
    cli()
