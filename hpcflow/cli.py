import click
from hpcflow import __version__


@click.group(name="hpcflow")
@click.version_option(version=__version__, package_name="hpcflow", prog_name="hpcflow")
def cli():
    pass


@cli.command()
def make_workflow():
    """Command on hpcflow"""


if __name__ == "__main__":
    cli()
