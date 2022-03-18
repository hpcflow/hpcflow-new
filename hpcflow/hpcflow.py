from dataclasses import dataclass

import click

from hpcflow.cli import cli
from hpcflow.runtime import RunTimeInfo


@dataclass
class HPCFlow:
    """Class for instantiating an HPCFlow application, which may provide, for instance,
    custom parameter logic, over the top of that provided by hpcflow."""

    name: str
    version: str

    def __post_init__(self):
        self.CLI = self.make_CLI()

    def make_CLI(self):
        def new_CLI(ctx, debug):
            ctx.obj = RunTimeInfo(name=self.name, debug=debug)
            if debug:
                click.echo("Debug mode is ON.")
                click.echo(f"run_time_info is: {ctx.obj!r}.")

        new_CLI = click.version_option(
            package_name=self.name, prog_name=self.name, version=self.version
        )(new_CLI)
        new_CLI = click.option("--debug/--no-debug", default=False)(new_CLI)
        new_CLI = click.pass_context(new_CLI)
        new_CLI = click.group(name=self.name)(new_CLI)

        # add hpcflow CLI as a sub command:
        new_CLI.add_command(cli)
        for name, command in cli.commands.items():
            # add each hpcflow command as a new CLI command:
            new_CLI.add_command(command, name=name)
        return new_CLI
