import click

import hpcflow.api.config as config_api
from hpcflow.config import Config
from hpcflow.cli.utils import CLI_exception_wrapper_gen
from hpcflow.errors import ConfigError


def show_all_config(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    config_api.show_config()
    ctx.exit()


def show_config_file(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    config_api.show_config_file_contents()
    ctx.exit()


@click.group()
@click.option("--invocation", default="_")
def config(invocation):
    """Configuration sub-command for getting and setting data in the configuration
    file(s)."""


@config.command("list")
def config_list():
    """Show a list of all configurable keys."""
    click.echo("\n".join(Config.get_configurable()))


@config.command("check")
def config_check():
    """Check task schema files and environment that are specified in the config file exist
    and are reachable."""
    config_api.check()


@config.command("get")
@click.argument("name")
@click.option(
    "--all",
    is_flag=True,
    expose_value=False,
    is_eager=True,
    help="Show all configuration items.",
    callback=show_all_config,
)
@click.option(
    "--file",
    is_flag=True,
    expose_value=False,
    is_eager=True,
    help="Show the contents of the configuration file.",
    callback=show_config_file,
)
@CLI_exception_wrapper_gen(ConfigError)
def config_get(name):
    """Show the value of the specified configuration item."""
    val = Config.get(name)
    if isinstance(val, list):
        val = "\n".join(str(i) for i in val)
    click.echo(val)


@config.command("set")
@click.argument("name")
@click.argument("value")
@click.option(
    "--json",
    "is_json",
    is_flag=True,
    default=False,
    help="Interpret VALUE as a JSON string.",
)
@CLI_exception_wrapper_gen(ConfigError)
def config_set(name, value, is_json):
    """Set the value of the specified configuration item."""
    return Config.set_value(name, value, is_json)


@config.command("unset")
@click.argument("name")
@CLI_exception_wrapper_gen(ConfigError)
def config_unset(name):
    """Unset the value of the specified configuration item."""
    return Config.unset_value(name)


@config.command("append")
@click.argument("name")
@click.argument("value")
@click.option(
    "--json",
    "is_json",
    is_flag=True,
    default=False,
    help="Interpret VALUE as a JSON string.",
)
@CLI_exception_wrapper_gen(ConfigError)
def config_append(name, value, is_json):
    """Append a new value to the specified configuration item."""
    return Config.append_value(name, value, is_json)


@config.command("prepend")
@click.argument("name")
@click.argument("value")
@click.option(
    "--json",
    "is_json",
    is_flag=True,
    default=False,
    help="Interpret VALUE as a JSON string.",
)
@CLI_exception_wrapper_gen(ConfigError)
def config_prepend(name, value, is_json):
    """Append a new value to the specified configuration item."""
    return Config.prepend_value(name, value, is_json)


@config.command("pop", context_settings={"ignore_unknown_options": True})
@click.argument("name")
@click.argument("index", type=click.types.INT)
@CLI_exception_wrapper_gen(ConfigError)
def config_pop(name, index):
    """Append a new value to the specified configuration item."""
    return Config.pop_value(name, index)
