"""Module defining a function that returns the click CLI group for manipulating the app
configuration."""

import json
import logging
import warnings
from functools import wraps
from contextlib import contextmanager

import click
from colorama import init as colorama_init
from termcolor import colored

from hpcflow.sdk.core import utils

from .errors import ConfigError

logger = logging.getLogger(__name__)

colorama_init(autoreset=True)


def custom_warning_formatter(message, category, filename, lineno, file=None, line=None):
    """Simple warning formatter that shows just the warning type and the message. We use
    this in the CLI, to avoid producing distracting output."""
    return f"{colored(category.__name__, 'yellow')}: {message}\n"


@contextmanager
def warning_formatter(func=custom_warning_formatter):
    """Context manager for modifying the warnings formatter.

    Parameters
    ----------
    func : function to set as the `warnings.formatwarning` function.

    """
    existing_func = warnings.formatwarning
    try:
        warnings.formatwarning = func
        yield
    finally:
        warnings.formatwarning = existing_func


def CLI_exception_wrapper_gen(*exception_cls):
    """Decorator factory"""

    def CLI_exception_wrapper(func):
        """Decorator

        Parameters
        ----------
        func
            Function that return a truthy value if the ???
        """

        @wraps(func)
        @click.pass_context
        def wrapper(ctx, *args, **kwargs):
            try:
                with warning_formatter():
                    out = func(*args, **kwargs)
                if out is not None:
                    click.echo(f"{colored('✔ Config file updated.', 'green')}")
                return out
            except exception_cls as err:
                click.echo(f"{colored(err.__class__.__name__, 'red')}: {err}")
                ctx.exit(1)

        return wrapper

    return CLI_exception_wrapper


def get_config_CLI(app):
    """Generate the configuration CLI for the app."""

    def show_all_config(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        ctx.obj["config"]._show(config=True, metadata=False)
        ctx.exit()

    def show_all_metadata(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        ctx.obj["config"]._show(config=False, metadata=True)
        ctx.exit()

    def show_config_file(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        print(ctx.obj["config"].config_file_contents)
        ctx.exit()

    @click.group()
    @click.option(
        "--no-callback",
        multiple=True,
        help="Exclude a named get/set callback function during execution of the command.",
    )
    @click.pass_context
    def config(ctx, no_callback):
        """Configuration sub-command for getting and setting data in the configuration
        file(s)."""
        ctx.ensure_object(dict)
        ctx.obj["config"] = app.config
        if no_callback:
            ctx.obj["config"]._disable_callbacks(no_callback)

    @config.command("list")
    @click.pass_context
    def config_list(ctx):
        """Show a list of all configurable keys."""
        click.echo("\n".join(ctx.obj["config"].get_configurable()))

    @config.command("import")
    @click.argument("file_path")
    @click.option(
        "--rename/--no-rename",
        default=True,
        help=(
            "Rename the currently loaded config file according to the name of the file "
            "that is being imported (default is to rename). Ignored if `--new` is "
            "specified."
        ),
    )
    @click.option(
        "--new",
        type=click.BOOL,
        is_flag=True,
        default=False,
        help=(
            "If True, generate a new default config, and import the file into this "
            "config. If False, modify the currently loaded config."
        ),
    )
    @click.pass_context
    def import_from_file(ctx, file_path, rename, new):
        """Update the config file with keys from a YAML file."""
        ctx.obj["config"].import_from_file(file_path, rename=rename, make_new=new)

    @config.command()
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
        "--metadata",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        help="Show all metadata items.",
        callback=show_all_metadata,
    )
    @click.option(
        "--file",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        help="Show the contents of the configuration file.",
        callback=show_config_file,
    )
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def get(ctx, name):
        """Show the value of the specified configuration item."""
        val = ctx.obj["config"].get(name)
        if isinstance(val, list):
            val = "\n".join(str(i) for i in val)
        click.echo(val)

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @click.option(
        "--json",
        "is_json",
        is_flag=True,
        default=False,
        help="Interpret VALUE as a JSON string.",
    )
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def set(ctx, name, value, is_json):
        """Set and save the value of the specified configuration item."""
        ctx.obj["config"].set(name, value, is_json)
        ctx.obj["config"].save()

    @config.command()
    @click.argument("name")
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def unset(ctx, name):
        """Unset and save the value of the specified configuration item."""
        ctx.obj["config"].unset(name)
        ctx.obj["config"].save()

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @click.option(
        "--json",
        "is_json",
        is_flag=True,
        default=False,
        help="Interpret VALUE as a JSON string.",
    )
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def append(ctx, name, value, is_json):
        """Append a new value to the specified configuration item.

        NAME is the dot-delimited path to the list to be appended to.

        """
        ctx.obj["config"].append(name, value, is_json)
        ctx.obj["config"].save()

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @click.option(
        "--json",
        "is_json",
        is_flag=True,
        default=False,
        help="Interpret VALUE as a JSON string.",
    )
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def prepend(ctx, name, value, is_json):
        """Prepend a new value to the specified configuration item.

        NAME is the dot-delimited path to the list to be prepended to.

        """
        ctx.obj["config"].prepend(name, value, is_json)
        ctx.obj["config"].save()

    @config.command(context_settings={"ignore_unknown_options": True})
    @click.argument("name")
    @click.argument("index", type=click.types.INT)
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def pop(ctx, name, index):
        """Remove a value from a list-like configuration item.

        NAME is the dot-delimited path to the list to be modified.

        """
        ctx.obj["config"].pop(name, index)
        ctx.obj["config"].save()

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @click.option(
        "--json",
        "is_json",
        is_flag=True,
        default=False,
        help="Interpret VALUE as a JSON string.",
    )
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def update(ctx, name, value, is_json):
        """Update a map-like value in the configuration.

        NAME is the dot-delimited path to the map to be updated.

        """
        ctx.obj["config"].update(name, value, is_json)
        ctx.obj["config"].save()

    @config.command()
    @click.argument("name")
    @click.option("--defaults")
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def add_scheduler(ctx, name, defaults):
        if defaults:
            defaults = json.loads(defaults)
        else:
            defaults = {}
        ctx.obj["config"].add_scheduler(name, **defaults)
        ctx.obj["config"].save()

    @config.command()
    @click.argument("name")
    @click.option("--defaults")
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def add_shell(ctx, name, defaults):
        if defaults:
            defaults = json.loads(defaults)
        else:
            defaults = {}
        ctx.obj["config"].add_shell(name, **defaults)
        ctx.obj["config"].save()

    @config.command()
    @click.option("--defaults")
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def add_shell_wsl(ctx, defaults):
        if defaults:
            defaults = json.loads(defaults)
        else:
            defaults = {}
        ctx.obj["config"].add_shell_WSL(**defaults)
        ctx.obj["config"].save()

    @config.command()
    @click.argument("sha")
    @click.pass_context
    @CLI_exception_wrapper_gen(ConfigError)
    def set_github_demo_data_dir(ctx, sha):
        ctx.obj["config"].set_github_demo_data_dir(sha=sha)
        ctx.obj["config"].save()

    @config.command()
    def load_data_files():
        """Check we can load the data files (e.g. task schema files) as specified in the
        configuration."""
        app.load_data_files()

    @config.command()
    @click.option("--path", is_flag=True, default=False)
    @click.pass_context
    def open(ctx, path=False):
        """Alias for `{package_name} open config`: open the configuration file, or retrieve
        it's path."""
        file_path = ctx.obj["config"].get("config_file_path")
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @config.command()
    @click.argument("known_name")
    @click.option(
        "--path",
        default=None,
        help=(
            "An `fsspec`-compatible path in which to look for configuration-import "
            "files."
        ),
    )
    @click.pass_context
    def init(ctx, known_name, path):
        ctx.obj["config"].init(known_name=known_name, path=path)

    open.help = open.help.format(package_name=app.package_name)

    return config
