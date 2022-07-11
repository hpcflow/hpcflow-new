"""Module defining a function that returns the click CLI group for manipulating the app
configuration."""

import logging
import warnings
from functools import wraps
from contextlib import contextmanager

import click
from colorama import init as colorama_init
from termcolor import colored

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
        print(app.config.to_string(exclude=["config_file_contents"]))
        ctx.exit()

    def show_config_file(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        print(app.config.config_file_contents)
        ctx.exit()

    @click.group()
    @click.option("--invocation", default="_")
    def config(invocation):
        """Configuration sub-command for getting and setting data in the configuration
        file(s)."""

    @config.command("list")
    def config_list():
        """Show a list of all configurable keys."""
        click.echo("\n".join(app.config.get_configurable()))

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
        "--file",
        is_flag=True,
        expose_value=False,
        is_eager=True,
        help="Show the contents of the configuration file.",
        callback=show_config_file,
    )
    @CLI_exception_wrapper_gen(ConfigError)
    def get(name):
        """Show the value of the specified configuration item."""
        val = app.config.get(name)
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
    @CLI_exception_wrapper_gen(ConfigError)
    def set(name, value, is_json):
        """Set and save the value of the specified configuration item."""
        app.config.set(name, value, is_json)
        app.config.save()

    @config.command()
    @click.argument("name")
    @CLI_exception_wrapper_gen(ConfigError)
    def unset(name):
        """Unset and save the value of the specified configuration item."""
        app.config.unset(name)
        app.config.save()

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @CLI_exception_wrapper_gen(ConfigError)
    def append(name, value):
        """Append a new value to the specified configuration item."""
        app.config.append(name, value)
        app.config.save()

    @config.command()
    @click.argument("name")
    @click.argument("value")
    @CLI_exception_wrapper_gen(ConfigError)
    def prepend(name, value):
        """Append a new value to the specified configuration item."""
        app.config.prepend(name, value)
        app.config.save()

    @config.command(context_settings={"ignore_unknown_options": True})
    @click.argument("name")
    @click.argument("index", type=click.types.INT)
    @CLI_exception_wrapper_gen(ConfigError)
    def pop(name, index):
        """Remove a value from a list-like configuration item."""
        app.config.pop(name, index)
        app.config.save()

    @config.command()
    def load_data_files():
        """Check we can load the data files (e.g. task schema files) as specified in the
        configuration."""
        app.load_data_files()

    return config
