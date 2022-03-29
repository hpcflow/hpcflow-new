from contextlib import contextmanager
from functools import wraps
import warnings
from colorama import init as colorama_init
from termcolor import colored
import click

colorama_init(autoreset=True)


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
