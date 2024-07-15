"""Module that defines built-in callback functions for configuration item values."""

from __future__ import annotations
import os
import re
import fsspec  # type: ignore
from typing import overload, TYPE_CHECKING
from hpcflow.sdk.core.errors import UnsupportedSchedulerError, UnsupportedShellError

from hpcflow.sdk.submission.shells import get_supported_shells

if TYPE_CHECKING:
    from typing import Any, TypeVar
    from .config import Config
    from ..typing import PathLike
    from ..submission.schedulers import QueuedScheduler

    T = TypeVar("T")


def callback_vars(config: Config, value) -> str:
    """Substitute configuration variables."""

    def vars_repl(match_obj: re.Match[str]) -> str:
        var_name = match_obj.groups()[0]
        return config._variables[var_name]

    vars_join = "|".join(config._variables.keys())
    vars_regex = r"\<\<(" + vars_join + r")\>\>"
    value = re.sub(
        pattern=vars_regex,
        repl=vars_repl,
        string=str(value),
    )
    return value


@overload
def callback_file_paths(config: Config, file_path: PathLike) -> PathLike:
    ...


@overload
def callback_file_paths(config: Config, file_path: list[PathLike]) -> list[PathLike]:
    ...


def callback_file_paths(config: Config, file_path: PathLike | list[PathLike]):
    if isinstance(file_path, list):
        return [config._resolve_path(i) for i in file_path]
    else:
        return config._resolve_path(file_path)


def callback_bool(config: Config, value: str | bool) -> bool:
    if not isinstance(value, bool):
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            raise TypeError(f"Cannot cast {value!r} to a bool type.")
    return value


@overload
def callback_lowercase(config: Config, value: list[str]) -> list[str]:
    ...


@overload
def callback_lowercase(config: Config, value: dict[str, T]) -> dict[str, T]:
    ...


@overload
def callback_lowercase(config: Config, value: str) -> str:
    ...


def callback_lowercase(
    config: Config, value: list[str] | dict[str, T] | str
) -> list[str] | dict[str, T] | str:
    if isinstance(value, list):
        return [i.lower() for i in value]
    elif isinstance(value, dict):
        return {k.lower(): v for k, v in value.items()}
    else:
        return value.lower()


def exists_in_schedulers(config: Config, value: T) -> T:
    if value not in config.schedulers:
        raise ValueError(
            f"Cannot set default scheduler; {value!r} is not a supported scheduler "
            f"according to the config file, which lists these schedulers as available "
            f"on this machine: {config.schedulers!r}."
        )
    return value


def callback_supported_schedulers(
    config: Config, schedulers: dict[str, Any]
) -> dict[str, Any]:
    # validate against supported schedulers according to the OS - this won't validate that
    # a particular scheduler actually exists on this system:
    available = config._app.get_OS_supported_schedulers()
    for k in schedulers:
        if k not in available:
            raise UnsupportedSchedulerError(scheduler=k, available=available)

    return schedulers


def set_scheduler_invocation_match(config: Config, scheduler: str) -> None:
    """Invoked on set of `default_scheduler`.

    For clusters with "proper" schedulers (SGE, SLURM, etc.), login nodes are typically
    named using the word "login". So we can use this knowledge to set a default for the
    "hostname" invocation match key, if it is not manually set. However, it is preferable
    that on clusters the hostname match is explicitly set.

    """
    default_args = config.get(f"schedulers.{scheduler}").get("defaults", {})
    sched = config._app.get_scheduler(
        scheduler_name=scheduler,
        os_name=os.name,
        scheduler_args=default_args,
    )
    if isinstance(sched, config._app.QueuedScheduler):
        if "hostname" not in config._file.get_invocation(config._config_key)["match"]:
            config._file.update_invocation(
                config_key=config._config_key,
                match={"hostname": sched.DEFAULT_LOGIN_NODE_MATCH},
            )


def callback_scheduler_set_up(
    config: Config, schedulers: dict[str, Any]
) -> dict[str, Any]:
    """Invoked on set of `schedulers`.

    Runs scheduler-specific config initialisation.

    """
    for k, v in schedulers.items():
        sched = config._app.get_scheduler(
            scheduler_name=k,
            os_name=os.name,
            scheduler_args=v.get("defaults", {}),
        )

        if isinstance(sched, config._app.SGEPosix):
            # some `QueuedScheduler` classes have a `get_login_nodes` method which can be used
            # to populate the names of login nodes explicitly, if not already set:
            if "hostname" not in config._file.get_invocation(config._config_key)["match"]:
                login_nodes = sched.get_login_nodes()
                config._file.update_invocation(
                    config_key=config._config_key,
                    match={"hostname": login_nodes},
                )
    return schedulers


def callback_supported_shells(config: Config, shell_name: str) -> str:
    supported = get_supported_shells(os.name)
    if shell_name not in supported:
        raise UnsupportedShellError(shell=shell_name, supported=supported)
    return shell_name


def set_callback_file_paths(config: Config, value: PathLike | list[PathLike]) -> None:
    """Check the file(s) is/are accessible. This is only done on `config.set` (and not on
    `config.get` or `config._validate`) because it could be expensive in the case of remote
    files."""
    value = callback_file_paths(config, value)

    to_check = value if isinstance(value, list) else [value]

    for file_path in to_check:
        if file_path is None:
            continue
        with fsspec.open(file_path, mode="rt") as fh:
            pass
            # TODO: also check something in it?
        print(f"Checked access to: {file_path}")


def check_load_data_files(config: Config, value: Any) -> None:
    """Check data files (e.g. task schema files) can be loaded successfully. This is only
    done on `config.set` (and not on `config.get` or `config._validate`) because it could
    be expensive in the case of remote files."""
    config._app.reload_template_components(warn=False)


def callback_update_log_console_level(config: Config, value: str) -> None:
    config._app.log.update_console_level(value)
