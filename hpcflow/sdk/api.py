"""API functions, which are dynamically added to the BaseApp class on __init__"""
from __future__ import annotations

import importlib
import os
from typing import Optional, Union
from hpcflow.sdk.core.workflow import ALL_TEMPLATE_FORMATS, DEFAULT_TEMPLATE_FORMAT
from hpcflow.sdk.persistence import DEFAULT_STORE_FORMAT

import hpcflow.sdk.scripting
from hpcflow.sdk.submission.shells import get_shell
from hpcflow.sdk.submission.shells.os_version import (
    get_OS_info_POSIX,
    get_OS_info_windows,
)
from hpcflow.sdk.typing import PathLike

__all__ = (
    "make_workflow",
    "make_and_submit_workflow",
    "submit_workflow",
    "run_hpcflow_tests",
    "run_tests",
    "get_OS_info",
    "get_shell_info",
)


def make_workflow(
    app: App,
    template_file_or_str: Union[PathLike, str],
    is_string: Optional[bool] = False,
    template_format: Optional[str] = DEFAULT_TEMPLATE_FORMAT,
    path: Optional[PathLike] = None,
    name: Optional[str] = None,
    overwrite: Optional[bool] = False,
    store: Optional[str] = DEFAULT_STORE_FORMAT,
    ts_fmt: Optional[str] = None,
    ts_name_fmt: Optional[str] = None,
) -> Workflow:
    """Generate a new {app_name} workflow from a file or string containing a workflow
    template parametrisation.

    Parameters
    ----------

    template_path_or_str
        Either a path to a template file in YAML or JSON format, or a YAML/JSON string.
    is_string
        Determines if passing a file path or a string.
    template_format
        If specified, one of "json" or "yaml". This forces parsing from a particular
        format.
    path
        The directory in which the workflow will be generated. The current directory
        if not specified.
    name
        The name of the workflow. If specified, the workflow directory will be `path`
        joined with `name`. If not specified the workflow template name will be used,
        in combination with a date-timestamp.
    overwrite
        If True and the workflow directory (`path` + `name`) already exists, the
        existing directory will be overwritten.
    store
        The persistent store type to use.
    ts_fmt
        The datetime format to use for storing datetimes. Datetimes are always stored
        in UTC (because Numpy does not store time zone info), so this should not
        include a time zone name.
    ts_name_fmt
        The datetime format to use when generating the workflow name, where it
        includes a timestamp.
    """

    app.API_logger.info("make_workflow called")

    common = {
        "path": path,
        "name": name,
        "overwrite": overwrite,
        "store": store,
        "ts_fmt": ts_fmt,
        "ts_name_fmt": ts_name_fmt,
    }

    if not is_string:
        wk = app.Workflow.from_file(
            template_path=template_file_or_str, template_format=template_format, **common
        )

    elif template_format == "json":
        wk = app.Workflow.from_JSON_string(JSON_str=template_file_or_str, **common)

    elif template_format == "yaml":
        wk = app.Workflow.from_YAML_string(YAML_str=template_file_or_str, **common)

    else:
        raise ValueError(
            f"Template format {template_format} not understood. Available template "
            f"formats are {ALL_TEMPLATE_FORMATS!r}."
        )
    return wk


def make_and_submit_workflow(
    app: App,
    template_file_or_str: Union[PathLike, str],
    is_string: Optional[bool] = False,
    template_format: Optional[str] = DEFAULT_TEMPLATE_FORMAT,
    path: Optional[PathLike] = None,
    name: Optional[str] = None,
    overwrite: Optional[bool] = False,
    store: Optional[str] = DEFAULT_STORE_FORMAT,
    ts_fmt: Optional[str] = None,
    ts_name_fmt: Optional[str] = None,
    JS_parallelism: Optional[bool] = None,
):
    """Generate and submit a new {app_name} workflow from a file or string containing a
    workflow template parametrisation.

    Parameters
    ----------

    template_path_or_str
        Either a path to a template file in YAML or JSON format, or a YAML/JSON string.
    is_string
        Determines whether `template_path_or_str` is a string or a file.
    template_format
        If specified, one of "json" or "yaml". This forces parsing from a particular
        format.
    path
        The directory in which the workflow will be generated. The current directory
        if not specified.
    name
        The name of the workflow. If specified, the workflow directory will be `path`
        joined with `name`. If not specified the `WorkflowTemplate` name will be used,
        in combination with a date-timestamp.
    overwrite
        If True and the workflow directory (`path` + `name`) already exists, the
        existing directory will be overwritten.
    store
        The persistent store to use for this workflow.
    ts_fmt
        The datetime format to use for storing datetimes. Datetimes are always stored
        in UTC (because Numpy does not store time zone info), so this should not
        include a time zone name.
    ts_name_fmt
        The datetime format to use when generating the workflow name, where it
        includes a timestamp.
    JS_parallelism
        If True, allow multiple jobscripts to execute simultaneously. Raises if set to
        True but the store type does not support the `jobscript_parallelism` feature. If
        not set, jobscript parallelism will be used if the store type supports it.
    """

    app.API_logger.info("make_and_submit_workflow called")

    wk = app.make_workflow(
        template_file_or_str=template_file_or_str,
        is_string=is_string,
        template_format=template_format,
        path=path,
        name=name,
        overwrite=overwrite,
        store=store,
        ts_fmt=ts_fmt,
        ts_name_fmt=ts_name_fmt,
    )
    return wk.submit(JS_parallelism=JS_parallelism)


def submit_workflow(
    app: App,
    workflow_path: PathLike,
    JS_parallelism: Optional[bool] = None,
):
    """Submit an existing {app_name} workflow.

    Parameters
    ----------
    workflow_path
        Path to an existing workflow
    JS_parallelism
        If True, allow multiple jobscripts to execute simultaneously. Raises if set to
        True but the store type does not support the `jobscript_parallelism` feature. If
        not set, jobscript parallelism will be used if the store type supports it.
    """

    app.API_logger.info("submit_workflow called")
    wk = app.Workflow(workflow_path)
    return wk.submit(JS_parallelism=JS_parallelism)


def run_hpcflow_tests(app, *args):
    """Run hpcflow test suite. This function is only available from derived apps.

    Notes
    -----
    It may not be possible to run hpcflow tests after/before running tests of the derived
    app within the same process, due to caching."""

    from hpcflow.api import hpcflow

    return hpcflow.run_tests(*args)


def run_tests(app, *args):
    """Run {app_name} test suite."""

    try:
        import pytest
    except ModuleNotFoundError:
        raise RuntimeError(f"{app.name} has not been built with testing dependencies.")

    test_args = (app.pytest_args or []) + list(args)
    if app.run_time_info.is_frozen:
        with importlib.resources.path(app.name, "tests") as test_dir:
            return pytest.main([str(test_dir)] + test_args)
    else:
        return pytest.main(["--pyargs", f"{app.name}"] + test_args)


def get_OS_info(app):
    """Get information about the operating system."""
    os_name = os.name
    if os_name == "posix":
        return get_OS_info_POSIX(linux_release_file=app.config.get("linux_release_file"))
    elif os_name == "nt":
        return get_OS_info_windows()


def get_shell_info(
    app,
    shell_name: str,
    exclude_os: Optional[bool] = False,
):
    """Get information about a given shell and the operating system.

    Parameters
    ----------
    shell_name
        One of the supported shell names.
    exclude_os
        If True, exclude operating system information.
    """
    shell = get_shell(
        shell_name=shell_name,
        os_args={"linux_release_file": app.config.linux_release_file},
    )
    return shell.get_version_info(exclude_os)
