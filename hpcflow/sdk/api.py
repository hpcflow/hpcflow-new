"""API functions, which are dynamically added to the BaseApp class on __init__"""
from __future__ import annotations

import importlib
from typing import Optional, Union
from hpcflow.sdk.core.workflow import ALL_TEMPLATE_FORMATS, DEFAULT_TEMPLATE_FORMAT
from hpcflow.sdk.persistence import DEFAULT_STORE_FORMAT

import hpcflow.sdk.scripting
from hpcflow.sdk.typing import PathLike

__all__ = (
    "make_workflow",
    "make_and_submit_workflow",
    "submit_workflow",
    "run_hpcflow_tests",
    "run_tests",
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
    """

    app.API_logger.info("make_workflow called")

    if not is_string:
        wk = app.Workflow.from_file(
            template_file_or_str,
            template_format,
            path,
            name,
            overwrite,
            store,
        )

    elif template_format == "json":
        wk = app.Workflow.from_JSON_string(
            template_file_or_str,
            store,
            path,
            name,
            overwrite,
        )

    elif template_format == "yaml":
        wk = app.Workflow.from_YAML_string(
            template_file_or_str,
            store,
            path,
            name,
            overwrite,
        )

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
    )
    wk.submit()


def submit_workflow(app: App, workflow_path: PathLike):
    """Submit an existing {app_name} workflow.

    Parameters
    ----------
    workflow_path
        Path to an existing workflow
    """

    app.API_logger.info("submit_workflow called")
    wk = app.Workflow(workflow_path)
    return wk.submit()


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
