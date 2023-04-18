"""API functions, which are dynamically added to the BaseApp class on __init__"""
from __future__ import annotations

import importlib

import hpcflow.sdk.scripting
from hpcflow.sdk.core.utils import load_config


@load_config
def make_workflow(app, template_file, dir):
    """Generate a new {app_name} workflow.

    Parameters
    ----------
    template_file:
        Path to YAML file workflow template.
    dir:
        Directory into which the workflow will be generated.

    Returns
    -------
    Workflow

    """
    app.API_logger.info("make workflow")
    wkt = app.WorkflowTemplate.from_YAML_file(template_file)
    wk = app.Workflow.from_template(wkt, path=dir)
    return wk


@load_config
def submit_workflow(app, template_file, dir):
    """Generate and submit a new {app_name} workflow.

    Parameters
    ----------
    template_file:
        Path to YAML file workflow template.
    dir:
        Directory into which the workflow will be generated.

    Returns
    -------
    Workflow
    """
    app.API_logger.info("submit workflow")
    wk = app.make_workflow(template_file, dir)
    wk.submit()
    return wk


def set_EAR_start(
    app,
    workflow: Workflow,
    task_insert_id: int,
    element_iteration_idx: int,
    action_idx: int,
    run_idx: int,
) -> None:
    """Set the start time of an EAR."""
    workflow.set_EAR_start(
        task_insert_id,
        element_iteration_idx,
        action_idx,
        run_idx,
    )


def set_EAR_end(
    app,
    workflow: Workflow,
    task_insert_id: int,
    element_iteration_idx: int,
    action_idx: int,
    run_idx: int,
) -> None:
    """Set the end time of an EAR."""
    workflow.set_EAR_end(
        task_insert_id,
        element_iteration_idx,
        action_idx,
        run_idx,
    )


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
