"""API functions, which are dynamically added to the BaseApp class on __init__"""

import importlib

import hpcflow.sdk.scripting
from hpcflow.sdk.core.utils import load_config


@load_config
def make_workflow(app, template_file, dir):
    """Generate a new {name} workflow.

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
    """Generate and submit a new {name} workflow.

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


def run_hpcflow_tests(app, *args):
    """Run hpcflow test suite. This function is only available from derived apps.

    Notes
    -----
    It may not be possible to run hpcflow tests after/before running tests of the derived
    app within the same process, due to caching."""

    from hpcflow.api import hpcflow

    hpcflow.run_tests(*args)


def run_tests(app, *args):
    """Run {name} test suite."""

    try:
        import pytest
    except ModuleNotFoundError:
        raise RuntimeError(f"{app.name} has not been built with testing dependencies.")

    test_args = (app.pytest_args or []) + list(args)
    if app.run_time_info.is_frozen:
        with importlib.resources.path(app.name, "tests") as test_dir:
            pytest.main([str(test_dir)] + test_args)
    else:
        pytest.main(["--pyargs", f"{app.name}"] + test_args)
