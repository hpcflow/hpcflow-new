"""API functions, which are dynamically added to the BaseApp class on __init__"""

import importlib
from hpcflow.sdk.core.utils import load_config


@load_config
def make_workflow(app, dir):
    """make a new {name} workflow.

    Parameters
    ----------
    dir
        Directory into which the workflow will be generated.

    Returns
    -------
    nonsense : Workflow

    """
    pass
    app.API_logger.info("hey")


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
