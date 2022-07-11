"""API functions, which are dynamically added to the BaseApp class on __init__"""

# from hpcflow.core.task import Task
# from hpcflow.core.task_schema import TaskSchema
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
