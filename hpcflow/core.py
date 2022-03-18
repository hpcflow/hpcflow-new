import datetime
import enum

import zarr

from hpcflow.utils import make_workflow_id, get_time_stamp
from hpcflow.config import Config


class WorkflowInteraction(enum.Enum):

    CREATE = 0


class Workflow:
    def __init__(self, tasks):
        self.tasks = tasks


class Task:
    def __init__(self, schema, parameter_values):
        self.schema = schema
        self.parameter_values


class TaskSchema:
    def __init__(self, parameters):
        self.parameters = parameters


def make_workflow():

    Config.set_config()

    workflow = Workflow(
        tasks=[
            Task(
                schema=TaskSchema(parameters=("A", "B")),
                parameter_values={"A": 1, "B": 2},
            ),
            Task(schema=TaskSchema(parameters=("D")), parameter_values={"D": 4}),
        ]
    )

    id_ = "jd3ifk"  # make_workflow_id()   TEMP
    store = zarr.DirectoryStore(f"workflow_{id_}.zarr")
    root = zarr.group(store=store, overwrite=True)
    parameter_group = root.create_group("parameters")
    history_group = root.create_group("history")

    ts_str = get_time_stamp()

    make_history = history_group.create_group(ts_str)
    make_history.attrs["timestamp"] = ts_str
    make_history.attrs["interaction"] = WorkflowInteraction.CREATE.name
