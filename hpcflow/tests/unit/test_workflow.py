import pytest

from hpcflow.api import (
    InputValue,
    Parameter,
    Task,
    TaskSchema,
    hpcflow,
    WorkflowTemplate,
    Workflow,
)
from hpcflow.sdk.core.errors import MissingInputs, WorkflowNotFoundError


@pytest.fixture
def null_config(tmp_path):
    hpcflow.load_config(config_dir=tmp_path)


@pytest.fixture
def empty_workflow(null_config, tmp_path):
    return Workflow.from_template(WorkflowTemplate(name="w1"), path=tmp_path)


@pytest.fixture
def param_p1():
    return Parameter("p1")


@pytest.fixture
def param_p2():
    return Parameter("p2")


@pytest.fixture
def param_p3():
    return Parameter("p3")


@pytest.fixture
def workflow_w1(null_config, tmp_path, param_p1, param_p2):
    s1 = TaskSchema("ts1", actions=[], inputs=[param_p1], outputs=[param_p2])
    t1 = Task(schemas=s1, inputs=[InputValue(param_p1, 101)])
    wkt = WorkflowTemplate(name="w1", tasks=[t1])
    return Workflow.from_template(wkt, path=tmp_path)


def test_make_empty_workflow(empty_workflow):
    assert empty_workflow.path is not None


def test_raise_on_missing_workflow(tmp_path):
    with pytest.raises(WorkflowNotFoundError):
        Workflow(tmp_path)


def test_raise_on_missing_inputs_add_first_task(empty_workflow, param_p1):
    s1 = TaskSchema("ts1", actions=[], inputs=[param_p1])
    t1 = Task(schemas=s1)
    with pytest.raises(MissingInputs) as exc_info:
        empty_workflow.add_task(t1)

    assert exc_info.value.missing_inputs == [param_p1.typ]


def test_raise_on_missing_inputs_add_second_task(workflow_w1, param_p2, param_p3):
    s2 = TaskSchema("ts2", actions=[], inputs=[param_p2, param_p3])
    t2 = Task(schemas=s2)
    with pytest.raises(MissingInputs) as exc_info:
        workflow_w1.add_task(t2)

    assert exc_info.value.missing_inputs == [param_p3.typ]  # p2 comes from existing task
