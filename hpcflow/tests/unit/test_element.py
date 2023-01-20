import pytest
from hpcflow.api import (
    ValueSequence,
    hpcflow,
    Parameter,
    TaskSchema,
    Task,
    WorkflowTemplate,
    Workflow,
)


@pytest.fixture
def null_config(tmp_path):
    hpcflow.load_config(config_dir=tmp_path)


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
    s1 = TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p2])
    s2 = TaskSchema("t2", actions=[], inputs=[param_p2])

    t1 = Task(
        schemas=s1,
        sequences=[ValueSequence("inputs.p1", values=[101, 102], nesting_order=1)],
    )
    t2 = Task(schemas=s2, nesting_order={"inputs.p2": 1})

    wkt = WorkflowTemplate(name="w1", tasks=[t1, t2])
    return Workflow.from_template(wkt, path=tmp_path)


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_element_task_dependencies(workflow_w1):
    assert workflow_w1.tasks.t2.elements[0].task_dependencies == [workflow_w1.tasks.t1]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_element_dependent_tasks(workflow_w1):
    assert workflow_w1.tasks.t1.elements[0].dependent_tasks == [workflow_w1.tasks.t2]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_element_element_dependencies(workflow_w1):
    assert all(
        (
            workflow_w1.tasks.t2.elements[0].element_dependencies == [0],
            workflow_w1.tasks.t2.elements[1].element_dependencies == [1],
        )
    )


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_element_dependent_elements(workflow_w1):
    assert all(
        (
            workflow_w1.tasks.t1.elements[0].dependent_elements == [2],
            workflow_w1.tasks.t1.elements[1].dependent_elements == [3],
        )
    )
