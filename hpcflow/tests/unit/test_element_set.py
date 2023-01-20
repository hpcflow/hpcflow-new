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


@pytest.fixture
def workflow_w2(workflow_w1):
    """Add another element set to the second task."""
    workflow_w1.tasks.t2.add_elements(nesting_order={"inputs.p2": 1})
    return workflow_w1


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_element_set_task_dependencies(workflow_w2):
    assert all(
        workflow_w2.tasks.t2.template.element_sets[0].task_dependencies
        == [workflow_w2.tasks.t1],
        workflow_w2.tasks.t2.template.element_sets[1].task_dependencies
        == [workflow_w2.tasks.t1],
    )


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_element_set_dependent_tasks(workflow_w2):
    assert workflow_w2.tasks.t1.template.element_sets[0].dependent_tasks == [
        workflow_w2.tasks.t2
    ]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_element_set_element_dependencies(workflow_w2):
    assert all(
        (
            workflow_w2.tasks.t2.template.element_sets[0].element_dependencies == [0, 1],
            workflow_w2.tasks.t2.template.element_sets[1].element_dependencies == [0, 1],
        )
    )


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_element_set_dependent_elements(workflow_w2):
    assert workflow_w2.tasks.t1.template.element_sets[0].dependent_elements == [
        2,
        3,
        4,
        5,
    ]
