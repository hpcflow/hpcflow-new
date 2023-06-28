import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_schemas


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@pytest.fixture
def workflow_w1(null_config, tmp_path):
    s1, s2 = make_schemas(
        [
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ]
    )

    t1 = hf.Task(
        schemas=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=1)],
    )
    t2 = hf.Task(schemas=s2, nesting_order={"inputs.p2": 1})

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    return hf.Workflow.from_template(wkt, path=tmp_path)


def test_element_task_dependencies(workflow_w1):
    assert workflow_w1.tasks.t2.elements[0].get_task_dependencies(as_objects=True) == [
        workflow_w1.tasks.t1
    ]


def test_element_dependent_tasks(workflow_w1):
    assert workflow_w1.tasks.t1.elements[0].get_dependent_tasks(as_objects=True) == [
        workflow_w1.tasks.t2
    ]


def test_element_element_dependencies(workflow_w1):
    assert all(
        (
            workflow_w1.tasks.t2.elements[0].get_element_dependencies() == [0],
            workflow_w1.tasks.t2.elements[1].get_element_dependencies() == [1],
        )
    )


def test_element_dependent_elements(workflow_w1):
    assert all(
        (
            workflow_w1.tasks.t1.elements[0].get_dependent_elements() == [2],
            workflow_w1.tasks.t1.elements[1].get_dependent_elements() == [3],
        )
    )
