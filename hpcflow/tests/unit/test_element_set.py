import pytest
from hpcflow.app import app as hf


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@pytest.fixture
def param_p1():
    return hf.Parameter("p1")


@pytest.fixture
def param_p2():
    return hf.Parameter("p2")


@pytest.fixture
def param_p3():
    return hf.Parameter("p3")


@pytest.fixture
def workflow_w1(null_config, tmp_path, param_p1, param_p2):
    s1 = hf.TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p2])
    s2 = hf.TaskSchema("t2", actions=[], inputs=[param_p2])

    t1 = hf.Task(
        schemas=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=1)],
    )
    t2 = hf.Task(schemas=s2, nesting_order={"inputs.p2": 1})

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    return hf.Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def workflow_w2(workflow_w1):
    """Add another element set to the second task."""
    workflow_w1.tasks.t2.add_elements(nesting_order={"inputs.p2": 1})
    return workflow_w1
