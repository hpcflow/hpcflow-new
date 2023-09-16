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


def test_equivalence_single_labelled_schema_input_element_get_label_and_non_label(
    new_null_config, tmp_path
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"), labels={"one": {}})],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command="Write-Output (<<parameter:p1[one]>> + 100)")
                ]
            )
        ],
    )
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1", label="one", value=101)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    assert wk.tasks.t1.elements[0].get("inputs.p1") == wk.tasks.t1.elements[0].get(
        "inputs.p1[one]"
    )


def test_element_dependencies_inputs_only_schema(new_null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<parameter:p2>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
    )
    tasks = [
        hf.Task(
            schemas=s1,
            inputs=[hf.InputValue("p1", value=101)],
        ),
        hf.Task(schemas=s2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    assert wk.tasks.t1.elements[0].get_dependent_elements() == [1]
    assert wk.tasks.t2.elements[0].get_element_dependencies() == [0]
    assert wk.tasks.t2.elements[0].get_EAR_dependencies() == [0]


def test_element_get_empty_path_single_labelled_input(null_config, tmp_path):
    p1_val = 101
    label = "my_label"
    s1 = hf.TaskSchema(
        objective="t1", inputs=[hf.SchemaInput(parameter="p1", labels={label: {}})]
    )
    t1 = hf.Task(schemas=[s1], inputs=[hf.InputValue("p1", p1_val, label=label)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
    )
    assert wk.tasks[0].elements[0].get() == {
        "resources": {"any": {}},
        "inputs": {"p1": p1_val},
    }


def test_element_get_labelled_non_labelled_equivalence(null_config, tmp_path):
    p1_val = 101
    label = "my_label"
    s1 = hf.TaskSchema(
        objective="t1", inputs=[hf.SchemaInput(parameter="p1", labels={label: {}})]
    )
    t1 = hf.Task(schemas=[s1], inputs=[hf.InputValue("p1", p1_val, label=label)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
    )
    assert wk.tasks[0].elements[0].get("inputs.p1") == wk.tasks[0].elements[0].get(
        f"inputs.p1[{label}]"
    )
