import numpy as np
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import MissingInputs
from hpcflow.sdk.core.test_utils import P1_parameter_cls as P1


def test_input_source_class_method_local():
    assert hf.InputSource.local() == hf.InputSource(hf.InputSourceType.LOCAL)


def test_input_source_class_method_default():
    assert hf.InputSource.default() == hf.InputSource(hf.InputSourceType.DEFAULT)


def test_input_source_class_method_task():
    task_ref = 0
    assert hf.InputSource.task(task_ref) == hf.InputSource(
        source_type=hf.InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_class_method_import():
    import_ref = (
        0  # TODO: interface to imports (and so how to reference) is not yet decided
    )
    assert hf.InputSource.import_(import_ref) == hf.InputSource(
        hf.InputSourceType.IMPORT, import_ref=import_ref
    )


def test_input_source_class_method_task_same_default_task_source_type():
    task_ref = 0
    assert (
        hf.InputSource(hf.InputSourceType.TASK, task_ref=task_ref).task_source_type
        == hf.InputSource.task(task_ref=task_ref).task_source_type
    )


def test_input_source_validate_source_type_string_local():
    assert hf.InputSource("local") == hf.InputSource(hf.InputSourceType.LOCAL)


def test_input_source_validate_source_type_string_default():
    assert hf.InputSource("default") == hf.InputSource(hf.InputSourceType.DEFAULT)


def test_input_source_validate_source_type_string_task():
    task_ref = 0
    assert hf.InputSource("task", task_ref=task_ref) == hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_validate_source_type_string_import():
    import_ref = (
        0  # TODO: interface to imports (and so how to reference) is not yet decided
    )
    assert hf.InputSource("import", import_ref=import_ref) == hf.InputSource(
        hf.InputSourceType.IMPORT, import_ref=import_ref
    )


def test_input_source_validate_source_type_raise_on_unknown_string():
    with pytest.raises(ValueError):
        hf.InputSource("bad_source_type")


def test_input_source_validate_task_source_type_string_any():
    task_ref = 0
    assert hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref, task_source_type="any"
    ) == hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref, task_source_type=hf.TaskSourceType.ANY
    )


def test_input_source_validate_task_source_type_string_input():
    task_ref = 0
    assert hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref, task_source_type="input"
    ) == hf.InputSource(
        hf.InputSourceType.TASK,
        task_ref=task_ref,
        task_source_type=hf.TaskSourceType.INPUT,
    )


def test_input_source_validate_task_source_type_string_output():
    task_ref = 0
    assert hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref, task_source_type="output"
    ) == hf.InputSource(
        hf.InputSourceType.TASK,
        task_ref=task_ref,
        task_source_type=hf.TaskSourceType.OUTPUT,
    )


def test_input_source_validate_task_source_type_raise_on_unknown_string():
    task_ref = 0
    with pytest.raises(ValueError):
        hf.InputSource(
            hf.InputSourceType.TASK,
            task_ref=task_ref,
            task_source_type="bad_task_source_type",
        )


def test_input_source_to_string_local():
    assert hf.InputSource.local().to_string() == "local"


def test_input_source_to_string_default():
    assert hf.InputSource.default().to_string() == "default"


def test_input_source_to_string_task_output():
    task_ref = 0
    assert (
        hf.InputSource.task(task_ref, task_source_type="output").to_string()
        == f"task.{task_ref}.output"
    )


def test_input_source_to_string_task_input():
    task_ref = 0
    assert (
        hf.InputSource.task(task_ref, task_source_type="input").to_string()
        == f"task.{task_ref}.input"
    )


def test_input_source_to_string_task_any():
    task_ref = 0
    assert (
        hf.InputSource.task(task_ref, task_source_type="any").to_string()
        == f"task.{task_ref}.any"
    )


def test_input_source_to_string_import():
    import_ref = 0
    assert hf.InputSource.import_(import_ref).to_string() == f"import.{import_ref}"


def test_input_source_from_string_local():
    assert hf.InputSource.from_string("local") == hf.InputSource(hf.InputSourceType.LOCAL)


def test_input_source_from_string_default():
    assert hf.InputSource.from_string("default") == hf.InputSource(
        hf.InputSourceType.DEFAULT
    )


def test_input_source_from_string_task():
    assert hf.InputSource.from_string("task.0.output") == hf.InputSource(
        hf.InputSourceType.TASK, task_ref=0, task_source_type=hf.TaskSourceType.OUTPUT
    )


def test_input_source_from_string_task_same_default_task_source():
    task_ref = 0
    assert hf.InputSource.from_string(f"task.{task_ref}") == hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_from_string_import():
    import_ref = 0
    assert hf.InputSource.from_string(f"import.{import_ref}") == hf.InputSource(
        hf.InputSourceType.IMPORT, import_ref=import_ref
    )


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
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_specified_sourceable_elements_subset(
    null_config, param_p1, param_p2, param_p3, tmp_path
):
    param_p1 = hf.SchemaInput(param_p1, default_value=1001)
    param_p2 = hf.SchemaInput(param_p2, default_value=np.array([2002, 2003]))
    param_p3 = hf.SchemaInput(param_p3)

    s1 = hf.TaskSchema("ts1", actions=[], inputs=[param_p1], outputs=[param_p3])
    s2 = hf.TaskSchema("ts2", actions=[], inputs=[param_p2, param_p3])

    t1 = hf.Task(
        schemas=s1,
        sequences=[
            hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=0),
        ],
    )
    t2 = hf.Task(
        schemas=s2,
        inputs=[hf.InputValue(param_p2, 201)],
        sourceable_elements=[0],
        nesting_order={"inputs.p3": 1},
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert (
        wk.tasks[1].num_elements == 1
        and wk.tasks[1].elements[0].input_sources["inputs.p3"] == "element.0.OUTPUT"
    )


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_specified_sourceable_elements_all_available(
    null_config, param_p1, param_p2, param_p3, tmp_path
):
    param_p1 = hf.SchemaInput(param_p1, default_value=1001)
    param_p2 = hf.SchemaInput(param_p2, default_value=np.array([2002, 2003]))
    param_p3 = hf.SchemaInput(param_p3)

    s1 = hf.TaskSchema("ts1", actions=[], inputs=[param_p1], outputs=[param_p3])
    s2 = hf.TaskSchema("ts2", actions=[], inputs=[param_p2, param_p3])

    t1 = hf.Task(
        schemas=s1,
        sequences=[
            hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=0),
        ],
    )
    t2 = hf.Task(
        schemas=s2,
        inputs=[hf.InputValue(param_p2, 201)],
        sourceable_elements=[0, 1],
        nesting_order={"inputs.p3": 1},
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert (
        wk.tasks[1].num_elements == 2
        and wk.tasks[1].elements[0].input_sources["inputs.p3"] == "element.0.OUTPUT"
        and wk.tasks[1].elements[1].input_sources["inputs.p3"] == "element.1.OUTPUT"
    )


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_no_sourceable_elements_so_raise_missing(
    null_config, param_p1, param_p2, param_p3, tmp_path
):
    param_p1 = hf.SchemaInput(param_p1, default_value=1001)
    param_p2 = hf.SchemaInput(param_p2, default_value=np.array([2002, 2003]))
    param_p3 = hf.SchemaInput(param_p3)

    s1 = hf.TaskSchema("ts1", actions=[], inputs=[param_p1], outputs=[param_p3])
    s2 = hf.TaskSchema("ts2", actions=[], inputs=[param_p2, param_p3])

    t1 = hf.Task(schemas=s1, inputs=[hf.InputValue(param_p1, 101)])
    t2 = hf.Task(
        schemas=s2,
        inputs=[hf.InputValue(param_p2, 201)],
        sourceable_elements=[],
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])

    with pytest.raises(MissingInputs):
        _ = hf.Workflow.from_template(wkt, path=tmp_path)


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_no_sourceable_elements_so_default_used(
    null_config, param_p1, param_p2, param_p3, tmp_path
):
    param_p1 = hf.SchemaInput(param_p1, default_value=1001)
    param_p2 = hf.SchemaInput(param_p2, default_value=np.array([2002, 2003]))
    param_p3 = hf.SchemaInput(param_p3, default_value=3001)

    s1 = hf.TaskSchema("ts1", actions=[], inputs=[param_p1], outputs=[param_p3])
    s2 = hf.TaskSchema("ts2", actions=[], inputs=[param_p2, param_p3])

    t1 = hf.Task(schemas=s1, inputs=[hf.InputValue(param_p1, 101)])
    t2 = hf.Task(
        schemas=s2,
        inputs=[hf.InputValue(param_p2, 201)],
        sourceable_elements=[],
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert wk.tasks[1].elements[0].input_sources["inputs.p3"] == "default"


def test_equivalent_where_args():
    rule_args = {"path": "inputs.p1", "condition": {"value.equal_to": 1}}
    i1 = hf.InputSource.task(task_ref=0, where=rule_args)
    i2 = hf.InputSource.task(task_ref=0, where=[rule_args])
    i3 = hf.InputSource.task(task_ref=0, where=hf.Rule(**rule_args))
    i4 = hf.InputSource.task(task_ref=0, where=[hf.Rule(**rule_args)])
    i5 = hf.InputSource.task(task_ref=0, where=hf.ElementFilter([hf.Rule(**rule_args)]))
    assert i1 == i2 == i3 == i4 == i5


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_source_where(null_config, tmp_path, store):
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
            sequences=[
                hf.ValueSequence(path="inputs.p1", values=[1, 2], nesting_order=0)
            ],
        ),
        hf.Task(
            schemas=s2,
            nesting_order={"inputs.p2": 0},
            input_sources={
                "p2": [
                    hf.InputSource.task(
                        task_ref=0,
                        where=hf.Rule(path="inputs.p1", condition={"value.equal_to": 2}),
                    )
                ]
            },
        ),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
        store=store,
    )
    assert wk.tasks.t2.num_elements == 1
    assert (
        wk.tasks.t2.elements[0].get_data_idx("inputs.p2")["inputs.p2"]
        == wk.tasks.t1.elements[1].get_data_idx("outputs.p2")["outputs.p2"]
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_source_where_parameter_value_class_sub_parameter(
    null_config, tmp_path, store
):
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
            sequences=[
                hf.ValueSequence(
                    path="inputs.p1", values=[P1(a=1), P1(a=2)], nesting_order=0
                )
            ],
        ),
        hf.Task(
            schemas=s2,
            nesting_order={"inputs.p2": 0},
            input_sources={
                "p2": [
                    hf.InputSource.task(
                        task_ref=0,
                        where=hf.Rule(
                            path="inputs.p1.a", condition={"value.equal_to": 2}
                        ),
                    )
                ]
            },
        ),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
        store=store,
    )
    assert wk.tasks.t2.num_elements == 1
    assert (
        wk.tasks.t2.elements[0].get_data_idx("inputs.p2")["inputs.p2"]
        == wk.tasks.t1.elements[1].get_data_idx("outputs.p2")["outputs.p2"]
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_source_where_parameter_value_class_sub_parameter_property(
    null_config, tmp_path, store
):
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
            sequences=[
                hf.ValueSequence(
                    path="inputs.p1", values=[P1(a=1), P1(a=2)], nesting_order=0
                )
            ],
        ),
        hf.Task(
            schemas=s2,
            nesting_order={"inputs.p2": 0},
            input_sources={
                "p2": [
                    hf.InputSource.task(
                        task_ref=0,
                        where=hf.Rule(
                            path="inputs.p1.twice_a", condition={"value.equal_to": 4}
                        ),
                    )
                ]
            },
        ),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
        store=store,
    )
    assert wk.tasks.t2.num_elements == 1
    assert (
        wk.tasks.t2.elements[0].get_data_idx("inputs.p2")["inputs.p2"]
        == wk.tasks.t1.elements[1].get_data_idx("outputs.p2")["outputs.p2"]
    )
