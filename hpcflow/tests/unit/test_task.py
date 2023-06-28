import copy
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    MissingInputs,
    TaskTemplateInvalidNesting,
    TaskTemplateMultipleInputValues,
    TaskTemplateMultipleSchemaObjectives,
    TaskTemplateUnexpectedInput,
)
from hpcflow.sdk.core.test_utils import make_schemas, make_tasks, make_workflow


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
def workflow_w0(null_config, tmp_path):
    t1 = hf.Task(schemas=[hf.TaskSchema(objective="t1", actions=[])])
    t2 = hf.Task(schemas=[hf.TaskSchema(objective="t2", actions=[])])

    wkt = hf.WorkflowTemplate(name="workflow_w0", tasks=[t1, t2])
    return hf.Workflow.from_template(wkt, path=tmp_path)


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
def workflow_w2(null_config, tmp_path, param_p1, param_p2):
    s1 = hf.TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p2])
    s2 = hf.TaskSchema("t2", actions=[], inputs=[param_p2, param_p3])

    t1 = hf.Task(
        schemas=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=1)],
    )
    t2 = hf.Task(
        schemas=s2,
        sequences=[
            hf.ValueSequence("inputs.p3", values=[301, 302, 303], nesting_order=1)
        ],
        nesting_order={"inputs.p2": 0},
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    return hf.Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def workflow_w3(null_config, tmp_path, param_p1, param_p2, param_p3, param_p4):
    s1 = hf.TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p3])
    s2 = hf.TaskSchema("t2", actions=[], inputs=[param_p2, param_p3], outputs=[param_p4])
    s3 = hf.TaskSchema("t3", actions=[], inputs=[param_p3, param_p4])

    t1 = hf.Task(schemas=s1, inputs=[hf.InputValue(param_p1, 101)])
    t2 = hf.Task(
        schemas=s2,
        sequences=[hf.ValueSequence("inputs.p2", values=[201, 202], nesting_order=1)],
    )
    t3 = hf.Task(schemas=s3, nesting_order={"inputs.p3": 0, "inputs.p4": 1})

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2, t3])
    return hf.Workflow.from_template(wkt, name=wkt.name, overwrite=True)


@pytest.fixture
def file_spec_fs1():
    return hf.FileSpec(label="file1", name="file1.txt")


@pytest.fixture
def env_1():
    return hf.Environment(name="env_1")


@pytest.fixture
def act_env_1(env_1):
    return hf.ActionEnvironment(env_1)


@pytest.fixture
def act_3(act_env_1, param_p2, file_spec_fs1):
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        output_file_parsers=[
            hf.OutputFileParser(output=param_p2, output_files=[file_spec_fs1]),
        ],
        environments=[act_env_1],
    )


@pytest.fixture
def schema_s3(param_p1, param_p2, act_3):
    return hf.TaskSchema("ts1", actions=[act_3], inputs=[param_p1], outputs=[param_p2])


@pytest.fixture
def workflow_w4(null_config, tmp_path, schema_s3, param_p1):
    t1 = hf.Task(schemas=schema_s3, inputs=[hf.InputValue(param_p1, 101)])
    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1])
    return hf.Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def env_1():
    return hf.Environment(name="env_1")


@pytest.fixture
def act_env_1(env_1):
    return hf.ActionEnvironment(env_1)


@pytest.fixture
def act_1(act_env_1):
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def act_2(act_env_1):
    return hf.Action(
        commands=[hf.Command("<<parameter:p2>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def schema_s1(param_p1, act_1):
    return hf.TaskSchema("ts1", actions=[act_1], inputs=[param_p1])


@pytest.fixture
def schema_s2(param_p1, act_1):
    return hf.TaskSchema(
        "ts1", actions=[act_1], inputs=[hf.SchemaInput(param_p1, default_value=101)]
    )


@pytest.fixture
def schema_s4(param_p2, act_2):
    return hf.TaskSchema("ts2", actions=[act_2], inputs=[param_p2])


@pytest.fixture
def schema_s5(param_p2, act_2):
    return hf.TaskSchema(
        "ts2", actions=[act_2], inputs=[hf.SchemaInput(param_p2, default_value=2002)]
    )


def test_task_get_available_task_input_sources_expected_return_first_task_local_value(
    schema_s1,
    param_p1,
):
    t1 = hf.Task(schemas=schema_s1, inputs=[hf.InputValue(param_p1, value=101)])

    available = t1.get_available_task_input_sources(
        element_set=t1.element_sets[0],
        source_tasks=[],
    )
    available_exp = {"p1": [hf.InputSource(source_type=hf.InputSourceType.LOCAL)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_first_task_default_value(
    schema_s2,
):
    t1 = hf.Task(schemas=schema_s2)
    available = t1.get_available_task_input_sources(element_set=t1.element_sets[0])
    available_exp = {"p1": [hf.InputSource(source_type=hf.InputSourceType.DEFAULT)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output(
    tmp_path,
):
    t1, t2 = make_tasks(
        schemas_spec=[[{"p1": None}, ("p2",), "t1"], [{"p2": None}, (), "t2"]],
        local_inputs={0: ("p1",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1.template],
    )
    available_exp = {
        "p2": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            )
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default(
    tmp_path,
):
    t1, t2 = make_tasks(
        schemas_spec=[[{"p1": None}, ("p2",), "t1"], [{"p2": 2001}, (), "t2"]],
        local_inputs={0: ("p1",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1.template],
    )
    available_exp = {
        "p2": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
            hf.InputSource(source_type=hf.InputSourceType.DEFAULT),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_local(
    tmp_path,
):
    t1, t2 = make_tasks(
        schemas_spec=[[{"p1": None}, ("p2",), "t1"], [{"p2": None}, (), "t2"]],
        local_inputs={0: ("p1",), 1: ("p2",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1.template],
    )
    available_exp = {
        "p2": [
            hf.InputSource(source_type=hf.InputSourceType.LOCAL),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default_and_local(
    tmp_path,
):
    t1, t2 = make_tasks(
        schemas_spec=[[{"p1": None}, ("p2",), "t1"], [{"p2": 2001}, (), "t2"]],
        local_inputs={0: ("p1",), 1: ("p2",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1.template],
    )
    available_exp = {
        "p2": [
            hf.InputSource(source_type=hf.InputSourceType.LOCAL),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
            hf.InputSource(source_type=hf.InputSourceType.DEFAULT),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_two_outputs(
    tmp_path,
):
    t1, t2, t3 = make_tasks(
        schemas_spec=[
            [{"p1": None}, ("p2", "p3"), "t1"],
            [{"p2": None}, ("p3", "p4"), "t2"],
            [{"p3": None}, (), "t3"],
        ],
        local_inputs={0: ("p1",), 1: ("p2",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1, t2]), path=tmp_path
    )
    available = t3.get_available_task_input_sources(
        element_set=t3.element_sets[0],
        source_tasks=[wk.tasks.t1.template, wk.tasks.t2.template],
    )
    available_exp = {
        "p3": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=1,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[1],
            ),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_two_params_one_output(
    tmp_path,
):
    t1, t2 = make_tasks(
        schemas_spec=[
            [{"p1": None}, ("p2", "p3"), "t1"],
            [{"p2": None, "p3": None}, (), "t2"],
        ],
        local_inputs={0: ("p1",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1.template],
    )
    available_exp = {
        "p2": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            )
        ],
        "p3": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            )
        ],
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_input_source_excluded_if_not_local(
    tmp_path,
):
    """Test an input source is excluded if it is not locally defined (meaning it comes
    from another task)."""

    t1, t2, t3 = make_tasks(
        schemas_spec=[
            [{"p1": None}, ("p1",), "t1"],  # sources for t3: input + output
            [{"p1": None}, ("p1",), "t2"],  # sources fot t3: output only
            [{"p1": None}, ("p1",), "t3"],
        ],
        local_inputs={0: ("p1",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1, t2]), path=tmp_path
    )
    available = t3.get_available_task_input_sources(
        element_set=t3.element_sets[0],
        source_tasks=[wk.tasks.t1.template, wk.tasks.t2.template],
    )
    available_exp = {
        "p1": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=1,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[1],
            ),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.INPUT,
                element_iters=[0],
            ),
        ],
    }
    assert available == available_exp


def test_get_task_unique_names_two_tasks_no_repeats():
    s1 = hf.TaskSchema("t1", actions=[])
    s2 = hf.TaskSchema("t2", actions=[])

    t1 = hf.Task(schemas=s1)
    t2 = hf.Task(schemas=s2)

    assert hf.Task.get_task_unique_names([t1, t2]) == ["t1", "t2"]


def test_get_task_unique_names_two_tasks_with_repeat():
    s1 = hf.TaskSchema("t1", actions=[])

    t1 = hf.Task(schemas=s1)
    t2 = hf.Task(schemas=s1)

    assert hf.Task.get_task_unique_names([t1, t2]) == ["t1_1", "t1_2"]


def test_raise_on_multiple_schema_objectives():
    s1 = hf.TaskSchema("t1", actions=[])
    s2 = hf.TaskSchema("t2", actions=[])
    with pytest.raises(TaskTemplateMultipleSchemaObjectives):
        hf.Task(schemas=[s1, s2])


def test_raise_on_unexpected_inputs(param_p1, param_p2):
    s1 = make_schemas([[{"p1": None}, ()]])

    with pytest.raises(TaskTemplateUnexpectedInput):
        hf.Task(
            schemas=s1,
            inputs=[
                hf.InputValue(param_p1, value=101),
                hf.InputValue(param_p2, value=4),
            ],
        )


def test_raise_on_multiple_input_values(param_p1):
    s1 = make_schemas([[{"p1": None}, ()]])

    with pytest.raises(TaskTemplateMultipleInputValues):
        hf.Task(
            schemas=s1,
            inputs=[
                hf.InputValue(param_p1, value=101),
                hf.InputValue(param_p1, value=7),
            ],
        )


def test_expected_return_defined_and_undefined_input_types(param_p1, param_p2):
    s1 = make_schemas([[{"p1": None, "p2": None}, ()]])

    t1 = hf.Task(schemas=s1, inputs=[hf.InputValue(param_p1, value=101)])
    element_set = t1.element_sets[0]
    assert element_set.defined_input_types == {
        param_p1.typ
    } and element_set.undefined_input_types == {param_p2.typ}


def test_expected_return_all_schema_input_types_single_schema(param_p1, param_p2):
    s1 = make_schemas([[{"p1": None, "p2": None}, ()]])
    t1 = hf.Task(schemas=s1)

    assert t1.all_schema_input_types == {param_p1.typ, param_p2.typ}


def test_expected_return_all_schema_input_types_multiple_schemas(
    param_p1, param_p2, param_p3
):
    s1, s2 = make_schemas(
        [[{"p1": None, "p2": None}, (), "t1"], [{"p1": None, "p3": None}, (), "t1"]]
    )

    t1 = hf.Task(schemas=[s1, s2])

    assert t1.all_schema_input_types == {param_p1.typ, param_p2.typ, param_p3.typ}


def test_expected_name_single_schema():
    s1 = hf.TaskSchema("t1", actions=[])
    t1 = hf.Task(schemas=[s1])
    assert t1.name == "t1"


def test_expected_name_single_schema_with_method():
    s1 = hf.TaskSchema("t1", method="m1", actions=[])
    t1 = hf.Task(schemas=s1)
    assert t1.name == "t1_m1"


def test_expected_name_single_schema_with_implementation():
    s1 = hf.TaskSchema("t1", implementation="i1", actions=[])
    t1 = hf.Task(schemas=s1)
    assert t1.name == "t1_i1"


def test_expected_name_single_schema_with_method_and_implementation():
    s1 = hf.TaskSchema("t1", method="m1", implementation="i1", actions=[])
    t1 = hf.Task(schemas=s1)
    assert t1.name == "t1_m1_i1"


def test_expected_name_multiple_schemas():
    s1 = hf.TaskSchema("t1", actions=[])
    s2 = hf.TaskSchema("t1", actions=[])
    t1 = hf.Task(schemas=[s1, s2])
    assert t1.name == "t1"


def test_expected_name_two_schemas_first_with_method():
    s1 = hf.TaskSchema("t1", method="m1", actions=[])
    s2 = hf.TaskSchema("t1", actions=[])
    t1 = hf.Task(schemas=[s1, s2])
    assert t1.name == "t1_m1"


def test_expected_name_two_schemas_first_with_method_and_implementation():
    s1 = hf.TaskSchema("t1", method="m1", implementation="i1", actions=[])
    s2 = hf.TaskSchema("t1", actions=[])
    t1 = hf.Task(schemas=[s1, s2])
    assert t1.name == "t1_m1_i1"


def test_expected_name_two_schemas_both_with_method():
    s1 = hf.TaskSchema("t1", method="m1", actions=[])
    s2 = hf.TaskSchema("t1", method="m2", actions=[])
    t1 = hf.Task(schemas=[s1, s2])
    assert t1.name == "t1_m1_and_m2"


def test_expected_name_two_schemas_first_with_method_second_with_implementation():
    s1 = hf.TaskSchema("t1", method="m1", actions=[])
    s2 = hf.TaskSchema("t1", implementation="i2", actions=[])
    t1 = hf.Task(schemas=[s1, s2])
    assert t1.name == "t1_m1_and_i2"


def test_expected_name_two_schemas_first_with_implementation_second_with_method():
    s1 = hf.TaskSchema("t1", implementation="i1", actions=[])
    s2 = hf.TaskSchema("t1", method="m2", actions=[])
    t1 = hf.Task(schemas=[s1, s2])
    assert t1.name == "t1_i1_and_m2"


def test_expected_name_two_schemas_both_with_method_and_implementation():
    s1 = hf.TaskSchema("t1", method="m1", implementation="i1", actions=[])
    s2 = hf.TaskSchema("t1", method="m2", implementation="i2", actions=[])
    t1 = hf.Task(schemas=[s1, s2])
    assert t1.name == "t1_m1_i1_and_m2_i2"


def test_raise_on_negative_nesting_order():
    s1 = make_schemas([[{"p1": None}, ()]])
    with pytest.raises(TaskTemplateInvalidNesting):
        hf.Task(schemas=s1, nesting_order={"p1": -1})


# TODO: test resolution of elements and with raise MissingInputs


def test_empty_task_init():
    """Check we can init a hf.Task with no input values."""
    s1 = make_schemas([[{"p1": None}, ()]])
    t1 = hf.Task(schemas=s1)


def test_task_task_dependencies(tmp_path):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    assert wk.tasks.t2.get_task_dependencies(as_objects=True) == [wk.tasks.t1]


def test_task_dependent_tasks(tmp_path):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    assert wk.tasks.t1.get_dependent_tasks(as_objects=True) == [wk.tasks.t2]


def test_task_element_dependencies(tmp_path):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    assert wk.tasks.t2.get_element_dependencies() == [0, 1]


def test_task_dependent_elements(tmp_path):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    assert wk.tasks.t1.get_dependent_elements() == [2, 3]


def test_task_add_elements_without_propagation_expected_workflow_num_elements(
    tmp_path, param_p1
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(inputs=[hf.InputValue(param_p1, 103)])
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 1


def test_task_add_elements_without_propagation_expected_task_num_elements(
    tmp_path, param_p1
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.tasks.t1.num_elements
    wk.tasks.t1.add_elements(inputs=[hf.InputValue(param_p1, 103)])
    num_elems_new = wk.tasks.t1.num_elements
    assert num_elems_new - num_elems == 1


def test_task_add_elements_without_propagation_expected_new_data_index(
    tmp_path, param_p1
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    data_index = [sorted(i.get_data_idx().keys()) for i in wk.tasks.t1.elements[:]]
    wk.tasks.t1.add_elements(inputs=[hf.InputValue(param_p1, 103)])
    data_index_new = [sorted(i.get_data_idx().keys()) for i in wk.tasks.t1.elements[:]]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == [["inputs.p1", "outputs.p2", "resources.any"]]


def test_task_add_elements_with_propagation_expected_workflow_num_elements(
    tmp_path, param_p1
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 103)],
        propagate_to=[hf.ElementPropagation(task=wk.tasks.t2)],
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 2


def test_task_add_elements_with_propagation_expected_task_num_elements(
    tmp_path, param_p1
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 103)],
        propagate_to=[hf.ElementPropagation(task=wk.tasks.t2)],
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 1 and num_elems_diff[1] == 1


def test_task_add_elements_with_propagation_expected_new_data_index(tmp_path, param_p1):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 103)],
        propagate_to=[hf.ElementPropagation(task=wk.tasks.t2)],
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    data_index_new = [sorted(i.get_data_idx().keys()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    assert new_elems_t1 == [
        [
            "inputs.p1",
            "outputs.p2",
            "resources.any",
        ]
    ] and new_elems_t2 == [["inputs.p2", "resources.any"]]


def test_task_add_elements_sequence_without_propagation_expected_workflow_num_elements(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[hf.ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 2


def test_task_add_elements_sequence_without_propagation_expected_task_num_elements(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.tasks.t1.num_elements
    wk.tasks.t1.add_elements(
        sequences=[hf.ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    num_elems_new = wk.tasks.t1.num_elements
    assert num_elems_new - num_elems == 2


def test_task_add_elements_sequence_without_propagation_expected_new_data_index(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    wk.tasks.t1.add_elements(
        sequences=[hf.ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    data_index_new = [sorted(i.get_data_idx().keys()) for i in wk.elements()]
    new_elems = data_index_new[t1_num_elems:t1_num_elems_new]
    assert new_elems == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
    ]


def test_task_add_elements_sequence_with_propagation_expected_workflow_num_elements(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(task=wk.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 6


def test_task_add_elements_sequence_with_propagation_expected_task_num_elements(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(task=wk.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 3 and num_elems_diff[1] == 3


def test_task_add_elements_sequence_with_propagation_expected_new_data_index(tmp_path):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(task=wk.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    data_index_new = [sorted(i.get_data_idx().keys()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    assert new_elems_t1 == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
    ] and new_elems_t2 == [
        ["inputs.p2", "resources.any"],
        ["inputs.p2", "resources.any"],
        ["inputs.p2", "resources.any"],
    ]


def test_task_add_elements_sequence_with_propagation_into_sequence_expected_workflow_num_elements(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None, "p3": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 1)], 1: [("inputs.p3", 3, 1)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(
                task=wk.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 12


def test_task_add_elements_sequence_with_propagation_into_sequence_expected_task_num_elements(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None, "p3": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 1)], 1: [("inputs.p3", 3, 1)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(
                task=wk.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 3 and num_elems_diff[1] == 9


def test_task_add_elements_sequence_with_propagation_into_sequence_expected_new_data_index(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None, "p3": None}, (), "t2"],
        ],
        local_sequences={0: [("inputs.p1", 2, 1)], 1: [("inputs.p3", 3, 1)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )

    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(
                task=wk.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    data_index_new = [sorted(i.get_data_idx().keys()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    assert new_elems_t1 == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
    ] and new_elems_t2 == [
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
    ]


def test_task_add_elements_multi_task_dependence_expected_workflow_num_elements(
    tmp_path, param_p1
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p3",), "t1"],
            [{"p2": None, "p3": None}, ("p4",), "t2"],
            [{"p3": None, "p4": None}, (), "t3"],
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 5


def test_task_add_elements_multi_task_dependence_expected_task_num_elements(
    tmp_path, param_p1
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p3",), "t1"],
            [{"p2": None, "p3": None}, ("p4",), "t2"],
            [{"p3": None, "p4": None}, (), "t3"],
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff == [1, 2, 2]


def test_task_add_elements_multi_task_dependence_expected_new_data_index(
    tmp_path, param_p1
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p3",), "t1"],
            [{"p2": None, "p3": None}, ("p4",), "t2"],
            [{"p3": None, "p4": None}, (), "t3"],
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    t3_num_elems = wk.tasks.t3.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    t3_num_elems_new = wk.tasks.t3.num_elements
    data_index_new = [sorted(i.get_data_idx().keys()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    new_elems_t3 = data_index_new[
        t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems : t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems_new
    ]

    assert (
        new_elems_t1 == [["inputs.p1", "outputs.p3", "resources.any"]]
        and new_elems_t2
        == [["inputs.p2", "inputs.p3", "outputs.p4", "resources.any"]] * 2
        and new_elems_t3 == [["inputs.p3", "inputs.p4", "resources.any"]] * 2
    )


def test_task_add_elements_sequence_multi_task_dependence_workflow_num_elements(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p3",), "t1"],
            [{"p2": None, "p3": None}, ("p4",), "t2"],
            [{"p3": None, "p4": None}, (), "t3"],
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 27


def test_task_add_elements_sequence_multi_task_dependence_expected_task_num_elements(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p3",), "t1"],
            [{"p2": None, "p3": None}, ("p4",), "t2"],
            [{"p3": None, "p4": None}, (), "t3"],
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff == [3, 6, 18]


def test_task_add_elements_sequence_multi_task_dependence_expected_new_data_index(
    tmp_path,
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p3",), "t1"],
            [{"p2": None, "p3": None}, ("p4",), "t2"],
            [{"p3": None, "p4": None}, (), "t3"],
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    t3_num_elems = wk.tasks.t3.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    t3_num_elems_new = wk.tasks.t3.num_elements

    data_index_new = [sorted(i.get_data_idx().keys()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    new_elems_t3 = data_index_new[
        t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems : t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems_new
    ]
    assert (
        new_elems_t1 == [["inputs.p1", "outputs.p3", "resources.any"]] * 3
        and new_elems_t2
        == [["inputs.p2", "inputs.p3", "outputs.p4", "resources.any"]] * 6
        and new_elems_t3 == [["inputs.p3", "inputs.p4", "resources.any"]] * 18
    )


def test_task_add_elements_simple_dependence_three_tasks(tmp_path, param_p1):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, ("p3",), "t2"],
            [{"p3": None}, (), "t3"],
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    num_elems = [i.num_elements for i in wk.tasks]
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={"t2": {}, "t3": {}},
    )
    num_elems_new = [i.num_elements for i in wk.tasks]
    assert num_elems_new == [i + 1 for i in num_elems]


def test_no_change_to_tasks_metadata_on_add_task_failure(tmp_path):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, (), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    tasks_meta = copy.deepcopy(wk._store.get_tasks())

    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = hf.Task(schemas=s2)
    with pytest.raises(MissingInputs) as exc_info:
        wk.add_task(t2)

    assert wk._store.get_tasks() == tasks_meta


def test_no_change_to_parameter_data_on_add_task_failure(tmp_path, param_p2, param_p3):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, (), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    param_data = copy.deepcopy(wk.get_all_parameters())
    s2 = make_schemas([[{"p1": None, "p2": None, "p3": None}, ()]])
    t2 = hf.Task(schemas=s2, inputs=[hf.InputValue(param_p2, 201)])
    with pytest.raises(MissingInputs) as exc_info:
        wk.add_task(t2)

    assert wk.get_all_parameters() == param_data


def test_expected_additional_parameter_data_on_add_task(tmp_path, param_p3):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, (), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    param_data = copy.deepcopy(wk.get_all_parameter_data())

    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = hf.Task(schemas=s2, inputs=[hf.InputValue(param_p3, 301)])
    wk.add_task(t2)

    param_data_new = wk.get_all_parameter_data()

    new_keys = set(param_data_new.keys()) - set(param_data.keys())
    new_data = [param_data_new[k] for k in new_keys]

    # one new key for resources, one for param_p3 value
    res = {k: None for k in hf.ResourceSpec.ALLOWED_PARAMETERS}
    assert new_data == [res, 301]


def test_parameters_accepted_on_add_task(tmp_path, param_p3):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, (), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = hf.Task(schemas=s2, inputs=[hf.InputValue(param_p3, 301)])
    wk.add_task(t2)
    assert not wk._store._pending.add_parameters


def test_parameters_pending_during_add_task(tmp_path, param_p3):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, (), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = hf.Task(schemas=s2, inputs=[hf.InputValue(param_p3, 301)])
    with wk.batch_update():
        wk.add_task(t2)
        assert wk._store._pending.add_parameters


def test_add_task_after(workflow_w0):
    new_task = hf.Task(schemas=hf.TaskSchema(objective="after_t1", actions=[]))
    workflow_w0.add_task_after(new_task, workflow_w0.tasks.t1)
    assert [i.name for i in workflow_w0.tasks] == ["t1", "after_t1", "t2"]


def test_add_task_after_no_ref(workflow_w0):
    new_task = hf.Task(schemas=hf.TaskSchema(objective="at_end", actions=[]))
    workflow_w0.add_task_after(new_task)
    assert [i.name for i in workflow_w0.tasks] == ["t1", "t2", "at_end"]


def test_add_task_before(workflow_w0):
    new_task = hf.Task(schemas=hf.TaskSchema(objective="before_t2", actions=[]))
    workflow_w0.add_task_before(new_task, workflow_w0.tasks.t2)
    assert [i.name for i in workflow_w0.tasks] == ["t1", "before_t2", "t2"]


def test_add_task_before_no_ref(workflow_w0):
    new_task = hf.Task(schemas=hf.TaskSchema(objective="at_start", actions=[]))
    workflow_w0.add_task_before(new_task)
    assert [i.name for i in workflow_w0.tasks] == ["at_start", "t1", "t2"]


@pytest.fixture
def env_1():
    return hf.Environment(name="env_1")


@pytest.fixture
def act_env_1(env_1):
    return hf.ActionEnvironment(env_1)


def test_parameter_two_modifying_actions_expected_data_indices(
    tmp_path, act_env_1, param_p1
):
    act1 = hf.Action(
        commands=[hf.Command("doSomething <<parameter:p1>>", stdout="<<parameter:p1>>")],
        environments=[act_env_1],
    )
    act2 = hf.Action(
        commands=[hf.Command("doSomething <<parameter:p1>>", stdout="<<parameter:p1>>")],
        environments=[act_env_1],
    )

    s1 = hf.TaskSchema("t1", actions=[act1, act2], inputs=[param_p1], outputs=[param_p1])
    t1 = hf.Task(schemas=[s1], inputs=[hf.InputValue(param_p1, 101)])

    wkt = hf.WorkflowTemplate(name="w3", tasks=[t1])
    wk = hf.Workflow.from_template(template=wkt, path=tmp_path)
    iter_0 = wk.tasks.t1.elements[0].iterations[0]
    act_runs = iter_0.action_runs

    p1_idx_schema_in = iter_0.data_idx["inputs.p1"]
    p1_idx_schema_out = iter_0.data_idx["outputs.p1"]

    p1_idx_0 = act_runs[0].data_idx["inputs.p1"]
    p1_idx_1 = act_runs[0].data_idx["outputs.p1"]
    p1_idx_2 = act_runs[1].data_idx["inputs.p1"]
    p1_idx_3 = act_runs[1].data_idx["outputs.p1"]

    assert (
        p1_idx_schema_in == p1_idx_0
        and p1_idx_1 == p1_idx_2
        and p1_idx_3 == p1_idx_schema_out
    )
