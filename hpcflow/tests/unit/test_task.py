import copy
import pytest
from hpcflow.api import (
    Action,
    ActionEnvironment,
    Command,
    ElementPropagation,
    Environment,
    FileSpec,
    OutputFileParser,
    ValueSequence,
    hpcflow,
    InputSourceType,
    Parameter,
    SchemaInput,
    SchemaOutput,
    TaskSchema,
    TaskObjective,
    TaskSourceType,
    Task,
    InputValue,
    InputSource,
    InputSourceMode,
    Workflow,
    WorkflowTemplate,
)
from hpcflow.sdk.core.errors import (
    MissingInputs,
    TaskTemplateInvalidNesting,
    TaskTemplateMultipleInputValues,
    TaskTemplateMultipleSchemaObjectives,
    TaskTemplateUnexpectedInput,
)
from hpcflow.sdk.core.test_utils import make_schemas


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
def workflow_w2(null_config, tmp_path, param_p1, param_p2):
    s1 = TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p2])
    s2 = TaskSchema("t2", actions=[], inputs=[param_p2, param_p3])

    t1 = Task(
        schemas=s1,
        sequences=[ValueSequence("inputs.p1", values=[101, 102], nesting_order=1)],
    )
    t2 = Task(
        schemas=s2,
        sequences=[ValueSequence("inputs.p3", values=[301, 302, 303], nesting_order=1)],
        nesting_order={"inputs.p2": 0},
    )

    wkt = WorkflowTemplate(name="w1", tasks=[t1, t2])
    return Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def workflow_w3(null_config, tmp_path, param_p1, param_p2, param_p3, param_p4):
    s1 = TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p3])
    s2 = TaskSchema("t2", actions=[], inputs=[param_p2, param_p3], outputs=[param_p4])
    s3 = TaskSchema("t3", actions=[], inputs=[param_p3, param_p4])

    t1 = Task(schemas=s1, inputs=[InputValue(param_p1, 101)])
    t2 = Task(
        schemas=s2,
        sequences=[ValueSequence("inputs.p2", values=[201, 202], nesting_order=1)],
    )
    t3 = Task(schemas=s3, nesting_order={"inputs.p3": 0, "inputs.p4": 1})

    wkt = WorkflowTemplate(name="w1", tasks=[t1, t2, t3])
    return Workflow.from_template(wkt, name=wkt.name, overwrite=True)


@pytest.fixture
def file_spec_fs1():
    return FileSpec(label="file1", name="file1.txt")


@pytest.fixture
def env_1():
    return Environment(name="env_1")


@pytest.fixture
def act_env_1(env_1):
    return ActionEnvironment(env_1)


@pytest.fixture
def act_3(act_env_1, param_p2, file_spec_fs1):
    return Action(
        commands=[Command("<<parameter:p1>>")],
        output_file_parsers=[
            OutputFileParser(output=param_p2, output_files=[file_spec_fs1]),
        ],
        environments=[act_env_1],
    )


@pytest.fixture
def schema_s3(param_p1, param_p2, act_3):
    return TaskSchema("ts1", actions=[act_3], inputs=[param_p1], outputs=[param_p2])


@pytest.fixture
def workflow_w4(null_config, tmp_path, schema_s3, param_p1):
    t1 = Task(schemas=schema_s3, inputs=[InputValue(param_p1, 101)])
    wkt = WorkflowTemplate(name="w1", tasks=[t1])
    return Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def env_1():
    return Environment(name="env_1")


@pytest.fixture
def act_env_1(env_1):
    return ActionEnvironment(env_1)


@pytest.fixture
def act_1(act_env_1):
    return Action(
        commands=[Command("<<parameter:p1>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def act_2(act_env_1):
    return Action(
        commands=[Command("<<parameter:p2>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def schema_s1(param_p1, act_1):
    return TaskSchema("ts1", actions=[act_1], inputs=[param_p1])


@pytest.fixture
def schema_s2(param_p1, act_1):
    return TaskSchema(
        "ts1", actions=[act_1], inputs=[SchemaInput(param_p1, default_value=101)]
    )


@pytest.fixture
def schema_s4(param_p2, act_2):
    return TaskSchema("ts2", actions=[act_2], inputs=[param_p2])


@pytest.fixture
def schema_s5(param_p2, act_2):
    return TaskSchema(
        "ts2", actions=[act_2], inputs=[SchemaInput(param_p2, default_value=2002)]
    )


def test_task_expected_input_source_mode_no_sources(schema_s1, param_p1):
    t1 = Task(
        schemas=schema_s1,
        inputs=[InputValue(param_p1, value=101)],
    )
    assert t1.element_sets[0].input_source_mode == InputSourceMode.AUTO


def test_task_expected_input_source_mode_with_sources(schema_s1, param_p1):
    t1 = Task(
        schemas=schema_s1,
        inputs=[InputValue(param_p1, value=101)],
        input_sources=[InputSource.local()],
    )
    assert t1.element_sets[0].input_source_mode == InputSourceMode.MANUAL


def test_task_get_available_task_input_sources_expected_return_first_task_local_value(
    schema_s1,
    param_p1,
):

    t1 = Task(schemas=schema_s1, inputs=[InputValue(param_p1, value=101)])

    available = t1.get_available_task_input_sources(
        element_set=t1.element_sets[0],
        source_tasks=[],
    )
    available_exp = {"p1": [InputSource(source_type=InputSourceType.LOCAL)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_first_task_default_value(
    schema_s2,
):

    t1 = Task(schemas=schema_s2)
    available = t1.get_available_task_input_sources(element_set=t1.element_sets[0])
    available_exp = {"p1": [InputSource(source_type=InputSourceType.DEFAULT)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output(
    schema_s3, schema_s4
):

    t1 = Task(schemas=schema_s3)
    t2 = Task(schemas=schema_s4)

    t1._insert_ID = 0

    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[t1],
    )
    available_exp = {
        "p2": [
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            )
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default(
    schema_s3, schema_s5
):

    t1 = Task(schemas=schema_s3)
    t2 = Task(schemas=schema_s5)

    t1._insert_ID = 0

    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[t1],
    )
    available_exp = {
        "p2": [
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            ),
            InputSource(source_type=InputSourceType.DEFAULT),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_local(
    schema_s3, schema_s4, param_p2
):

    t1 = Task(schemas=schema_s3)
    t2 = Task(schemas=schema_s4, inputs=[InputValue(param_p2, value=202)])

    t1._insert_ID = 0

    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[t1],
    )
    available_exp = {
        "p2": [
            InputSource(source_type=InputSourceType.LOCAL),
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            ),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default_and_local(
    schema_s3, schema_s5, param_p2
):

    t1 = Task(schemas=schema_s3)
    t2 = Task(schemas=schema_s5, inputs=[InputValue(param_p2, value=202)])

    t1._insert_ID = 0

    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[t1],
    )
    available_exp = {
        "p2": [
            InputSource(source_type=InputSourceType.LOCAL),
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            ),
            InputSource(source_type=InputSourceType.DEFAULT),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_two_outputs():
    s1, s2, s3 = make_schemas(
        [
            [{"p1": None}, ("p2", "p3")],
            [{"p2": None}, ("p3", "p4")],
            [{"p3": None}, ()],
        ]
    )

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s2)
    t3 = Task(schemas=s3)

    t1._insert_ID = 0
    t2._insert_ID = 1

    available = t3.get_available_task_input_sources(
        element_set=t3.element_sets[0],
        source_tasks=[t1, t2],
    )
    available_exp = {
        "p3": [
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            ),
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=1,
                task_source_type=TaskSourceType.OUTPUT,
            ),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_two_params_one_output(
    param_p1,
    param_p2,
    param_p3,
):

    s1, s2 = make_schemas(
        [
            [{"p1": None}, ("p2", "p3")],
            [{"p2": None, "p3": None}, ()],
        ]
    )

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s2)

    t1._insert_ID = 0

    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[t1],
    )
    available_exp = {
        "p2": [
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            )
        ],
        "p3": [
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            )
        ],
    }
    assert available == available_exp


def test_get_task_unique_names_two_tasks_no_repeats():
    s1 = TaskSchema("t1", actions=[])
    s2 = TaskSchema("t2", actions=[])

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s2)

    assert Task.get_task_unique_names([t1, t2]) == ["t1", "t2"]


def test_get_task_unique_names_two_tasks_with_repeat():

    s1 = TaskSchema("t1", actions=[])

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s1)

    assert Task.get_task_unique_names([t1, t2]) == ["t1_1", "t1_2"]


def test_raise_on_multiple_schema_objectives():

    s1 = TaskSchema("t1", actions=[])
    s2 = TaskSchema("t2", actions=[])
    with pytest.raises(TaskTemplateMultipleSchemaObjectives):
        Task(schemas=[s1, s2])


def test_raise_on_unexpected_inputs(param_p1, param_p2):

    s1 = make_schemas([[{"p1": None}, ()]])

    with pytest.raises(TaskTemplateUnexpectedInput):
        Task(
            schemas=s1,
            inputs=[
                InputValue(param_p1, value=101),
                InputValue(param_p2, value=4),
            ],
        )


def test_raise_on_multiple_input_values(param_p1):

    s1 = make_schemas([[{"p1": None}, ()]])

    with pytest.raises(TaskTemplateMultipleInputValues):
        Task(
            schemas=s1,
            inputs=[
                InputValue(param_p1, value=101),
                InputValue(param_p1, value=7),
            ],
        )


def test_expected_return_defined_and_undefined_input_types(param_p1, param_p2):

    s1 = make_schemas([[{"p1": None, "p2": None}, ()]])

    t1 = Task(schemas=s1, inputs=[InputValue(param_p1, value=101)])
    element_set = t1.element_sets[0]
    assert element_set.defined_input_types == {
        param_p1.typ
    } and element_set.undefined_input_types == {param_p2.typ}


def test_expected_return_all_schema_input_types_single_schema(param_p1, param_p2):

    s1 = make_schemas([[{"p1": None, "p2": None}, ()]])
    t1 = Task(schemas=s1)

    assert t1.all_schema_input_types == {param_p1.typ, param_p2.typ}


def test_expected_return_all_schema_input_types_multiple_schemas(
    param_p1, param_p2, param_p3
):

    s1, s2 = make_schemas(
        [[{"p1": None, "p2": None}, (), "t1"], [{"p1": None, "p3": None}, (), "t1"]]
    )

    t1 = Task(schemas=[s1, s2])

    assert t1.all_schema_input_types == {param_p1.typ, param_p2.typ, param_p3.typ}


def test_expected_name_single_schema():
    s1 = TaskSchema("t1", actions=[])
    t1 = Task(schemas=[s1])
    assert t1.name == "t1"


def test_expected_name_single_schema_with_method():
    s1 = TaskSchema("t1", method="m1", actions=[])
    t1 = Task(schemas=s1)
    assert t1.name == "t1_m1"


def test_expected_name_single_schema_with_implementation():
    s1 = TaskSchema("t1", implementation="i1", actions=[])
    t1 = Task(schemas=s1)
    assert t1.name == "t1_i1"


def test_expected_name_single_schema_with_method_and_implementation():
    s1 = TaskSchema("t1", method="m1", implementation="i1", actions=[])
    t1 = Task(schemas=s1)
    assert t1.name == "t1_m1_i1"


def test_expected_name_multiple_schemas():
    s1 = TaskSchema("t1", actions=[])
    s2 = TaskSchema("t1", actions=[])
    t1 = Task(schemas=[s1, s2])
    assert t1.name == "t1"


def test_expected_name_two_schemas_first_with_method():
    s1 = TaskSchema("t1", method="m1", actions=[])
    s2 = TaskSchema("t1", actions=[])
    t1 = Task(schemas=[s1, s2])
    assert t1.name == "t1_m1"


def test_expected_name_two_schemas_first_with_method_and_implementation():
    s1 = TaskSchema("t1", method="m1", implementation="i1", actions=[])
    s2 = TaskSchema("t1", actions=[])
    t1 = Task(schemas=[s1, s2])
    assert t1.name == "t1_m1_i1"


def test_expected_name_two_schemas_both_with_method():
    s1 = TaskSchema("t1", method="m1", actions=[])
    s2 = TaskSchema("t1", method="m2", actions=[])
    t1 = Task(schemas=[s1, s2])
    assert t1.name == "t1_m1_and_m2"


def test_expected_name_two_schemas_first_with_method_second_with_implementation():
    s1 = TaskSchema("t1", method="m1", actions=[])
    s2 = TaskSchema("t1", implementation="i2", actions=[])
    t1 = Task(schemas=[s1, s2])
    assert t1.name == "t1_m1_and_i2"


def test_expected_name_two_schemas_first_with_implementation_second_with_method():
    s1 = TaskSchema("t1", implementation="i1", actions=[])
    s2 = TaskSchema("t1", method="m2", actions=[])
    t1 = Task(schemas=[s1, s2])
    assert t1.name == "t1_i1_and_m2"


def test_expected_name_two_schemas_both_with_method_and_implementation():
    s1 = TaskSchema("t1", method="m1", implementation="i1", actions=[])
    s2 = TaskSchema("t1", method="m2", implementation="i2", actions=[])
    t1 = Task(schemas=[s1, s2])
    assert t1.name == "t1_m1_i1_and_m2_i2"


def test_raise_on_negative_nesting_order():
    s1 = make_schemas([[{"p1": None}, ()]])
    with pytest.raises(TaskTemplateInvalidNesting):
        Task(schemas=s1, nesting_order={"p1": -1})


# TODO: test resolution of elements and with raise MissingInputs


def test_empty_task_init():
    """Check we can init a Task with no input values."""
    s1 = make_schemas([[{"p1": None}, ()]])
    t1 = Task(schemas=s1)


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_task_dependencies(workflow_w1):
    assert workflow_w1.tasks.t2.task_dependencies == [workflow_w1.tasks.t1]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_dependent_tasks(workflow_w1):
    assert workflow_w1.tasks.t1.dependent_tasks == [workflow_w1.tasks.t2]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_element_dependencies(workflow_w1):
    assert workflow_w1.tasks.t2.element_dependencies == [0, 1]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_dependent_elements(workflow_w1):
    assert workflow_w1.tasks.t1.dependent_elements == [2, 3]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_without_propagation_expected_workflow_num_elements(
    workflow_w1, param_p1
):
    num_elems = workflow_w1.num_elements
    workflow_w1.tasks.t1.add_elements(inputs=[InputValue(param_p1, 103)])
    num_elems_new = workflow_w1.num_elements
    assert num_elems_new - num_elems == 1


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_without_propagation_expected_task_num_elements(
    workflow_w1, param_p1
):
    num_elems = workflow_w1.tasks.t1.num_elements
    workflow_w1.tasks.t1.add_elements(inputs=[InputValue(param_p1, 103)])
    num_elems_new = workflow_w1.tasks.t1.num_elements
    assert num_elems_new - num_elems == 1


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_without_propagation_expected_new_data_index(
    workflow_w1, param_p1
):
    data_index = [sorted(i.data_index.keys()) for i in workflow_w1.elements]
    workflow_w1.tasks.t1.add_elements(inputs=[InputValue(param_p1, 103)])
    data_index_new = [sorted(i.data_index.keys()) for i in workflow_w1.elements]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == [["inputs.p1", "outputs.p2", "resources.any"]]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_with_propagation_expected_workflow_num_elements(
    workflow_w1, param_p1
):
    num_elems = workflow_w1.num_elements
    workflow_w1.tasks.t1.add_elements(
        inputs=[InputValue(param_p1, 103)],
        propagate_to=[ElementPropagation(task=workflow_w1.tasks.t2)],
    )
    num_elems_new = workflow_w1.num_elements
    assert num_elems_new - num_elems == 2


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_with_propagation_expected_task_num_elements(
    workflow_w1, param_p1
):
    num_elems = [task.num_elements for task in workflow_w1.tasks]
    workflow_w1.tasks.t1.add_elements(
        inputs=[InputValue(param_p1, 103)],
        propagate_to=[ElementPropagation(task=workflow_w1.tasks.t2)],
    )
    num_elems_new = [task.num_elements for task in workflow_w1.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 1 and num_elems_diff[1] == 1


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_with_propagation_expected_new_data_index(
    workflow_w1, param_p1
):
    data_index = [sorted(i.data_index.keys()) for i in workflow_w1.elements]
    workflow_w1.tasks.t1.add_elements(
        inputs=[InputValue(param_p1, 103)],
        propagate_to=[ElementPropagation(task=workflow_w1.tasks.t2)],
    )
    data_index_new = [sorted(i.data_index.keys()) for i in workflow_w1.elements]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p2", "resources.any"],
    ]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_without_propagation_expected_workflow_num_elements(
    workflow_w1,
):
    num_elems = workflow_w1.num_elements
    workflow_w1.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    num_elems_new = workflow_w1.num_elements
    assert num_elems_new - num_elems == 2


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_without_propagation_expected_task_num_elements(
    workflow_w1,
):
    num_elems = workflow_w1.tasks.t1.num_elements
    workflow_w1.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    num_elems_new = workflow_w1.tasks.t1.num_elements
    assert num_elems_new - num_elems == 2


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_without_propagation_expected_new_data_index(
    workflow_w1,
):
    data_index = [sorted(i.data_index.keys()) for i in workflow_w1.elements]
    workflow_w1.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    data_index_new = [sorted(i.data_index.keys()) for i in workflow_w1.elements]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
    ]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_with_propagation_expected_workflow_num_elements(
    workflow_w1,
):
    num_elems = workflow_w1.num_elements
    workflow_w1.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)],
        propagate_to=[
            ElementPropagation(task=workflow_w1.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    num_elems_new = workflow_w1.num_elements
    assert num_elems_new - num_elems == 6


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_with_propagation_expected_task_num_elements(
    workflow_w1,
):
    num_elems = [task.num_elements for task in workflow_w1.tasks]
    workflow_w1.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)],
        propagate_to=[
            ElementPropagation(task=workflow_w1.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    num_elems_new = [task.num_elements for task in workflow_w1.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 3 and num_elems_diff[1] == 3


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_with_propagation_expected_new_data_index(workflow_w1):
    data_index = [sorted(i.data_index.keys()) for i in workflow_w1.elements]
    workflow_w1.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)],
        propagate_to=[
            ElementPropagation(task=workflow_w1.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    data_index_new = [sorted(i.data_index.keys()) for i in workflow_w1.elements]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p2", "resources.any"],
        ["inputs.p2", "resources.any"],
        ["inputs.p2", "resources.any"],
    ]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_with_propagation_into_sequence_expected_workflow_num_elements(
    workflow_w2,
):
    num_elems = workflow_w2.num_elements
    workflow_w2.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w2.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    num_elems_new = workflow_w2.num_elements
    assert num_elems_new - num_elems == 12


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_with_propagation_into_sequence_expected_task_num_elements(
    workflow_w2,
):
    num_elems = [task.num_elements for task in workflow_w2.tasks]
    workflow_w2.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w2.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    num_elems_new = [task.num_elements for task in workflow_w2.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 3 and num_elems_diff[1] == 9


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_with_propagation_into_sequence_expected_new_data_index(
    workflow_w2,
):
    data_index = [sorted(i.data_index.keys()) for i in workflow_w2.elements]
    workflow_w2.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w2.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    data_index_new = [sorted(i.data_index.keys()) for i in workflow_w2.elements]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
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


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_with_default_propagation(workflow_w1):
    workflow_w1_copy = workflow_w1.copy()

    workflow_w1.tasks.t1.add_elements(
        inputs=[InputValue(param_p1, 103)],
        propagate_to=[],
    )

    workflow_w1_copy.tasks.t1.add_elements(
        inputs=[InputValue(param_p1, 103)],
        propagate_to=[ElementPropagation(task=workflow_w1.tasks.t2)],
    )

    assert workflow_w1._persistent_metadata == workflow_w1_copy._persistent_metadata


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_multi_task_dependence_expected_workflow_num_elements(
    workflow_w3, param_p1
):
    num_elems = workflow_w3.num_elements
    workflow_w3.tasks.t1.add_elements(
        inputs=[InputValue(param_p1, 102)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w3.tasks.t2, nesting_order={"inputs.p2": 0, "inputs.p3": 1}
            ),
            ElementPropagation(
                task=workflow_w3.tasks.t3,
                nesting_order={"inputs.p3": 0, "inputs.p4": 1},
            ),
        ],
    )
    num_elems_new = workflow_w3.num_elements
    assert num_elems_new - num_elems == 5


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_multi_task_dependence_expected_task_num_elements(
    workflow_w3, param_p1
):
    num_elems = [task.num_elements for task in workflow_w3.tasks]
    workflow_w3.tasks.t1.add_elements(
        inputs=[InputValue(param_p1, 102)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w3.tasks.t2, nesting_order={"inputs.p2": 0, "inputs.p3": 1}
            ),
            ElementPropagation(
                task=workflow_w3.tasks.t3,
                nesting_order={"inputs.p3": 0, "inputs.p4": 1},
            ),
        ],
    )
    num_elems_new = [task.num_elements for task in workflow_w3.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff == [1, 2, 2]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_multi_task_dependence_expected_new_data_index(
    workflow_w3, param_p1
):
    data_index = [sorted(i.data_index.keys()) for i in workflow_w3.elements]
    workflow_w3.tasks.t1.add_elements(
        inputs=[InputValue(param_p1, 102)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w3.tasks.t2, nesting_order={"inputs.p2": 0, "inputs.p3": 1}
            ),
            ElementPropagation(
                task=workflow_w3.tasks.t3,
                nesting_order={"inputs.p3": 0, "inputs.p4": 1},
            ),
        ],
    )
    data_index_new = [sorted(i.data_index.keys()) for i in workflow_w3.elements]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == (
        [["inputs.p1", "outputs.p3", "resources.any"]] * 1
        + [["inputs.p2", "inputs.p3", "outputs.p4", "resources.any"]] * 2
        + [["inputs.p3", "inputs.p4", "resources.any"]] * 2
    )


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_multi_task_dependence_workflow_num_elements(
    workflow_w3,
):
    num_elems = workflow_w3.num_elements
    workflow_w3.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w3.tasks.t2, nesting_order={"inputs.p2": 0, "inputs.p3": 1}
            ),
            ElementPropagation(
                task=workflow_w3.tasks.t3,
                nesting_order={"inputs.p3": 0, "inputs.p4": 1},
            ),
        ],
    )
    num_elems_new = workflow_w3.num_elements
    assert num_elems_new - num_elems == 27


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_multi_task_dependence_expected_task_num_elements(
    workflow_w3,
):
    num_elems = [task.num_elements for task in workflow_w3.tasks]
    workflow_w3.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w3.tasks.t2, nesting_order={"inputs.p2": 0, "inputs.p3": 1}
            ),
            ElementPropagation(
                task=workflow_w3.tasks.t3,
                nesting_order={"inputs.p3": 0, "inputs.p4": 1},
            ),
        ],
    )
    num_elems_new = [task.num_elements for task in workflow_w3.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff == [3, 6, 18]


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_task_add_elements_sequence_multi_task_dependence_expected_new_data_index(
    workflow_w3,
):
    data_index = [sorted(i.data_index.keys()) for i in workflow_w3.elements]
    workflow_w3.tasks.t1.add_elements(
        sequences=[ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)],
        propagate_to=[
            ElementPropagation(
                task=workflow_w3.tasks.t2, nesting_order={"inputs.p2": 0, "inputs.p3": 1}
            ),
            ElementPropagation(
                task=workflow_w3.tasks.t3,
                nesting_order={"inputs.p3": 0, "inputs.p4": 1},
            ),
        ],
    )
    data_index_new = [sorted(i.data_index.keys()) for i in workflow_w3.elements]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == (
        [["inputs.p1", "outputs.p3", "resources.any"]] * 3
        + [["inputs.p2", "inputs.p3", "outputs.p4", "resources.any"]] * 6
        + [["inputs.p3", "inputs.p4", "resources.any"]] * 18
    )


def test_no_change_to_persistent_metadata_on_add_task_failure(workflow_w4):

    data = copy.deepcopy(workflow_w4.metadata)

    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = Task(schemas=s2)
    with pytest.raises(MissingInputs) as exc_info:
        workflow_w4.add_task(t2)

    assert workflow_w4.metadata == data


def test_no_change_to_parameter_keys_on_add_task_failure(workflow_w4, param_p2, param_p3):

    param_keys = list(workflow_w4._get_workflow_parameter_group().keys())
    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = Task(schemas=s2)
    with pytest.raises(MissingInputs) as exc_info:
        workflow_w4.add_task(t2)

    param_keys_new = list(workflow_w4._get_workflow_parameter_group().keys())

    assert param_keys_new == param_keys


def test_expected_additional_parameter_keys_on_add_task(workflow_w4, param_p2, param_p3):

    param_keys = workflow_w4._get_parameter_keys()
    param_key_max = max(param_keys)

    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = Task(schemas=s2, inputs=[InputValue(param_p3, 301)])
    workflow_w4.add_task(t2)

    param_keys_new = workflow_w4._get_parameter_keys()
    param_keys_diff = set(param_keys_new) - set(param_keys)

    # one new key for resources, one for param_p3 value
    assert param_keys_diff == {param_key_max + 1, param_key_max + 2}


def test_parameters_accepted_on_add_task(workflow_w4, param_p2, param_p3):
    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = Task(schemas=s2, inputs=[InputValue(param_p3, 301)])
    workflow_w4.add_task(t2)
    assert not workflow_w4._get_pending_add_parameter_keys()


def test_parameters_pending_during_add_task(workflow_w4, param_p2, param_p3):
    s2 = make_schemas([[{"p1": None, "p3": None}, ()]])
    t2 = Task(schemas=s2, inputs=[InputValue(param_p3, 301)])
    with workflow_w4.batch_update():
        workflow_w4.add_task(t2)
        assert workflow_w4._get_pending_add_parameter_keys()
