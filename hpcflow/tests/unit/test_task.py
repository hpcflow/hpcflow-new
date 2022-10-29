import pytest
from hpcflow.api import (
    InputSourceType,
    Parameter,
    SchemaInput,
    SchemaOutput,
    TaskSchema,
    TaskSourceType,
    Task,
    InputValue,
    InputSource,
    InputSourceMode,
)


@pytest.fixture
def param_p1():
    return Parameter("p1")


@pytest.fixture
def param_p2():
    return Parameter("p2")


@pytest.fixture
def param_p3():
    return Parameter("p3")


def test_task_expected_input_source_mode_no_sources(param_p1):
    s1 = TaskSchema("ts1", actions=[], inputs=[SchemaInput(param_p1)])
    t1 = Task(
        schemas=s1,
        inputs=[InputValue(param_p1, value=101)],
    )
    assert t1.input_source_mode == InputSourceMode.AUTO


def test_task_expected_input_source_mode_with_sources(param_p1):
    s1 = TaskSchema("ts1", actions=[], inputs=[SchemaInput(param_p1)])
    t1 = Task(
        schemas=s1,
        inputs=[InputValue(param_p1, value=101)],
        input_sources=[InputSource.local()],
    )
    assert t1.input_source_mode == InputSourceMode.MANUAL


def test_task_get_available_task_input_sources_expected_return_first_task_local_value(
    param_p1,
):

    s1 = TaskSchema("ts1", actions=[], inputs=[SchemaInput(param_p1)])
    t1 = Task(schemas=s1, inputs=[InputValue(param_p1, value=101)])

    available = t1.get_available_task_input_sources()
    available_exp = {"p1": [InputSource(source_type=InputSourceType.LOCAL)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_first_task_default_value(
    param_p1,
):

    s1 = TaskSchema("ts1", actions=[], inputs=[SchemaInput(param_p1, default_value=101)])

    t1 = Task(schemas=s1)

    available = t1.get_available_task_input_sources()
    available_exp = {"p1": [InputSource(source_type=InputSourceType.DEFAULT)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output(
    param_p1, param_p2
):

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput(param_p1)],
        outputs=[SchemaOutput(param_p2)],
    )
    s2 = TaskSchema("ts2", actions=[], inputs=[SchemaInput(param_p2)])

    t1 = Task(schemas=s1, insert_ID=0)
    t2 = Task(schemas=s2)

    available = t2.get_available_task_input_sources([t1])
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
    param_p1, param_p2
):

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput(param_p1)],
        outputs=[SchemaOutput(param_p2)],
    )
    s2 = TaskSchema("ts2", actions=[], inputs=[SchemaInput(param_p2, default_value=2002)])

    t1 = Task(schemas=s1, insert_ID=0)
    t2 = Task(schemas=s2)

    available = t2.get_available_task_input_sources([t1])
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
    param_p1, param_p2
):

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput(param_p1)],
        outputs=[SchemaOutput(param_p2)],
    )
    s2 = TaskSchema("ts2", actions=[], inputs=[SchemaInput(param_p2)])

    t1 = Task(schemas=s1, insert_ID=0)
    t2 = Task(schemas=s2, inputs=[InputValue(param_p2, value=202)])

    available = t2.get_available_task_input_sources([t1])
    available_exp = {
        "p2": [
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            ),
            InputSource(source_type=InputSourceType.LOCAL),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default_and_local(
    param_p1, param_p2
):

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput(param_p1)],
        outputs=[SchemaOutput(param_p2)],
    )
    s2 = TaskSchema("ts2", actions=[], inputs=[SchemaInput(param_p2, default_value=2002)])

    t1 = Task(schemas=s1, insert_ID=0)
    t2 = Task(schemas=s2, inputs=[InputValue(param_p2, value=202)])

    available = t2.get_available_task_input_sources([t1])
    available_exp = {
        "p2": [
            InputSource(
                source_type=InputSourceType.TASK,
                task_ref=0,
                task_source_type=TaskSourceType.OUTPUT,
            ),
            InputSource(source_type=InputSourceType.LOCAL),
            InputSource(source_type=InputSourceType.DEFAULT),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_two_outputs(
    param_p1, param_p2, param_p3
):
    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput(param_p1)],
        outputs=[SchemaOutput(param_p2), SchemaOutput(param_p3)],
    )
    s2 = TaskSchema(
        "ts2",
        actions=[],
        inputs=[SchemaInput(param_p2)],
        outputs=[SchemaOutput(param_p3), SchemaOutput("p4")],
    )
    s3 = TaskSchema("ts3", actions=[], inputs=[SchemaInput(param_p3)])

    t1 = Task(schemas=s1, insert_ID=0)
    t2 = Task(schemas=s2, insert_ID=1)
    t3 = Task(schemas=s3)

    available = t3.get_available_task_input_sources([t1, t2])
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

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput(param_p1)],
        outputs=[SchemaOutput(param_p2), SchemaOutput(param_p3)],
    )
    s2 = TaskSchema(
        "ts2",
        actions=[],
        inputs=[SchemaInput(param_p2), SchemaInput(param_p3)],
    )

    t1 = Task(schemas=s1, insert_ID=0)
    t2 = Task(schemas=s2)

    available = t2.get_available_task_input_sources([t1])
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
    s1 = TaskSchema("ts1", actions=[])
    s2 = TaskSchema("ts2", actions=[])

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s2)

    assert Task.get_task_unique_names([t1, t2]) == ["ts1", "ts2"]


def test_get_task_unique_names_two_tasks_with_repeat():

    s1 = TaskSchema("ts1", actions=[])

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s1)

    assert Task.get_task_unique_names([t1, t2]) == ["ts1_1", "ts1_2"]
