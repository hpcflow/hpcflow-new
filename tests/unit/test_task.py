from hpcflow.api import (
    InputSourceType,
    SchemaInput,
    SchemaOutput,
    TaskSchema,
    TaskSourceType,
    hpcflow,
    Task,
    InputValue,
    InputSource,
    InputSourceMode,
)


def test_task_expected_input_source_mode_no_sources():
    t1 = Task(
        schemas=hpcflow.task_schemas.dummy_task_1,
        inputs=[InputValue("p1", value=101)],
    )
    assert t1.input_source_mode == InputSourceMode.AUTO


def test_task_expected_input_source_mode_with_sources():
    t1 = Task(
        schemas=hpcflow.task_schemas.dummy_task_1,
        inputs=[InputValue("p1", value=101)],
        input_sources=[InputSource.local()],
    )
    assert t1.input_source_mode == InputSourceMode.MANUAL


def test_task_get_available_task_input_sources_expected_return_first_task_local_value():

    s1 = TaskSchema("ts1", actions=[], inputs=[SchemaInput("p1")])

    t1 = Task(schemas=s1, inputs=[InputValue("p1", value=101)])

    available = t1.get_available_task_input_sources()
    available_exp = {"p1": [InputSource(source_type=InputSourceType.LOCAL)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_first_task_default_value():

    s1 = TaskSchema("ts1", actions=[], inputs=[SchemaInput("p1", default_value=101)])

    t1 = Task(schemas=s1)

    available = t1.get_available_task_input_sources()
    available_exp = {"p1": [InputSource(source_type=InputSourceType.DEFAULT)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output():

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput("p1")],
        outputs=[SchemaOutput("p2")],
    )
    s2 = TaskSchema("ts2", actions=[], inputs=[SchemaInput("p2")])

    t1 = Task(schemas=s1)
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


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default():

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput("p1")],
        outputs=[SchemaOutput("p2")],
    )
    s2 = TaskSchema("ts2", actions=[], inputs=[SchemaInput("p2", default_value=2002)])

    t1 = Task(schemas=s1)
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


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_local():

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput("p1")],
        outputs=[SchemaOutput("p2")],
    )
    s2 = TaskSchema("ts2", actions=[], inputs=[SchemaInput("p2")])

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s2, inputs=[InputValue("p2", value=202)])

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


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default_and_local():

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput("p1")],
        outputs=[SchemaOutput("p2")],
    )
    s2 = TaskSchema("ts2", actions=[], inputs=[SchemaInput("p2", default_value=2002)])

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s2, inputs=[InputValue("p2", value=202)])

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


def test_task_get_available_task_input_sources_expected_return_one_param_two_outputs():
    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput("p1")],
        outputs=[SchemaOutput("p2"), SchemaOutput("p3")],
    )
    s2 = TaskSchema(
        "ts2",
        actions=[],
        inputs=[SchemaInput("p2")],
        outputs=[SchemaOutput("p3"), SchemaOutput("p4")],
    )
    s3 = TaskSchema("ts3", actions=[], inputs=[SchemaInput("p3")])

    t1 = Task(schemas=s1)
    t2 = Task(schemas=s2)
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


def test_task_get_available_task_input_sources_expected_return_two_params_one_output():

    s1 = TaskSchema(
        "ts1",
        actions=[],
        inputs=[SchemaInput("p1")],
        outputs=[SchemaOutput("p2"), SchemaOutput("p3")],
    )
    s2 = TaskSchema(
        "ts2",
        actions=[],
        inputs=[SchemaInput("p2"), SchemaInput("p3")],
    )

    t1 = Task(schemas=s1)
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
