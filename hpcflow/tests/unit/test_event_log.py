import copy
import pytest
from hpcflow.api import (
    Action,
    ActionEnvironment,
    Command,
    Environment,
    FileSpec,
    InputValue,
    OutputFileParser,
    ValueSequence,
    hpcflow,
    Parameter,
    TaskSchema,
    Task,
    WorkflowTemplate,
    Workflow,
)
from hpcflow.sdk.core.errors import MissingInputs


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
def file_spec_fs1():
    return FileSpec(label="file1", name="file1.txt")


@pytest.fixture
def env_1():
    return Environment(name="env_1")


@pytest.fixture
def act_env_1(env_1):
    return ActionEnvironment(env_1)


@pytest.fixture
def act_2(act_env_1):
    return Action(
        commands=[Command("<<parameter:p2>> <<parameter:p3>>")],
        environments=[act_env_1],
    )


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
def schema_s2(param_p2, param_p3, act_2):
    return TaskSchema("ts2", actions=[act_2], inputs=[param_p2, param_p3])


@pytest.fixture
def workflow_w1(null_config, tmp_path, schema_s3, param_p1):
    t1 = Task(schemas=schema_s3, inputs=[InputValue(param_p1, 101)])
    wkt = WorkflowTemplate(name="w1", tasks=[t1])
    return Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def empty_workflow(null_config, tmp_path):
    return Workflow.from_template(WorkflowTemplate(name="w1"), path=tmp_path)


def test_event_create_workflow_num_events(empty_workflow):
    # one for create workflow, one for batch update
    assert len(empty_workflow.event_log.get_events()) == 1


def test_event_create_workflow_event_type(empty_workflow):
    assert empty_workflow.event_log.get_events()[0].event_type == "create_workflow"


def test_event_create_workflow_current_machine(empty_workflow):
    assert empty_workflow.event_log.get_events()[0].machine == hpcflow.config.machine


def test_no_change_to_event_log_on_add_task_failure(workflow_w1, schema_s2):

    event_log = copy.deepcopy(workflow_w1.event_log)

    t2 = Task(schemas=schema_s2)
    with pytest.raises(MissingInputs) as exc_info:
        workflow_w1.add_task(t2)

    assert workflow_w1.event_log == event_log


def test_event_log_inequality_after_add_element(workflow_w1, param_p2, param_p3):
    pass


def test_event_log_expected_events_on_create_workflow(workflow_w1, param_p2, param_p3):
    pass


def test_event_log_expected_events_on_add_task(workflow_w1, param_p2, param_p3):
    pass


def test_event_log_expected_events_on_add_elements(workflow_w1, param_p2, param_p3):
    pass


def test_event_log_expected_events_after_bad_then_good_add_elements(
    workflow_w1, param_p2, param_p3
):
    pass
