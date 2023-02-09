import copy
from textwrap import dedent
import pytest

from hpcflow.api import (
    Action,
    ActionEnvironment,
    Command,
    Environment,
    FileSpec,
    InputValue,
    OutputFileParser,
    Parameter,
    Task,
    TaskSchema,
    hpcflow,
    WorkflowTemplate,
    Workflow,
)
from hpcflow.sdk.core.errors import (
    MissingInputs,
    WorkflowBatchUpdateFailedError,
    WorkflowNotFoundError,
)


def modify_workflow_metadata_on_disk(workflow):
    """Make a non-sense change to the on-disk metadata."""
    changed_md = copy.deepcopy(workflow.metadata)
    changed_md["new_key"] = "new_value"
    workflow._get_workflow_root_group(mode="r+").attrs.put(changed_md)


@pytest.fixture
def null_config(tmp_path):
    hpcflow.load_config(config_dir=tmp_path)


@pytest.fixture
def empty_workflow(null_config, tmp_path):
    return Workflow.from_template(WorkflowTemplate(name="w1"), path=tmp_path)


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
        commands=[Command("<<parameter:p2>> <<parameter:p3>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def file_spec_fs1():
    return FileSpec(label="file1", name="file1.txt")


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
def schema_s1(param_p1, act_1):
    return TaskSchema("ts1", actions=[act_1], inputs=[param_p1])


@pytest.fixture
def schema_s2(param_p2, param_p3, act_2):
    return TaskSchema("ts2", actions=[act_2], inputs=[param_p2, param_p3])


@pytest.fixture
def schema_s3(param_p1, param_p2, act_3):
    return TaskSchema("ts1", actions=[act_3], inputs=[param_p1], outputs=[param_p2])


@pytest.fixture
def workflow_w1(null_config, tmp_path, schema_s3, param_p1):
    t1 = Task(schemas=schema_s3, inputs=[InputValue(param_p1, 101)])
    wkt = WorkflowTemplate(name="w1", tasks=[t1])
    return Workflow.from_template(wkt, path=tmp_path)


def test_make_empty_workflow(empty_workflow):
    assert empty_workflow.path is not None


def test_raise_on_missing_workflow(tmp_path):
    with pytest.raises(WorkflowNotFoundError):
        Workflow(tmp_path)


def test_add_empty_task(empty_workflow, schema_s1):
    t1 = Task(schemas=schema_s1)
    wk_t1 = empty_workflow._add_empty_task(t1, parent_events=None)

    assert len(empty_workflow.tasks) == 1 and wk_t1.index == 0 and wk_t1.name == "ts1"


def test_raise_on_missing_inputs_add_first_task(empty_workflow, schema_s1, param_p1):
    t1 = Task(schemas=schema_s1)
    with pytest.raises(MissingInputs) as exc_info:
        empty_workflow.add_task(t1)

    assert exc_info.value.missing_inputs == [param_p1.typ]


def test_raise_on_missing_inputs_add_second_task(workflow_w1, schema_s2, param_p3):
    t2 = Task(schemas=schema_s2)
    with pytest.raises(MissingInputs) as exc_info:
        workflow_w1.add_task(t2)

    assert exc_info.value.missing_inputs == [param_p3.typ]  # p2 comes from existing task


def test_new_workflow_deleted_on_creation_failure():
    pass


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_WorkflowTemplate_from_YAML_string(null_config, param_p1, param_p2):
    s1 = TaskSchema("ts1", actions=[], inputs=[param_p1, param_p2])
    wkt_yml = dedent(
        """
        name: simple_workflow

        tasks:
        - schemas: [ts1]
          element_sets:
            inputs:
              p2: 201
              p5: 501
            sequences:
              - path: inputs.p1
                nesting_order: 0
                values: [101, 102]
    """
    )
    wkt = WorkflowTemplate.from_YAML_string(wkt_yml)


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_WorkflowTemplate_from_YAML_string_without_element_sets(
    null_config, param_p1, param_p2
):
    s1 = TaskSchema("ts1", actions=[], inputs=[param_p1, param_p2])
    wkt_yml = dedent(
        """
        name: simple_workflow

        tasks:
        - schemas: [ts1]
          inputs:
            p2: 201
            p5: 501
          sequences:
            - path: inputs.p1
              nesting_order: 0
              values: [101, 102]
    """
    )
    wkt = WorkflowTemplate.from_YAML_string(wkt_yml)


@pytest.mark.skip(
    reason=(
        "Need to be able to either add app data to the app here, or have support for "
        "built in app data; can't init ValueSequence."
    )
)
def test_WorkflowTemplate_from_YAML_string_with_and_without_element_sets_equivalence(
    null_config, param_p1, param_p2
):
    s1 = TaskSchema("ts1", actions=[], inputs=[param_p1, param_p2])
    wkt_yml_1 = dedent(
        """
        name: simple_workflow

        tasks:
        - schemas: [ts1]
          element_sets:
            inputs:
              p2: 201
              p5: 501
            sequences:
              - path: inputs.p1
                nesting_order: 0
                values: [101, 102]
    """
    )
    wkt_yml_2 = dedent(
        """
        name: simple_workflow

        tasks:
        - schemas: [ts1]
          inputs:
            p2: 201
            p5: 501
          sequences:
            - path: inputs.p1
              nesting_order: 0
              values: [101, 102]
    """
    )
    wkt_1 = WorkflowTemplate.from_YAML_string(wkt_yml_1)
    wkt_2 = WorkflowTemplate.from_YAML_string(wkt_yml_2)
    assert wkt_1 == wkt_2


def test_check_is_modified_during_add_task(workflow_w1, schema_s2, param_p3):
    t2 = Task(schemas=schema_s2, inputs=[InputValue(param_p3, 301)])
    with workflow_w1.batch_update():
        workflow_w1.add_task(t2)
        assert workflow_w1._check_is_modified()


def test_empty_batch_update_does_nothing(workflow_w1):
    with workflow_w1.batch_update():
        assert not workflow_w1._check_is_modified()


def test_check_is_modified_on_disk_when_metadata_changed(workflow_w1):
    modify_workflow_metadata_on_disk(workflow_w1)
    assert workflow_w1._check_is_modified_on_disk()


def test_batch_update_abort_if_modified_on_disk(workflow_w1, schema_s2, param_p3):
    t2 = Task(schemas=schema_s2, inputs=[InputValue(param_p3, 301)])
    with pytest.raises(WorkflowBatchUpdateFailedError):
        with workflow_w1.batch_update():
            workflow_w1.add_task(t2)
            modify_workflow_metadata_on_disk(workflow_w1)
