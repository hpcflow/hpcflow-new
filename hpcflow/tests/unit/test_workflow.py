import copy
from textwrap import dedent
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    MissingInputs,
    WorkflowBatchUpdateFailedError,
    WorkflowNotFoundError,
)
from hpcflow.sdk.core.test_utils import make_workflow


def modify_workflow_metadata_on_disk(workflow):
    """Make a non-sense change to the on-disk metadata."""
    assert workflow.store_format == "zarr"
    wk_md = workflow._store.load_metadata()
    changed_md = copy.deepcopy(wk_md)
    changed_md["new_key"] = "new_value"
    workflow._store._get_root_group(mode="r+").attrs.put(changed_md)


def make_workflow_w1_with_config_kwargs(config_kwargs, path, param_p1, param_p2):
    hf.load_config(**config_kwargs)
    s1 = hf.TaskSchema("ts1", actions=[], inputs=[param_p1], outputs=[param_p2])
    t1 = hf.Task(schemas=s1, inputs=[hf.InputValue(param_p1, 101)])
    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1])
    return hf.Workflow.from_template(wkt, path=path)


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@pytest.fixture
def empty_workflow(null_config, tmp_path):
    return hf.Workflow.from_template(hf.WorkflowTemplate(name="w1"), path=tmp_path)


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
        commands=[hf.Command("<<parameter:p2>> <<parameter:p3>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def file_spec_fs1():
    return hf.FileSpec(label="file1", name="file1.txt")


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
def schema_s1(param_p1, act_1):
    return hf.TaskSchema("ts1", actions=[act_1], inputs=[param_p1])


@pytest.fixture
def schema_s2(param_p2, param_p3, act_2):
    return hf.TaskSchema("ts2", actions=[act_2], inputs=[param_p2, param_p3])


@pytest.fixture
def schema_s3(param_p1, param_p2, act_3):
    return hf.TaskSchema("ts1", actions=[act_3], inputs=[param_p1], outputs=[param_p2])


@pytest.fixture
def workflow_w1(null_config, tmp_path, schema_s3, param_p1):
    t1 = hf.Task(schemas=schema_s3, inputs=[hf.InputValue(param_p1, 101)])
    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1])
    return hf.Workflow.from_template(wkt, path=tmp_path)


def test_make_empty_workflow(empty_workflow):
    assert empty_workflow.path is not None


def test_raise_on_missing_workflow(tmp_path):
    with pytest.raises(WorkflowNotFoundError):
        hf.Workflow(tmp_path)


def test_add_empty_task(empty_workflow, schema_s1):
    t1 = hf.Task(schemas=schema_s1)
    wk_t1 = empty_workflow._add_empty_task(t1)
    assert len(empty_workflow.tasks) == 1 and wk_t1.index == 0 and wk_t1.name == "ts1"


def test_raise_on_missing_inputs_add_first_task(empty_workflow, schema_s1, param_p1):
    t1 = hf.Task(schemas=schema_s1)
    with pytest.raises(MissingInputs) as exc_info:
        empty_workflow.add_task(t1)

    assert exc_info.value.missing_inputs == [param_p1.typ]


def test_raise_on_missing_inputs_add_second_task(workflow_w1, schema_s2, param_p3):
    t2 = hf.Task(schemas=schema_s2)
    with pytest.raises(MissingInputs) as exc_info:
        workflow_w1.add_task(t2)

    assert exc_info.value.missing_inputs == [param_p3.typ]  # p2 comes from existing task


@pytest.mark.skip(reason="TODO: Not implemented.")
def test_new_workflow_deleted_on_creation_failure():
    pass


def test_WorkflowTemplate_from_YAML_string(null_config):
    wkt_yml = dedent(
        """
        name: simple_workflow

        tasks:
        - schemas: [dummy_task_1]
          element_sets:
          - inputs:
              p2: 201
              p5: 501
            sequences:
              - path: inputs.p1
                nesting_order: 0
                values: [101, 102]
    """
    )
    hf.WorkflowTemplate.from_YAML_string(wkt_yml)


def test_WorkflowTemplate_from_YAML_string_without_element_sets(null_config):
    wkt_yml = dedent(
        """
        name: simple_workflow

        tasks:
        - schemas: [dummy_task_1]
          inputs:
            p2: 201
            p5: 501
          sequences:
            - path: inputs.p1
              nesting_order: 0
              values: [101, 102]
    """
    )
    hf.WorkflowTemplate.from_YAML_string(wkt_yml)


def test_WorkflowTemplate_from_YAML_string_with_and_without_element_sets_equivalence(
    null_config,
):
    wkt_yml_1 = dedent(
        """
        name: simple_workflow

        tasks:
        - schemas: [dummy_task_1]
          element_sets:
            - inputs:
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
        - schemas: [dummy_task_1]
          inputs:
            p2: 201
            p5: 501
          sequences:
            - path: inputs.p1
              nesting_order: 0
              values: [101, 102]
    """
    )
    wkt_1 = hf.WorkflowTemplate.from_YAML_string(wkt_yml_1)
    wkt_2 = hf.WorkflowTemplate.from_YAML_string(wkt_yml_2)
    assert wkt_1 == wkt_2


def test_store_has_pending_during_add_task(workflow_w1, schema_s2, param_p3):
    t2 = hf.Task(schemas=schema_s2, inputs=[hf.InputValue(param_p3, 301)])
    with workflow_w1.batch_update():
        workflow_w1.add_task(t2)
        assert workflow_w1._store.has_pending


def test_empty_batch_update_does_nothing(workflow_w1):
    with workflow_w1.batch_update():
        assert not workflow_w1._store.has_pending


@pytest.mark.skip("need to re-implement `is_modified_on_disk`")
def test_is_modified_on_disk_when_metadata_changed(workflow_w1):
    # this is ZarrPersistentStore-specific; might want to consider a refactor later
    with workflow_w1._store.cached_load():
        modify_workflow_metadata_on_disk(workflow_w1)
        assert workflow_w1._store.is_modified_on_disk()


@pytest.mark.skip("need to re-implement `is_modified_on_disk`")
def test_batch_update_abort_if_modified_on_disk(workflow_w1, schema_s2, param_p3):
    t2 = hf.Task(schemas=schema_s2, inputs=[hf.InputValue(param_p3, 301)])
    with pytest.raises(WorkflowBatchUpdateFailedError):
        with workflow_w1._store.cached_load():
            with workflow_w1.batch_update():
                workflow_w1.add_task(t2)
                modify_workflow_metadata_on_disk(workflow_w1)


def test_closest_task_input_source_chosen(tmp_path):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p1",), "t1"],
            [{"p1": None}, ("p1",), "t2"],
            [{"p1": None}, ("p1",), "t3"],
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    assert wk.tasks.t3.get_task_dependencies(as_objects=True) == [wk.tasks.t2]


def test_WorkflowTemplate_from_JSON_string_without_element_sets(null_config):
    wkt_json = dedent(
        """
        {
            "name": "test_wk",
            "tasks": [
                {
                    "schemas": [
                        "test_t1_bash"
                    ],
                    "inputs": {
                        "p1": 101
                    }
                }
            ]
        }
    """
    )
    hf.WorkflowTemplate.from_JSON_string(wkt_json)
