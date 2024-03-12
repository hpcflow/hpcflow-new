from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow_template


def test_merge_template_level_resources_into_element_set(null_config):
    wkt = hf.WorkflowTemplate(
        name="w1",
        tasks=[hf.Task(schema=[hf.task_schemas.test_t1_ps])],
        resources={"any": {"num_cores": 1}},
    )
    assert wkt.tasks[0].element_sets[0].resources == hf.ResourceList.from_json_like(
        {"any": {"num_cores": 1}}
    )


def test_equivalence_from_YAML_and_JSON_files(null_config):
    wkt_yaml = make_test_data_YAML_workflow_template("workflow_1.yaml")
    wkt_json = make_test_data_YAML_workflow_template("workflow_1.json")
    assert wkt_json == wkt_yaml


def test_reuse(null_config, tmp_path):
    """Test we can re-use a template that has already been made persistent."""
    wkt = hf.WorkflowTemplate(name="test", tasks=[])
    wk1 = hf.Workflow.from_template(wkt, name="test_1", path=tmp_path)
    wk2 = hf.Workflow.from_template(wkt, name="test_2", path=tmp_path)


def test_workflow_template_vars(tmp_path, new_null_config):
    num_repeats = 2
    wkt = make_test_data_YAML_workflow_template(
        workflow_name="benchmark_N_elements.yaml",
        variables={"N": num_repeats},
    )
    assert wkt.tasks[0].element_sets[0].repeats[0]["number"] == num_repeats
