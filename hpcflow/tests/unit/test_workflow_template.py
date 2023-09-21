from importlib import resources
import pytest

from hpcflow.app import app as hf


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


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
    package = "hpcflow.sdk.demo.data"
    with resources.path(package=package, resource="workflow_1.yaml") as path:
        wkt_yaml = hf.WorkflowTemplate.from_file(path)

    with resources.path(package=package, resource="workflow_1.json") as path:
        wkt_json = hf.WorkflowTemplate.from_file(path)

    assert wkt_json == wkt_yaml
