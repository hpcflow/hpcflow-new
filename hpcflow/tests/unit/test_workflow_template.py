from importlib import resources
import pytest

from hpcflow.app import app as hf


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
    package = "hpcflow.tests.data"
    with resources.path(package=package, resource="workflow_1.yaml") as path:
        wkt_yaml = hf.WorkflowTemplate.from_file(path)

    with resources.path(package=package, resource="workflow_1.json") as path:
        wkt_json = hf.WorkflowTemplate.from_file(path)

    assert wkt_json == wkt_yaml


def test_reuse(null_config, tmp_path):
    """Test we can re-use a template that has already been made persistent."""
    wkt = hf.WorkflowTemplate(name="test", tasks=[])
    wk1 = hf.Workflow.from_template(wkt, name="test_1", path=tmp_path)
    wk2 = hf.Workflow.from_template(wkt, name="test_2", path=tmp_path)
