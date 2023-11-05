from importlib import resources
import pytest
from hpcflow.app import app as hf


@pytest.mark.slurm
def test_workflow_1(tmp_path, null_config):
    hf.config.add_scheduler("slurm")
    with resources.path("hpcflow.tests.data", "workflow_1_slurm.yaml") as path:
        wk = hf.Workflow.from_YAML_file(YAML_path=path, path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    assert wk.tasks[0].elements[0].outputs.p2.value == "201"
