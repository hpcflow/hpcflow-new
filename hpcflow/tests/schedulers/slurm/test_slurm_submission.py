import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow


@pytest.mark.slurm
def test_workflow_1(tmp_path, null_config):
    hf.config.add_scheduler("slurm")
    wk = make_test_data_YAML_workflow("workflow_1_slurm.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    assert wk.tasks[0].elements[0].outputs.p2.value == "201"
