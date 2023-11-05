from importlib import resources
import time

import pytest

from hpcflow.app import app as hf


@pytest.mark.wsl
def test_workflow_1(tmp_path, null_config):
    with resources.path("hpcflow.tests.data", "workflow_1_wsl.yaml") as path:
        wk = hf.Workflow.from_YAML_file(YAML_path=path, path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    time.sleep(20)  # TODO: bug! for some reason the new parameter isn't actually written
    # to disk when using WSL until several seconds after the workflow has finished!
    assert wk.tasks[0].elements[0].outputs.p2.value == "201"
