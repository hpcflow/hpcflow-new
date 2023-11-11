import time

import pytest

from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow


@pytest.mark.wsl
def test_workflow_1(tmp_path, null_config):
    wk = make_test_data_YAML_workflow("workflow_1_wsl.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    time.sleep(20)  # TODO: bug! for some reason the new parameter isn't actually written
    # to disk when using WSL until several seconds after the workflow has finished!
    assert wk.tasks[0].elements[0].outputs.p2.value == "201"
