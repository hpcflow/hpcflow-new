from importlib import resources
import sys
import pytest
from hpcflow.app import app as hf


def test_workflow_1(tmp_path, null_config):
    package = "hpcflow.sdk.demo.data"
    with resources.path(package=package, resource="workflow_1.yaml") as path:
        wk = hf.Workflow.from_YAML_file(YAML_path=path, path=tmp_path)
    wk.submit(wait=True)
    assert wk.tasks[0].elements[0].outputs.p2.value == "201"
