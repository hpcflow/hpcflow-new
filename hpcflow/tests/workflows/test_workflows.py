from importlib import resources
import sys
import time
import pytest
from hpcflow.app import app as hf


def test_workflow_1(tmp_path, new_null_config):
    package = "hpcflow.sdk.demo.data"
    with resources.path(package=package, resource="workflow_1.yaml") as path:
        wk = hf.Workflow.from_YAML_file(YAML_path=path, path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    assert wk.tasks[0].elements[0].outputs.p2.value == "201"


def test_workflow_1_with_working_dir_with_spaces(tmp_path, new_null_config):
    workflow_dir = tmp_path / "sub path with spaces"
    workflow_dir.mkdir()
    package = "hpcflow.sdk.demo.data"
    with resources.path(package=package, resource="workflow_1.yaml") as path:
        wk = hf.Workflow.from_YAML_file(YAML_path=path, path=workflow_dir)
    wk.submit(wait=True, add_to_known=False)
    assert wk.tasks[0].elements[0].outputs.p2.value == "201"


def test_run_abort(tmp_path, new_null_config):
    package = "hpcflow.sdk.demo.data"
    with resources.path(package=package, resource="workflow_test_run_abort.yaml") as path:
        wk = hf.Workflow.from_YAML_file(YAML_path=path, path=tmp_path)
    wk.submit(add_to_known=False)

    # wait for the run to start;
    # TODO: instead of this: we should add a `wait_to_start=RUN_ID` method to submit()
    max_wait_iter = 15
    aborted = False
    for _ in range(max_wait_iter):
        time.sleep(4)
        try:
            wk.abort_run()  # single task and element so no need to disambiguate
        except ValueError:
            continue
        else:
            aborted = True
            break
    if not aborted:
        raise RuntimeError("Could not abort the run")

    wk.wait()
    assert wk.tasks[0].outputs.is_finished[0].value == "true"
