"""Tests concerning the directory structure of a created or submitted workflow"""

import os
from pathlib import Path
import pytest

from hpcflow.sdk.core.test_utils import (
    make_test_data_YAML_workflow,
    make_workflow_to_run_command,
)


@pytest.mark.integration
def test_std_stream_file_not_created(tmp_path, new_null_config):
    """Normally, the app standard stream file should not be written."""
    wk = make_test_data_YAML_workflow("workflow_1.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    run_dir = Path(wk.path).joinpath("execute/task_0_test_t1_conditional_OS/e_0/r_0")
    assert run_dir.is_dir()
    assert not run_dir.joinpath("hpcflow_std.txt").is_file()


@pytest.mark.integration
def test_std_stream_file_created_on_exception_raised(tmp_path, new_null_config):
    command = 'wkflow_app --std-stream "$STD_STREAM_FILE" internal noop --raise'
    wk = make_workflow_to_run_command(command=command, path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    run_dir = Path(wk.path).joinpath("execute/task_0_run_command/e_0/r_0")
    assert run_dir.is_dir()
    assert run_dir.joinpath("hpcflow_std.txt").is_file()
    assert (
        "ValueError: internal noop raised!"
        in run_dir.joinpath("hpcflow_std.txt").read_text()
    )
