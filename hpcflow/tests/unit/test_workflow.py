import pytest

from hpcflow.api import hpcflow, WorkflowTemplate, Workflow
from hpcflow.sdk.core.errors import WorkflowNotFoundError


@pytest.fixture
def null_config(tmp_path):
    hpcflow.load_config(config_dir=tmp_path)


def test_make_empty_workflow(null_config, tmp_path):
    Workflow.from_template(WorkflowTemplate(name="w1"), path=tmp_path)


def test_raise_on_missing_workflow(tmp_path):
    with pytest.raises(WorkflowNotFoundError):
        Workflow(tmp_path)
