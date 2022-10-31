import pytest

from hpcflow.api import WorkflowTemplate, Workflow
from hpcflow.sdk.core.errors import WorkflowNotFoundError


def test_make_empty_workflow(tmp_path):
    Workflow.from_template(WorkflowTemplate(name="w1"), path=tmp_path)


def test_raise_on_missing_workflow(tmp_path):
    with pytest.raises(WorkflowNotFoundError):
        Workflow(tmp_path)
