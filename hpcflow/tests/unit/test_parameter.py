from dataclasses import dataclass

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.parameters import ParameterValue


@dataclass
class MyParameterP1(ParameterValue):
    _typ = "p1"
    a: int

    def CLI_format(self):
        return str(self.a)


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_submission_with_specified_parameter_class_module(null_config, tmp_path, store):
    """Test we can use a ParameterValue subclass that is defined separately from the main
    code (i.e. not automatically imported on app init)."""

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1")],
        outputs=[hf.SchemaOutput(parameter="p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<parameter:p2>>",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.tests.unit.test_parameter"],
    )
    p1_value = MyParameterP1(a=10)
    t1 = hf.Task(schemas=s1, inputs=[hf.InputValue("p1", value=p1_value)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        path=tmp_path,
        store=store,
    )
    wk.submit(wait=True)
    assert wk.tasks.t1.elements[0].get("outputs.p2") == "110"
