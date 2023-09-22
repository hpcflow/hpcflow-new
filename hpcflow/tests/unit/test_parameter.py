from dataclasses import dataclass

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.parameters import ParameterValue


@dataclass
class MyParameterP1(ParameterValue):
    _typ = "p1_test"
    a: int

    def CLI_format(self):
        return str(self.a)


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_submission_with_specified_parameter_class_module(null_config, tmp_path, store):
    """Test we can use a ParameterValue subclass that is defined separately from the main
    code (i.e. not automatically imported on app init)."""

    # not sure why this is necessary; without this we end up with two `p1_test`
    # parameters, despite `skip_duplicates=True`:
    hf.reload_template_components()

    hf.parameters.add_object(hf.Parameter("p1_test"), skip_duplicates=True)
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1_test")],
        outputs=[hf.SchemaOutput(parameter="p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1_test>> + 100)",
                        stdout="<<parameter:p2>>",
                    )
                ],
                rules=[
                    hf.ActionRule(
                        rule=hf.Rule(
                            path="resources.os_name",
                            condition={"value.equal_to": "nt"},
                        )
                    )
                ],
            ),
            hf.Action(
                commands=[
                    hf.Command(
                        command='echo "$((<<parameter:p1_test>> + 100))"',
                        stdout="<<parameter:p2>>",
                    )
                ],
                rules=[
                    hf.ActionRule(
                        rule=hf.Rule(
                            path="resources.os_name",
                            condition={"value.equal_to": "posix"},
                        )
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.tests.unit.test_parameter"],
    )
    p1_value = MyParameterP1(a=10)
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue("p1_test", value=p1_value)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        path=tmp_path,
        store=store,
    )
    wk.submit(wait=True)
    assert wk.tasks.t1.elements[0].get("outputs.p2") == "110"
