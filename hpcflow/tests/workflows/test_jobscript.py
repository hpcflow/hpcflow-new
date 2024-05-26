import os
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core import SKIPPED_EXIT_CODE


@pytest.mark.integration
@pytest.mark.parametrize("exit_code", [0, 1, 98, -1, -123124])
def test_action_exit_code_parsing(null_config, tmp_path, exit_code):
    act = hf.Action(commands=[hf.Command(command=f"exit {exit_code}")])
    s1 = hf.TaskSchema(
        objective="t1",
        actions=[act],
    )
    t1 = hf.Task(schema=[s1])
    wk = hf.Workflow.from_template_data(tasks=[t1], template_name="test", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    recorded_exit = wk.get_EARs_from_IDs([0])[0].exit_code
    if os.name == "posix":
        # exit code from bash wraps around:
        exit_code %= 256
    assert recorded_exit == exit_code


@pytest.mark.integration
def test_skipped_action_same_element(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2"), hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command=f"echo <<parameter:p1>>", stdout="<<parameter:p2>>"
                    ),
                    hf.Command(command=f"exit 1"),
                ],
            ),
            hf.Action(  # should be skipped
                commands=[
                    hf.Command(
                        command=f"echo <<parameter:p2>>", stdout="<<parameter:p3>>"
                    ),
                    hf.Command(command=f"exit 0"),  # exit code should be ignored
                ],
            ),
        ],
    )
    t1 = hf.Task(schema=s1, inputs={"p1": 101})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="test_skip", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_EARs_from_IDs([0, 1])
    exit_codes = [i.exit_code for i in runs]
    is_skipped = [i.skip for i in runs]

    assert exit_codes == [1, SKIPPED_EXIT_CODE]
    assert is_skipped == [0, 1]
