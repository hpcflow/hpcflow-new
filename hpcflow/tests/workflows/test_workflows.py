from importlib import resources
import os
import sys
import time
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import (
    P1_parameter_cls as P1,
    P1_sub_parameter_cls as P1_sub,
)


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


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_multi_command_action_stdout_parsing(null_config, tmp_path, store):
    if os.name == "nt":
        cmds = [
            "Write-Output (<<parameter:p1>> + 100)",
            "Write-Output (<<parameter:p1>> + 200)",
        ]
    else:
        cmds = [
            'echo "$((<<parameter:p1>> + 100))"',
            'echo "$((<<parameter:p1>> + 200))"',
        ]
    act = hf.Action(
        commands=[
            hf.Command(
                command=cmds[0],
                stdout="<<int(parameter:p2)>>",
            ),
            hf.Command(
                command=cmds[1],
                stdout="<<float(parameter:p3)>>",
            ),
        ]
    )
    s1 = hf.TaskSchema(
        objective="t1",
        actions=[act],
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2"), hf.SchemaOutput("p3")],
    )
    t1 = hf.Task(schemas=[s1], inputs=[hf.InputValue("p1", 1)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="wk2",
        path=tmp_path,
        store=store,
    )
    wk.submit(wait=True)
    assert wk.tasks.t1.elements[0].get("outputs") == {"p2": 101, "p3": 201.0}


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_element_get_group(null_config, tmp_path, store):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1c")],
        outputs=[hf.SchemaOutput(parameter="p1c")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c>> + 100)",
                        stdout="<<parameter:p1c.CLI_parse()>>",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"), group="my_group")],
    )

    t1 = hf.Task(
        schemas=s1,
        inputs=[hf.InputValue("p1c", value=P1(a=10, sub_param=P1_sub(e=5)))],
        sequences=[hf.ValueSequence("inputs.p1c.a", values=[20, 30], nesting_order=0)],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(
        schemas=s2,
        nesting_order={"inputs.p1c": 0},
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        path=tmp_path,
        store=store,
    )
    wk.submit(wait=True)
    assert wk.tasks.t2.num_elements == 1
    assert wk.tasks.t2.elements[0].get("inputs.p1c") == [P1(a=120), P1(a=130)]
