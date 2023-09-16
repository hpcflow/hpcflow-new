import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.submission.shells import ALL_SHELLS
from hpcflow.sdk.core.test_utils import (
    P1_parameter_cls as P1,
    P1_sub_parameter_cls as P1_sub,
)


def test_get_command_line(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = 1
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value} + 100)"


@pytest.mark.parametrize("shell_args", [("powershell", "nt"), ("bash", "posix")])
def test_get_command_line_with_stdout(null_config, tmp_path, shell_args):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<parameter:p2>>",
                    ),
                ],
            )
        ],
    )
    p1_value = 1
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS[shell_args[0]][shell_args[1]]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    if shell_args == ("powershell", "nt"):
        assert cmd_str == f"$parameter_p2 = Write-Output ({p1_value} + 100)"

    elif shell_args == ("bash", "posix"):
        assert cmd_str == f"parameter_p2=`Write-Output ({p1_value} + 100)`"


def test_get_command_line_single_labelled_input(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"), labels={"one": {}})],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1[one]>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = 1
    tasks = [
        hf.Task(schemas=s1, inputs=[hf.InputValue("p1", label="one", value=p1_value)])
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value} + 100)"


def test_get_command_line_multiple_labelled_input(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p1"), multiple=True, labels={"one": {}, "two": {}}
            )
        ],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1[one]>> + <<parameter:p1[two]>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_one_value = 1
    p1_two_value = 2
    tasks = [
        hf.Task(
            schemas=s1,
            inputs=[
                hf.InputValue("p1", label="one", value=p1_one_value),
                hf.InputValue("p1", label="two", value=p1_two_value),
            ],
        ),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_one_value} + {p1_two_value} + 100)"


def test_get_command_line_sub_parameter(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1.a>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = {"a": 1}
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value['a']} + 100)"


def test_get_command_line_parameter_value(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = P1(a=1)  # has a `CLI_format` method defined which returns `str(a)`
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.a} + 100)"


def test_get_command_line_parameter_value_custom_method(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c.custom_CLI_format()>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = P1(a=1)
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.a + 4} + 100)"


def test_get_command_line_parameter_value_custom_method_with_args(null_config, tmp_path):
    add_val = 35
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command=f"Write-Output (<<parameter:p1c.custom_CLI_format(add={add_val})>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = P1(a=1)
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.a + add_val} + 100)"


def test_get_command_line_parameter_value_custom_method_with_two_args(
    null_config, tmp_path
):
    add_val = 35
    sub_val = 10
    cmd = (
        f"Write-Output ("
        f"<<parameter:p1c.custom_CLI_format(add={add_val}, sub={sub_val})>> + 100)"
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[hf.Action(commands=[hf.Command(command=cmd)])],
    )
    p1_value = P1(a=1)
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.a + add_val - sub_val} + 100)"


def test_get_command_line_parameter_value_sub_object(null_config, tmp_path):
    cmd = f"Write-Output (<<parameter:p1c.sub_param>> + 100)"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[hf.Action(commands=[hf.Command(command=cmd)])],
    )
    p1_value = P1(a=1, sub_param=P1_sub(e=5))
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.sub_param.e} + 100)"


def test_get_command_line_parameter_value_sub_object_attr(null_config, tmp_path):
    cmd = f"Write-Output (" f"<<parameter:p1c.sub_param.e>> + 100)"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[hf.Action(commands=[hf.Command(command=cmd)])],
    )
    p1_value = P1(a=1, sub_param=P1_sub(e=5))
    tasks = [hf.Task(schemas=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.sub_param.e} + 100)"
