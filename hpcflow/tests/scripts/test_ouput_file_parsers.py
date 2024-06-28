import os
import time

import pytest
from hpcflow.app import app as hf


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_output_file_parser_parses_file(null_config, tmp_path):
    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                script="<<script:output_file_parser_basic.py>>",
            ),
        ],
        environments=[hf.ActionEnvironment(environment="python_env")],
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
    )

    p1_val = 101
    p2_val_expected = p1_val + 100
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="output_file_parser_test",
        path=tmp_path,
    )

    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    # check the command successfully generated the output file:
    out_file_path = wk.execution_path / f"task_0_t1/e_0/r_0/{out_file.name.name}"
    out_file_contents = out_file_path.read_text()
    assert out_file_contents.strip() == str(p2_val_expected)

    # check the output is parsed correctly:
    assert wk.tasks[0].elements[0].outputs.p2.value == p2_val_expected


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_OFP_std_stream_redirect_on_exception(new_null_config, tmp_path):
    """Test exceptions raised by the app during execution of an OFP script are printed to the
    std-stream redirect file (and not the jobscript's standard error file)."""

    # define a custom python environment which redefines the `WK_PATH` shell variable to
    # a nonsense value so the app cannot load the workflow and thus raises an exception

    app_caps = hf.package_name.upper()
    if os.name == "nt":
        env_cmd = f'$env:{app_caps}_WK_PATH = "nonsense_path"'
    else:
        env_cmd = f'export {app_caps}_WK_PATH="nonsense_path"'

    env_cmd += "; python <<script_name>> <<args>>"
    bad_env = hf.Environment(
        name="bad_python_env",
        executables=[
            hf.Executable(
                label="python_script",
                instances=[
                    hf.ExecutableInstance(
                        command=env_cmd,
                        num_cores=1,
                        parallel_mode=None,
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(bad_env, skip_duplicates=True)

    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                script="<<script:output_file_parser_basic.py>>",
            ),
        ],
        environments=[hf.ActionEnvironment(environment="bad_python_env")],
    )

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
    )

    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="output_file_parser_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    # jobscript stderr should be empty
    assert not wk.submissions[0].jobscripts[0].direct_stderr_path.read_text()

    # std stream file has workflow not found traceback
    run = wk.get_all_EARs()[1]
    std_stream_path = run.get_std_path()
    assert std_stream_path.is_file()
    assert "WorkflowNotFoundError" in std_stream_path.read_text()


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_OFP_std_out_std_err_not_redirected(null_config, tmp_path):
    """Test that standard error and output streams from an OFP script are written to the jobscript
    standard error and output files."""
    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                inputs=["p1"],
                script="<<script:output_file_parser_test_stdout_stderr.py>>",
            ),
        ],
        environments=[hf.ActionEnvironment(environment="python_env")],
    )

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
    )
    p1_val = 101
    stdout_msg = str(p1_val)
    stderr_msg = str(p1_val)
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="ouput_file_parser_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text()
    std_err = wk.submissions[0].jobscripts[0].direct_stderr_path.read_text()

    assert std_out.strip() == stdout_msg
    assert std_err.strip() == stderr_msg
