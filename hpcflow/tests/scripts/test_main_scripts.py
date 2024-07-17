import json
import os
from pathlib import Path
import shutil
import time
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import P1_parameter_cls as P1

# note: when testing the frozen app, we might not have MatFlow installed in the built in
# python_env MatFlow environment, so we should skip these tests.


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_direct_out(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_sub_param_in_direct_out(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_sub_param_in_direct_out.py>>",
                script_data_in={"p1.a": "direct"},
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = {"a": 101}
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val["a"] + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_direct_out_single_label(null_config, tmp_path):
    """This uses the same test script as the `test_script_direct_in_direct_out` test;
    single labels are trivial and need not be referenced in the script."""
    p1_label = "one"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"), labels={p1_label: {}})],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={f"p1[{p1_label}]": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_direct_out_labels(null_config, tmp_path):
    p1_label_1 = "one"
    p1_label_2 = "two"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p1"),
                labels={p1_label_1: {}, p1_label_2: {}},
                multiple=True,
            )
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_labels.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_1_val = 101
    p1_2_val = 201
    t1 = hf.Task(
        schema=s1,
        inputs={
            f"p1[{p1_label_1}]": p1_1_val,
            f"p1[{p1_label_2}]": p1_2_val,
        },
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_1_val + p1_2_val


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_in_json_out(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_json_out.py>>",
                script_data_in="json",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_in_json_out_labels(null_config, tmp_path):
    p1_label_1 = "one"
    p1_label_2 = "two"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p1"),
                labels={p1_label_1: {}, p1_label_2: {}},
                multiple=True,
            )
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_json_out_labels.py>>",
                script_data_in="json",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_1_val = 101
    p1_2_val = 201
    t1 = hf.Task(
        schema=s1,
        inputs={
            f"p1[{p1_label_1}]": p1_1_val,
            f"p1[{p1_label_2}]": p1_2_val,
        },
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_1_val + p1_2_val


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_sub_param_in_json_out_labels(null_config, tmp_path):
    p1_label_1 = "one"
    p1_label_2 = "two"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p1"),
                labels={p1_label_1: {}, p1_label_2: {}},
                multiple=True,
            )
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_sub_param_in_json_out_labels.py>>",
                script_data_in={"p1[one].a": "json", "p1[two]": "json"},
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    a_val = 101
    p1_2_val = 201
    t1 = hf.Task(
        schema=s1,
        inputs={
            f"p1[{p1_label_1}]": {"a": a_val},
            f"p1[{p1_label_2}]": p1_2_val,
        },
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == a_val + p1_2_val


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_and_direct_in_json_out(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("p1")),
            hf.SchemaInput(parameter=hf.Parameter("p2")),
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p3"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_and_direct_in_json_out.py>>",
                script_data_in={"p1": "json", "p2": "direct"},
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    p2_val = 201
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val, "p2": p2_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p3.value == p1_val + p2_val


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_in_json_and_direct_out(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[
            hf.SchemaInput(parameter=hf.Parameter("p2")),
            hf.SchemaOutput(parameter=hf.Parameter("p3")),
        ],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_json_and_direct_out.py>>",
                script_data_in="json",
                script_data_out={"p2": "json", "p3": "direct"},
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val + 100
    assert wk.tasks[0].elements[0].outputs.p3.value == p1_val + 200


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_in_obj(null_config, tmp_path):
    """Use a custom JSON dumper defined in the P1 class."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_obj.py>>",
                script_data_in="json",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    a_val = 1
    t1 = hf.Task(schema=s1, inputs={"p1c": P1(a=a_val)})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == a_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_hdf5_in_obj(null_config, tmp_path):
    """Use a custom HDF5 dumper defined in the P1 class."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_hdf5_in_obj.py>>",
                script_data_in="hdf5",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    a_val = 1
    t1 = hf.Task(schema=s1, inputs={"p1c": P1(a=a_val)})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == a_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_out_obj(null_config, tmp_path):
    """Use a custom JSON saver defined in the P1 class."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_out_obj.py>>",
                script_data_in="direct",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    p1_val = 1
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p1c.value == P1(a=p1_val + 100)


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_hdf5_out_obj(null_config, tmp_path):
    """Use a custom HDF5 saver defined in the P1 class."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_hdf5_out_obj.py>>",
                script_data_in="direct",
                script_data_out="hdf5",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    p1_val = 1
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p1c.value == P1(a=p1_val + 100)


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_pass_env_spec(new_null_config, tmp_path):

    vers_spec = {"version": "1.2"}
    env = hf.Environment(
        name="python_env_with_specifiers",
        specifiers=vers_spec,
        executables=[
            hf.Executable(
                label="python_script",
                instances=[
                    hf.ExecutableInstance(
                        command="python <<script_path>> <<args>>",
                        num_cores=1,
                        parallel_mode=None,
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(env, skip_duplicates=True)

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_env_spec.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                script_pass_env_spec=True,
                environments=[
                    hf.ActionEnvironment(environment="python_env_with_specifiers")
                ],
            )
        ],
    )
    t1 = hf.Task(
        schema=s1,
        inputs={"p1": 101},
        environments={"python_env_with_specifiers": vers_spec},
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="main_script_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == {
        "name": "python_env_with_specifiers",
        **vers_spec,
    }
    hf.reload_template_components()  # remove extra envs


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_std_stream_redirect_on_exception(new_null_config, tmp_path):
    """Test exceptions raised by the app during execution of a script are printed to the
    std-stream redirect file (and not the jobscript's standard error file)."""

    # define a custom python environment which redefines the `WK_PATH` shell variable to
    # a nonsense value so the app cannot load the workflow and thus raises an exception
    app_caps = hf.package_name.upper()
    if os.name == "nt":
        env_cmd = f'$env:{app_caps}_WK_PATH = "nonsense_path"'
    else:
        env_cmd = f'export {app_caps}_WK_PATH="nonsense_path"'

    env_cmd += "; python <<script_path>> <<args>>"
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

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="bad_python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    # jobscript stderr should be empty
    assert not wk.submissions[0].jobscripts[0].direct_stderr_path.read_text()

    # std stream file has workflow not found traceback
    run = wk.get_all_EARs()[0]
    std_stream_path = run.get_app_std_path()
    assert std_stream_path.is_file()
    assert "WorkflowNotFoundError" in std_stream_path.read_text()

    hf.reload_template_components()  # remove extra envs


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_std_out_std_err_not_redirected(null_config, tmp_path):
    """Test that standard error and output streams from a script are written to the jobscript
    standard error and output files."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("stdout_msg")),
            hf.SchemaInput(parameter=hf.Parameter("stderr_msg")),
        ],
        actions=[
            hf.Action(
                script="<<script:main_script_test_std_out_std_err.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    stdout_msg = "hello stdout!"
    stderr_msg = "hello stderr!"
    t1 = hf.Task(schema=s1, inputs={"stdout_msg": stdout_msg, "stderr_msg": stderr_msg})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text()
    std_err = wk.submissions[0].jobscripts[0].direct_stderr_path.read_text()

    assert std_out.strip() == stdout_msg
    assert std_err.strip() == stderr_msg


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_pass_env_spec(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:env_specifier_test/main_script_test_pass_env_spec.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                script_pass_env_spec=True,
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test_pass_env_spec", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text().strip()
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val + 100
    assert std_out == "{'name': 'python_env'}"


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_env_specifier_in_main_script_path(new_null_config, tmp_path):
    py_env = hf.Environment(
        name="python_env",
        specifiers={"version": "v1"},
        executables=[
            hf.Executable(
                label="python_script",
                instances=[
                    hf.ExecutableInstance(
                        command="python <<script_path>> <<args>>",
                        num_cores=1,
                        parallel_mode=None,
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(py_env, skip_duplicates=True)

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:env_specifier_test/<<env:version>>/main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    p1_val = 101
    t1 = hf.Task(
        schema=s1,
        inputs={"p1": p1_val},
        environments={"python_env": {"version": "v1"}},
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test_env_spec_script_path", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val + 100

    hf.reload_template_components()  # remove extra envs


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_env_specifier_in_main_script_path_multiple_scripts(new_null_config, tmp_path):
    """Test two elements with different environment specifiers use two distinct scripts"""
    py_env_v1 = hf.Environment(
        name="python_env",
        specifiers={"version": "v1"},
        executables=[
            hf.Executable(
                label="python_script",
                instances=[
                    hf.ExecutableInstance(
                        command="python <<script_path>> <<args>>",
                        num_cores=1,
                        parallel_mode=None,
                    )
                ],
            )
        ],
    )
    py_env_v2 = hf.Environment(
        name="python_env",
        specifiers={"version": "v2"},
        executables=[
            hf.Executable(
                label="python_script",
                instances=[
                    hf.ExecutableInstance(
                        command="python <<script_path>> <<args>>",
                        num_cores=1,
                        parallel_mode=None,
                    )
                ],
            )
        ],
    )
    hf.envs.add_objects([py_env_v1, py_env_v2], skip_duplicates=True)

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:env_specifier_test/<<env:version>>/main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    p1_val = 101
    t1 = hf.Task(
        schema=s1,
        inputs={"p1": p1_val},
        environments={"python_env": {"version": "v1"}},
        sequences=[
            hf.ValueSequence(
                path="environments.python_env.version",
                values=["v1", "v2"],
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="main_script_test_multiple_env_spec_script",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    # v1 and v2 scripts output different values:
    e1, e2 = wk.tasks.t1.elements
    assert e1.outputs.p2.value == 201
    assert e2.outputs.p2.value == 301

    hf.reload_template_components()  # remove extra envs


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_direct_out_multi_element(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_vals = (101, 102, 103)
    t1 = hf.Task(
        schema=s1, sequences=[hf.ValueSequence(path="inputs.p1", values=p1_vals)]
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test_multi_element", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    assert wk.tasks[0].elements[0].outputs.p2.value == p1_vals[0] + 100
    assert wk.tasks[0].elements[1].outputs.p2.value == p1_vals[1] + 100
    assert wk.tasks[0].elements[2].outputs.p2.value == p1_vals[2] + 100

    # check only one script generated, and its name:
    script_name, _ = t1.schema.actions[0].get_script_artifact_name(env_spec={}, act_idx=0)
    script_files = list(i.name for i in wk.submissions[0].scripts_path.glob("*"))
    assert len(script_files) == 1
    assert script_files[0] == script_name


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_repeated_action_in_schema(null_config, tmp_path):
    # TODO: cannot currently use same Action object multiple times in a schema
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="main_script_test_two_equivalent_actions",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    # check scripts generated for act 0 and 1 have the same contents
    act_0_script, _ = wk.tasks.t1.template.schema.actions[0].get_script_artifact_name(
        env_spec={}, act_idx=0
    )
    act_1_script, _ = wk.tasks.t1.template.schema.actions[1].get_script_artifact_name(
        env_spec={}, act_idx=1
    )
    act_0_script_path = wk.submissions[0].scripts_path / act_0_script
    act_1_script_path = wk.submissions[0].scripts_path / act_1_script
    assert act_0_script_path.read_text() == act_1_script_path.read_text()

    # the two files will be symlinked if not on Windows (may be symlinked on Windows,
    # depending on if user is admin)
    if os.name != "nt":
        assert act_1_script_path.is_symlink()

    # output will be taken from second action
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val + 100


# TODO: same action with different env spec path (v1/v2) in same schema (check contents
# different!). Cannot yet do this because it is not possible to set environment spec
# for diferrent "main" actions within the same task.


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_main_script_two_schemas_same_action(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    t2 = hf.Task(schema=s2, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="main_script_test_two_schemas_same_action",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    # check scripts generated for t1 and t2 have the same contents
    t1_script, _ = wk.tasks.t1.template.schema.actions[0].get_script_artifact_name(
        env_spec={}, act_idx=0
    )
    t2_script, _ = wk.tasks.t2.template.schema.actions[0].get_script_artifact_name(
        env_spec={}, act_idx=0
    )
    t1_script_path = wk.submissions[0].scripts_path / t1_script
    t2_script_path = wk.submissions[0].scripts_path / t2_script
    assert t1_script_path.read_text() == t2_script_path.read_text()

    # the two files will be symlinked if not on Windows (may be symlinked on Windows,
    # depending on if user is admin)
    if os.name != "nt":
        assert t2_script_path.is_symlink()

    # check output
    assert wk.tasks[0].elements[0].outputs.p2.value == p1_val + 100
    assert wk.tasks[1].elements[0].outputs.p2.value == p1_val + 100

    # now copy the workflow elsewhere and check the symlink between the scripts still
    # works:
    wk_path = Path(wk.path)
    copy_path = wk_path.parent.joinpath(wk_path.with_suffix(".copy"))
    shutil.copytree(wk.path, copy_path, symlinks=True)
    t2_script_path_copy = Path(str(t2_script_path).replace(wk.path, f"{wk.path}.copy"))
    assert t1_script_path.read_text() == t2_script_path_copy.read_text()


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_main_script_two_actions_same_schema(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
            hf.Action(
                script="<<script:main_script_test_json_in_json_out.py>>",
                script_data_in="json",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="main_script_test_distinct_actions_same_schema",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    # check scripts generated for act 0 and 1 have different contents
    act_0_script, _ = wk.tasks.t1.template.schema.actions[0].get_script_artifact_name(
        env_spec={}, act_idx=0
    )
    act_1_script, _ = wk.tasks.t1.template.schema.actions[1].get_script_artifact_name(
        env_spec={}, act_idx=1
    )
    act_0_script_path = wk.submissions[0].scripts_path / act_0_script
    act_1_script_path = wk.submissions[0].scripts_path / act_1_script
    assert act_0_script_path.read_text() != act_1_script_path.read_text()


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_shell_env_vars(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_shell_env_vars.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    tasks = [
        hf.Task(
            schema=s1,
            inputs={"p1": 1},
            repeats=3,
        ),
        hf.Task(
            schema=s1,
            inputs={"p1": 1},
        ),
        hf.Task(
            schema=s1,
            inputs={"p1": 1},
            repeats=2,
        ),
    ]
    loops = [
        hf.Loop(
            tasks=[2],
            num_iterations=2,
        )
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        loops=loops,
        template_name="main_script_test_shell_env",
        path=tmp_path,
    )
    wk.add_submission(tasks=[0, 1])
    wk.submit(wait=True, add_to_known=False, status=False)  # first submission
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    wk.submit(wait=True, add_to_known=False, status=False)  # outstanding runs
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)

    for run in wk.get_all_EARs():
        run_dir = run.get_directory()
        with run_dir.joinpath("env_vars.json").open("rt") as fp:
            env_dat = json.load(fp)

        assert env_dat["HPCFLOW_WK_PATH"] == str(run.workflow.path)
        assert env_dat["HPCFLOW_WK_PATH_ARG"] == str(run.workflow.path)

        for js in wk.submissions[run.submission_idx].jobscripts:
            js_funcs_path = str(js.jobscript_functions_path)
            for block in js.blocks:
                for run_i in block.all_EARs:
                    if run_i.id_ == run.id_:
                        assert int(env_dat["HPCFLOW_JS_IDX"]) == js.index
                        assert env_dat["HPCFLOW_JS_FUNCS_PATH"] == js_funcs_path

        assert int(env_dat["HPCFLOW_RUN_ID"]) == run.id_
        assert int(env_dat["HPCFLOW_RUN_IDX"]) == run.index
        assert int(env_dat["HPCFLOW_RUN_PORT"]) == run.port_number

        script_name = run.get_script_artifact_name()
        sub_scripts_dir = wk.submissions[run.submission_idx].scripts_path
        script_path = sub_scripts_dir.joinpath(script_name)

        assert env_dat["HPCFLOW_SUB_SCRIPTS_DIR"] == str(sub_scripts_dir)
        assert int(env_dat["HPCFLOW_SUB_IDX"]) == run.submission_idx

        assert env_dat["HPCFLOW_RUN_SCRIPT_DIR"] == str(script_path.parent)
        assert env_dat["HPCFLOW_RUN_SCRIPT_PATH"] == str(script_path)
        assert env_dat["HPCFLOW_RUN_SCRIPT_NAME"] == script_name
        assert env_dat["HPCFLOW_RUN_SCRIPT_NAME_NO_EXT"] == script_path.stem

        assert env_dat["HPCFLOW_RUN_STD_PATH"] == str(run.get_app_std_path())
        assert (
            env_dat["HPCFLOW_RUN_LOG_PATH"]
            == env_dat["HPCFLOW_LOG_PATH"]
            == str(run.get_log_path())
        )

        assert env_dat["HPCFLOW_ELEMENT_ID"] == str(run.element.id_)
        assert env_dat["HPCFLOW_ELEMENT_IDX"] == str(run.element.index)

        assert env_dat["HPCFLOW_ELEMENT_ITER_ID"] == str(run.element_iteration.id_)
        assert env_dat["HPCFLOW_ELEMENT_ITER_IDX"] == str(run.element_iteration.index)
