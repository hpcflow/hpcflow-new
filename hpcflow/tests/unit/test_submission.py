from datetime import timedelta

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    MissingEnvironmentError,
    MissingEnvironmentExecutableError,
)
from hpcflow.sdk.submission.jobscript import group_resource_map_into_jobscripts
from hpcflow.sdk.submission.submission import timedelta_format, timedelta_parse


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


def test_group_resource_map_into_jobscripts(null_config):
    # x-axis corresponds to elements; y-axis corresponds to actions:
    examples = (
        {
            "resources": [
                [1, 1, 1, 2, -1, 2, 4, -1, 1],
                [1, 3, 1, 2, 2, 2, 4, 4, 1],
                [1, 1, 3, 2, 2, 2, 4, -1, 1],
            ],
            "expected": [
                {
                    "resources": 1,
                    "elements": {0: [0, 1, 2], 1: [0], 2: [0, 1], 8: [0, 1, 2]},
                },
                {"resources": 2, "elements": {3: [0, 1, 2], 4: [1, 2], 5: [0, 1, 2]}},
                {"resources": 4, "elements": {6: [0, 1, 2], 7: [1]}},
                {"resources": 3, "elements": {1: [1]}},
                {"resources": 1, "elements": {1: [2]}},
                {"resources": 3, "elements": {2: [2]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [8, 8, 1],
                [4, 4, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 2]}},
                {"resources": 8, "elements": {0: [1], 1: [1]}},
                {"resources": 4, "elements": {0: [2], 1: [2]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [2, 2, 1],
                [4, 4, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0, 1], 1: [0, 1]}},
                {"resources": 1, "elements": {2: [1, 2]}},
                {"resources": 4, "elements": {0: [2], 1: [2]}},
            ],
        },
        {
            "resources": [
                [2, 1, 2],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "elements": {1: [0, 1, 2]}},
                {"resources": 2, "elements": {0: [0], 2: [0]}},
                {"resources": 1, "elements": {0: [1, 2], 2: [1, 2]}},
            ],
        },
        {
            "resources": [
                [2, -1, 2],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 2: [0]}},
                {"resources": 1, "elements": {0: [1, 2], 1: [1, 2], 2: [1, 2]}},
            ],
        },
        {
            "resources": [
                [1, 1],
                [1, 1],
                [1, 1],
            ],
            "expected": [{"resources": 1, "elements": {0: [0, 1, 2], 1: [0, 1, 2]}}],
        },
        {
            "resources": [
                [1, 1, 1],
                [1, 1, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "elements": {0: [0, 1, 2], 1: [0, 1, 2], 2: [0, 2]}}
            ],
        },
        {
            "resources": [
                [1, 1, -1],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "elements": {0: [0, 1, 2], 1: [0, 1, 2], 2: [1, 2]}}
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 4, -1],
                [2, 2, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 3]}},
                {"resources": 4, "elements": {0: [1, 2], 1: [1, 2]}},
                {"resources": 2, "elements": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 4, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 3]}},
                {"resources": 4, "elements": {0: [1, 2], 1: [1, 2]}},
                {"resources": 1, "elements": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 8, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 3]}},
                {"resources": 4, "elements": {0: [1, 2], 1: [1]}},
                {"resources": 8, "elements": {1: [2]}},
                {"resources": 1, "elements": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, -1, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 3]}},
                {"resources": 4, "elements": {0: [1, 2], 1: [1]}},
                {"resources": 1, "elements": {0: [3], 1: [3]}},
            ],
        },
    )
    for i in examples:
        jobscripts_i, _ = group_resource_map_into_jobscripts(i["resources"])
        assert jobscripts_i == i["expected"]


def test_timedelta_parse_format_round_trip(null_config):
    td = timedelta(days=2, hours=25, minutes=92, seconds=77)
    td_str = timedelta_format(td)
    assert td_str == timedelta_format(timedelta_parse(td_str))


def test_raise_missing_env_executable(null_config, tmp_path):
    exec_name = (
        "my_executable"  # null_env (the default) has no executable "my_executable"
    )
    ts = hf.TaskSchema(
        objective="test_sub",
        actions=[hf.Action(commands=[hf.Command(command=f"<<executable:{exec_name}>>")])],
    )
    t1 = hf.Task(schema=ts)
    wkt = hf.WorkflowTemplate(
        name="test_sub",
        tasks=[t1],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    with pytest.raises(MissingEnvironmentExecutableError):
        wk.add_submission()


def test_raise_missing_env(null_config, tmp_path):
    env_name = "my_hpcflow_env"
    ts = hf.TaskSchema(
        objective="test_sub",
        actions=[hf.Action(environments=[hf.ActionEnvironment(environment=env_name)])],
    )
    t1 = hf.Task(schema=ts)
    wkt = hf.WorkflowTemplate(
        name="test_sub",
        tasks=[t1],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    with pytest.raises(MissingEnvironmentError):
        wk.add_submission()


def test_custom_env_and_executable(new_null_config, tmp_path):
    env_name = "my_hpcflow_env"
    exec_label = "my_exec_name"
    env = hf.Environment(
        name=env_name,
        executables=[
            hf.Executable(
                label=exec_label,
                instances=[
                    hf.ExecutableInstance(
                        command="command", num_cores=1, parallel_mode=None
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(env, skip_duplicates=True)

    ts = hf.TaskSchema(
        objective="test_sub",
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment(environment=env_name)],
                commands=[hf.Command(command=f"<<executable:{exec_label}>>")],
            )
        ],
    )
    t1 = hf.Task(schema=ts)
    wkt = hf.WorkflowTemplate(
        name="test_sub",
        tasks=[t1],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    wk.add_submission()


def test_abort_EARs_file_creation(null_config, tmp_path):
    wk_name = "temp"
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2, 3])],
    )
    wkt = hf.WorkflowTemplate(name=wk_name, tasks=[t1])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    sub = wk.add_submission()
    wk.submissions_path.mkdir(exist_ok=True, parents=True)
    sub.path.mkdir(exist_ok=True)
    sub._write_abort_EARs_file()
    with sub.abort_EARs_file_path.open("rt") as fp:
        lines = fp.read()

    assert lines == "0\n0\n0\n"


def test_abort_EARs_file_update(null_config, tmp_path):
    wk_name = "temp"
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2, 3])],
    )
    wkt = hf.WorkflowTemplate(name=wk_name, tasks=[t1])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    sub = wk.add_submission()
    wk.submissions_path.mkdir(exist_ok=True, parents=True)
    sub.path.mkdir(exist_ok=True)
    sub._write_abort_EARs_file()

    sub._set_run_abort(2)

    with sub.abort_EARs_file_path.open("rt") as fp:
        lines = fp.read()

    assert lines == "0\n0\n1\n"
