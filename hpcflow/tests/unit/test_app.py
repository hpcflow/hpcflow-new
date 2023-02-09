import pytest

from hpcflow.api import (
    Action,
    ActionEnvironment,
    Command,
    Environment,
    TaskSchema,
    hpcflow,
    Parameter,
)


@pytest.fixture
def env_1():
    return Environment(name="env_1")


@pytest.fixture
def act_env_1(env_1):
    return ActionEnvironment(env_1)


@pytest.fixture
def act_1(act_env_1):
    return Action(
        commands=[Command("<<parameter:p1>>")],
        environments=[act_env_1],
    )


def test_shared_data_from_json_like_with_shared_data_dependency(act_1):
    """Check we can generate some shared data objects where one depends on another."""

    p1 = Parameter("p1")
    p1._set_hash()
    p1_hash = p1._hash_value

    ts1 = TaskSchema(objective="ts1", actions=[act_1], inputs=[p1])
    ts1._set_hash()
    ts1_hash = ts1._hash_value

    env = ts1.actions[0].environments[0].environment
    env._set_hash()
    env_hash = env._hash_value

    shared_data_json = {
        "parameters": {
            p1_hash: {
                "is_file": p1.is_file,
                "sub_parameters": [],
                "type": p1.typ,
            }
        },
        "envs": {
            env_hash: {
                "name": "env_1",
                "setup": None,
                "specifiers": {},
                "executables": [],
            }
        },
        "task_schemas": {
            ts1_hash: {
                "method": ts1.method,
                "implementation": ts1.implementation,
                "version": ts1.version,
                "objective": ts1.objective.name,
                "inputs": [
                    {
                        "group": None,
                        "where": None,
                        "parameter": f"hash:{p1_hash}",
                        "default_value": None,
                        "propagation_mode": "IMPLICIT",
                    }
                ],
                "outputs": [],
                "actions": [
                    {
                        "_from_expand": False,
                        "commands": [
                            {
                                "command": "<<parameter:p1>>",
                                "arguments": None,
                                "stdout": None,
                                "stderr": None,
                                "stdin": None,
                            }
                        ],
                        "input_files": [],
                        "outputs": [],
                        "environments": [
                            {
                                "scope": {"kwargs": {}, "type": "ANY"},
                                "environment": f"hash:{env_hash}",
                            }
                        ],
                        "rules": [],
                    }
                ],
            }
        },
    }

    sh = hpcflow.shared_data_from_json_like(shared_data_json)

    assert sh["parameters"] == hpcflow.ParametersList([p1]) and sh[
        "task_schemas"
    ] == hpcflow.TaskSchemasList([ts1])
