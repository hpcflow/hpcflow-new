import pytest

from hpcflow.api import TaskSchema, hpcflow, Parameter


def test_shared_data_from_json_like_with_shared_data_dependency():
    """Check we can generate some shared data objects where one depends on another."""

    p1 = Parameter("p1")
    p1._set_hash()
    p1_hash = p1._hash_value

    ts1 = TaskSchema(objective="ts1", actions=[], inputs=[p1])
    ts1._set_hash()
    ts1_hash = ts1._hash_value

    shared_data_json = {
        "parameters": {
            p1_hash: {
                "is_file": p1.is_file,
                "sub_parameters": [],
                "type": p1.typ,
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
                "actions": [],
            }
        },
    }

    sh = hpcflow.shared_data_from_json_like(shared_data_json)

    assert sh["parameters"] == hpcflow.ParametersList([p1]) and sh[
        "task_schemas"
    ] == hpcflow.TaskSchemasList([ts1])
