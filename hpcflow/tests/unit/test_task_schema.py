import pytest

from hpcflow.api import Action, Command, TaskObjective, TaskSchema
from hpcflow.sdk.core.errors import InvalidIdentifier


@pytest.fixture
def action_a1():
    return Action(commands=[Command("ls")], environments=[])


@pytest.fixture
def schema_s1_kwargs(action_a1):
    return {"objective": TaskObjective("t1"), "actions": [action_a1]}


def test_task_schema_equality():
    t1a = TaskSchema("t1", actions=[])
    t1b = TaskSchema("t1", actions=[])
    assert t1a == t1b


def test_init_with_str_objective(action_a1):
    obj_str = "t1"
    obj = TaskObjective(obj_str)
    common = {"actions": [action_a1]}
    assert TaskSchema(obj_str, **common) == TaskSchema(obj, **common)


def test_init_with_method_with_underscore(schema_s1_kwargs):
    TaskSchema(method="my_method", **schema_s1_kwargs)


def test_raise_on_invalid_method_digit(schema_s1_kwargs):
    with pytest.raises(InvalidIdentifier):
        TaskSchema(method="9", **schema_s1_kwargs)


def test_raise_on_invalid_method_space(schema_s1_kwargs):
    with pytest.raises(InvalidIdentifier):
        TaskSchema(method="my method", **schema_s1_kwargs)


def test_raise_on_invalid_method_non_alpha_numeric(schema_s1_kwargs):
    with pytest.raises(InvalidIdentifier):
        TaskSchema(method="_mymethod", **schema_s1_kwargs)


def test_method_lowercasing(schema_s1_kwargs):
    assert TaskSchema(method="MyMethod", **schema_s1_kwargs) == TaskSchema(
        method="mymethod", **schema_s1_kwargs
    )
