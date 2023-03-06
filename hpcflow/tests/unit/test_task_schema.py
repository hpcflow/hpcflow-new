import pytest

from hpcflow.api import (
    Action,
    ActionEnvironment,
    Command,
    Environment,
    Parameter,
    TaskObjective,
    TaskSchema,
)
from hpcflow.sdk.core.errors import InvalidIdentifier
from hpcflow.sdk.core.test_utils import make_actions, make_parameters


@pytest.fixture
def env_1():
    return Environment(name="env_1")


@pytest.fixture
def act_env_1(env_1):
    return ActionEnvironment(env_1)


@pytest.fixture
def action_a1(act_env_1):
    return Action(commands=[Command("ls")], environments=[act_env_1])


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


def test_schema_action_validate():
    p1, p2, p3, p4 = make_parameters(4)
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    TaskSchema("t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4])


def test_schema_action_validate_raise_on_extra_schema_input():
    # assert raise ValueError
    p1, p2, p3, p4 = make_parameters(4)
    p7 = Parameter("p7")
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    with pytest.raises(ValueError):
        TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2, p7], outputs=[p3, p4]
        )


def test_schema_action_validate_raise_on_extra_schema_output():
    p7 = Parameter("p7")
    p1, p2, p3, p4 = make_parameters(4)
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    with pytest.raises(ValueError):
        TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4, p7]
        )


def test_schema_action_validate_raise_on_extra_action_input():
    p1, p2, p3, p4 = make_parameters(4)
    act_1, act_2, act_3 = make_actions(
        [(("p1", "p7"), "p5"), (("p2", "p5"), "p3"), ("p3", "p4")]
    )
    with pytest.raises(ValueError):
        TaskSchema("t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4])


def test_schema_action_validate_raise_on_extra_action_output():
    p1, p2, p3, p4 = make_parameters(4)
    act_1, act_2, act_3 = make_actions(
        [("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4", "p7")]
    )
    with pytest.raises(ValueError):
        TaskSchema("t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4])
