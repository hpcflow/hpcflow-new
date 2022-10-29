# from multiprocessing import dummy
# import pytest
# from hpcflow.actions import Action
# from hpcflow.commands import Command

# from hpcflow.errors import InvalidIdentifier, MissingActionsError
# from hpcflow.task_schema import TaskObjective, TaskSchema


# @pytest.fixture
# def dummy_action():
#     return Action(commands=[Command("ls")])


# @pytest.fixture
# def dummy_schema(dummy_action):
#     return TaskSchema(TaskObjective("simulate"), actions=[dummy_action])


# @pytest.fixture
# def dummy_schema_args(dummy_action):
#     return {"objective": TaskObjective("simulate"), "actions": [dummy_action]}


# def test_init_with_str_objective(dummy_action):
#     obj_str = "simulate"
#     obj = TaskObjective(obj_str)
#     common = {"actions": [dummy_action]}
#     assert TaskSchema(obj_str, **common) == TaskSchema(obj, **common)


# def test_raise_on_missing_actions():
#     with pytest.raises(MissingActionsError):
#         TaskSchema("simulate", actions=[])


# def test_init_with_method_with_underscore(dummy_schema_args):
#     TaskSchema(method="my_method", **dummy_schema_args)


# def test_raise_on_invalid_method_digit(dummy_schema_args):
#     with pytest.raises(InvalidIdentifier):
#         TaskSchema(method="9", **dummy_schema_args)


# def test_raise_on_invalid_method_space(dummy_schema_args):
#     with pytest.raises(InvalidIdentifier):
#         TaskSchema(method="my method", **dummy_schema_args)


# def test_raise_on_invalid_method_non_alpha_numeric(dummy_schema_args):
#     with pytest.raises(InvalidIdentifier):
#         TaskSchema(method="_mymethod", **dummy_schema_args)


# def test_method_lowercasing(dummy_schema_args):
#     assert TaskSchema(method="MyMethod", **dummy_schema_args) == TaskSchema(
#         method="mymethod", **dummy_schema_args
#     )
