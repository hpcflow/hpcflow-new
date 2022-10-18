# import pytest

# from hpcflow.actions import Action
# from hpcflow.command_files import FileSpec, InputFileGenerator
# from hpcflow.commands import Command
# from hpcflow.errors import MissingActionEnvironment
# from hpcflow.parameters import Parameter


# @pytest.fixture
# def dummy_commands_kwargs():
#     return {"commands": [Command("ls")]}


# @pytest.fixture
# def dummy_commands_spec():
#     return {"commands": [{"command": "ls"}]}


# @pytest.fixture
# def dummy_action_kwargs_no_env(dummy_commands_kwargs):
#     act_kwargs = {**dummy_commands_kwargs}
#     return act_kwargs


# @pytest.fixture
# def dummy_action_spec_no_env(dummy_commands_spec):
#     act_spec = {**dummy_commands_spec}
#     return act_spec


# @pytest.fixture
# def dummy_action_kwargs_pre_proc():
#     act_kwargs = {
#         "commands": [Command("ls")],
#         "input_file_generators": [
#             InputFileGenerator(
#                 input_file=FileSpec("inp_file", name="file.inp"), inputs=[Parameter("p1")]
#             )
#         ],
#     }
#     return act_kwargs


# def test_raise_on_no_envs(dummy_action_kwargs_no_env):
#     with pytest.raises(TypeError):
#         Action(**dummy_action_kwargs_no_env)


# def test_spec_raise_on_no_envs(dummy_action_spec_no_env):
#     with pytest.raises(MissingActionEnvironment):
#         Action.from_spec(dummy_action_spec_no_env)


# def test_1(dummy_action_kwargs_pre_proc):
#     act = Action(**dummy_action_kwargs_pre_proc)
