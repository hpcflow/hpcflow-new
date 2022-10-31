import pytest

from hpcflow.api import Action, FileSpec, InputFileGenerator, Command, Parameter
from hpcflow.sdk.core.errors import MissingActionEnvironment


@pytest.fixture
def dummy_action_kwargs_pre_proc():
    act_kwargs = {
        "commands": [Command("ls")],
        "input_file_generators": [
            InputFileGenerator(
                input_file=FileSpec("inp_file", name="file.inp"), inputs=[Parameter("p1")]
            )
        ],
    }
    return act_kwargs


def test_action_equality():
    a1 = Action(commands=[Command("ls")], environments=[])
    a2 = Action(commands=[Command("ls")], environments=[])
    assert a1 == a2


def test_raise_on_no_envs():
    with pytest.raises(TypeError):
        Action(commands=[])
