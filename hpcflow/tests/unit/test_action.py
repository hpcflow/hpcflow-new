import pytest

from hpcflow.api import (
    Action,
    ActionScope,
    ActionScopeType,
    FileSpec,
    InputFileGenerator,
    Command,
    Parameter,
)
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


def test_action_scope_to_string_any():
    assert ActionScope.any().to_string() == "any"


def test_action_scope_to_string_main():
    assert ActionScope.main().to_string() == "main"


def test_action_scope_to_string_processing():
    assert ActionScope.processing().to_string() == "processing"


def test_action_scope_to_string_input_file_generator_no_kwargs():
    assert ActionScope.input_file_generator().to_string() == "input_file_generator"


def test_action_scope_to_string_output_file_parser_no_kwargs():
    assert ActionScope.output_file_parser().to_string() == "output_file_parser"


def test_action_scope_to_string_input_file_generator_with_kwargs():
    assert (
        ActionScope.input_file_generator(file="file1").to_string()
        == "input_file_generator[file=file1]"
    )


def test_action_scope_to_string_output_file_parser_with_kwargs():
    assert (
        ActionScope.output_file_parser(output="out1").to_string()
        == "output_file_parser[output=out1]"
    )


def test_action_scope_class_method_init_scope_any():
    assert ActionScope(typ=ActionScopeType.ANY) == ActionScope.any()


def test_action_scope_class_method_init_scope_main():
    assert ActionScope(typ=ActionScopeType.MAIN) == ActionScope.main()


def test_action_scope_class_method_init_scope_processing():
    assert ActionScope(typ=ActionScopeType.PROCESSING) == ActionScope.processing()


def test_action_scope_class_method_init_scope_input_file_generator_no_kwargs():
    assert (
        ActionScope(typ=ActionScopeType.INPUT_FILE_GENERATOR)
        == ActionScope.input_file_generator()
    )


def test_action_scope_class_method_init_scope_output_file_parser_no_kwargs():
    assert (
        ActionScope(typ=ActionScopeType.OUTPUT_FILE_PARSER)
        == ActionScope.output_file_parser()
    )


def test_action_scope_class_method_init_scope_input_file_generator_with_kwargs():
    assert ActionScope(
        typ=ActionScopeType.INPUT_FILE_GENERATOR, file="file1"
    ) == ActionScope.input_file_generator(file="file1")


def test_action_scope_class_method_init_scope_output_file_parser_with_kwargs():
    assert ActionScope(
        typ=ActionScopeType.OUTPUT_FILE_PARSER, output="out1"
    ) == ActionScope.output_file_parser(output="out1")


def test_action_scope_raise_on_unknown_kwargs_type_any():
    with pytest.raises(TypeError):
        ActionScope(typ=ActionScopeType.ANY, bad="arg")


def test_action_scope_raise_on_unknown_kwargs_type_main():
    with pytest.raises(TypeError):
        ActionScope(typ=ActionScopeType.MAIN, bad="arg")


def test_action_scope_raise_on_unknown_kwargs_type_processing():
    with pytest.raises(TypeError):
        ActionScope(typ=ActionScopeType.PROCESSING, bad="arg")


def test_action_scope_raise_on_unknown_kwargs_type_input_file_generator():
    with pytest.raises(TypeError):
        ActionScope(typ=ActionScopeType.INPUT_FILE_GENERATOR, bad="arg")


def test_action_scope_raise_on_unknown_kwargs_type_output_file_parser():
    with pytest.raises(TypeError):
        ActionScope(typ=ActionScopeType.OUTPUT_FILE_PARSER, bad="arg")


def test_action_scope_no_raise_on_good_kwargs_type_input_file_generator():
    ActionScope(typ=ActionScopeType.INPUT_FILE_GENERATOR, file="file1")


def test_action_scope_no_raise_on_good_kwargs_type_output_file_parser():
    ActionScope(typ=ActionScopeType.OUTPUT_FILE_PARSER, output="out1")


def test_action_scope_no_raise_on_no_kwargs_type_input_file_generator():
    ActionScope(typ=ActionScopeType.INPUT_FILE_GENERATOR)


def test_action_scope_no_raise_on_no_kwargs_type_output_file_parser():
    ActionScope(typ=ActionScopeType.OUTPUT_FILE_PARSER)


def test_action_scope_json_like_round_trip():
    as1 = ActionScope.input_file_generator(file="file1")
    js, _ = as1.to_json_like()
    as1_rl = ActionScope.from_json_like(js)
    assert as1 == as1_rl


def test_action_scope_from_json_like_string_and_dict_equality():
    as1_js = "input_file_generator[file=file1]"
    as2_js = {
        "type": "input_file_generator",
        "kwargs": {
            "file": "file1",
        },
    }
    assert ActionScope.from_json_like(as1_js) == ActionScope.from_json_like(as2_js)
