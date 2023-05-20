import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import InputValueDuplicateSequenceAddress


@pytest.fixture
def param_p1():
    return hf.Parameter("p1")


def test_fix_trailing_path_delimiter(param_p1):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path="a.")
    iv2 = hf.InputValue(parameter=param_p1, value=101, path="a")
    assert iv1.path == iv2.path


def test_fix_single_path_delimiter(param_p1):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path=".")
    iv2 = hf.InputValue(parameter=param_p1, value=101)
    assert iv1.path == iv2.path


def test_normalised_path_without_path(param_p1):
    iv1 = hf.InputValue(parameter=param_p1, value=101)
    assert iv1.normalised_path == "inputs.p1"


def test_normalised_path_with_single_element_path(param_p1):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path="a")
    assert iv1.normalised_path == "inputs.p1.a"


def test_normalised_path_with_multi_element_path(param_p1):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path="a.b")
    assert iv1.normalised_path == "inputs.p1.a.b"


def test_normalised_path_with_empty_path(param_p1):
    iv1 = hf.InputValue(parameter=param_p1, value=101, path="")
    assert iv1.normalised_path == "inputs.p1"


def test_resource_spec_get_param_path():
    rs1 = hf.ResourceSpec()
    assert rs1.normalised_path == "resources.any"


def test_resource_spec_get_param_path_scope_any_with_single_kwarg():
    rs1 = hf.ResourceSpec(scratch="local")
    assert rs1.normalised_path == "resources.any"


def test_resources_spec_get_param_path_scope_main():
    rs1 = hf.ResourceSpec(scope=hf.ActionScope.main())
    assert rs1.normalised_path == "resources.main"


def test_resources_spec_get_param_path_scope_with_kwargs():
    rs1 = hf.ResourceSpec(scope=hf.ActionScope.input_file_generator(file="file1"))
    assert rs1.normalised_path == "resources.input_file_generator[file=file1]"


def test_resources_spec_get_param_path_scope_with_no_kwargs():
    rs1 = hf.ResourceSpec(scope=hf.ActionScope.input_file_generator())
    assert rs1.normalised_path == "resources.input_file_generator"


# def test_raise_on_duplicate_input_value_sequence_address(param_p1):
#     with pytest.raises(InputValueDuplicateSequenceAddress):
#         hf.InputValue(
#             parameter=param_p1,
#             value={"A": 1},
#             sequences=[
#                 hf.ValueSequence(values=[1, 2, 3], path=("A",), nesting_order=0),
#                 hf.ValueSequence(values=[4, 5, 6], path=("A",), nesting_order=0),
#             ],
#         )

#     s1 = hf.TaskSchema("t1", inputs=[param_p1], actions=[])
#     t1 = hf.Task(schemas=[s1], inputs=[hf.InputValue(param_p1, value=101)])


# def test_raise_on_duplicate_input_value_sequence_address_empty():
#     p1 = hf.Parameter("p1")
#     with pytest.raises(InputValueDuplicateSequenceAddress):
#         hf.InputValue(
#             parameter=p1,
#             sequences=[hf.ValueSequence(values=[1, 2, 3]), hf.ValueSequence(values=[4, 5, 6])],
#         )
