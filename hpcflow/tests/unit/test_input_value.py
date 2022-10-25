# import pytest

# from hpcflow.parameters import Parameter, InputValue, ValueSequence
# from hpcflow.errors import InputValueDuplicateSequenceAddress


# def test_raise_on_duplicate_input_value_sequence_address():
#     p1 = Parameter("p1")
#     with pytest.raises(InputValueDuplicateSequenceAddress):
#         InputValue(
#             parameter=p1,
#             value={"A": 1},
#             sequences=[
#                 ValueSequence(values=[1, 2, 3], path=("A",), nesting_order=0),
#                 ValueSequence(values=[4, 5, 6], path=("A",), nesting_order=0),
#             ],
#         )


# def test_raise_on_duplicate_input_value_sequence_address_empty():
#     p1 = Parameter("p1")
#     with pytest.raises(InputValueDuplicateSequenceAddress):
#         InputValue(
#             parameter=p1,
#             sequences=[ValueSequence(values=[1, 2, 3]), ValueSequence(values=[4, 5, 6])],
#         )
