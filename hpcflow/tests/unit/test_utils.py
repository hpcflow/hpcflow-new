# import pytest

# from hpcflow.utils import (
#     get_duplicate_items,
#     check_valid_py_identifier,
#     group_by_dict_key_values,
# )


# def test_get_list_duplicate_items_no_duplicates():
#     lst = [1, 2, 3]
#     assert not get_duplicate_items(lst)


# def test_get_list_duplicate_items_one_duplicate():
#     lst = [1, 1, 3]
#     assert get_duplicate_items(lst) == [1]


# def test_get_list_duplicate_items_all_duplicates():
#     lst = [1, 1, 1]
#     assert get_duplicate_items(lst) == [1]


# def test_raise_check_valid_py_identifier_empty_str():
#     with pytest.raises(ValueError):
#         check_valid_py_identifier("")


# def test_raise_check_valid_py_identifier_start_digit():
#     with pytest.raises(ValueError):
#         check_valid_py_identifier("9sdj")


# def test_raise_check_valid_py_identifier_single_digit():
#     with pytest.raises(ValueError):
#         check_valid_py_identifier("9")


# def test_raise_check_valid_py_identifier_py_keyword():
#     with pytest.raises(ValueError):
#         check_valid_py_identifier("if")


# def test_expected_return_check_valid_py_identifier_all_latin_alpha():
#     assert check_valid_py_identifier("abc") == "abc"


# def test_expected_return_check_valid_py_identifier_all_latin_alphanumeric():
#     assert check_valid_py_identifier("abc123") == "abc123"


# def test_expected_return_check_valid_py_identifier_all_greek_alpha():
#     assert check_valid_py_identifier("αβγ") == "αβγ"


# def test_check_valid_py_identifier_case_insensitivity():
#     assert (
#         check_valid_py_identifier("abc012")
#         == check_valid_py_identifier("ABC012")
#         == check_valid_py_identifier("aBc012")
#         == "abc012"
#     )


# def test_expected_return_group_by_dict_key_values_single_key_items_single_key_passed():
#     item_1 = {"b": 1}
#     item_2 = {"b": 2}
#     item_3 = {"b": 1}
#     assert group_by_dict_key_values([item_1, item_2, item_3], "b") == [
#         [item_1, item_3],
#         [item_2],
#     ]


# def test_expected_return_group_by_dict_key_values_multi_key_items_single_key_passed():
#     item_1 = {"a": 9, "b": 1}
#     item_2 = {"a": 8, "b": 2}
#     item_3 = {"a": 9, "b": 1}
#     assert group_by_dict_key_values([item_1, item_2, item_3], "b") == [
#         [item_1, item_3],
#         [item_2],
#     ]


# def test_expected_return_group_by_dict_key_values_multi_key_items_multi_key_passed_two_groups():
#     item_1 = {"a": 9, "b": 1}
#     item_2 = {"a": 8, "b": 2}
#     item_3 = {"a": 9, "b": 1}
#     assert group_by_dict_key_values([item_1, item_2, item_3], "a", "b") == [
#         [item_1, item_3],
#         [item_2],
#     ]


# def test_expected_return_group_by_dict_key_values_multi_key_items_multi_key_passed_three_groups():
#     item_1 = {"a": 9, "b": 1}
#     item_2 = {"a": 9, "b": 2}
#     item_3 = {"a": 8, "b": 1}
#     assert group_by_dict_key_values([item_1, item_2, item_3], "a", "b") == [
#         [item_1],
#         [item_2],
#         [item_3],
#     ]


# def test_expected_return_group_by_dict_key_values_multi_key_items_multi_key_passed_one_group():
#     item_1 = {"a": 9, "b": 1}
#     item_2 = {"a": 9, "b": 1}
#     item_3 = {"a": 9, "b": 1}
#     assert group_by_dict_key_values([item_1, item_2, item_3], "a", "b") == [
#         [item_1, item_2, item_3]
#     ]


# def test_expected_return_group_by_dict_key_values_excluded_items_for_missing_keys_first_item():
#     item_1 = {"a": 9}
#     item_2 = {"a": 9, "b": 1}
#     item_3 = {"a": 9, "b": 1}
#     assert group_by_dict_key_values([item_1, item_2, item_3], "a", "b") == [
#         [item_1],
#         [item_2, item_3],
#     ]


# def test_expected_return_group_by_dict_key_values_excluded_items_for_missing_keys_second_item():
#     item_1 = {"a": 9, "b": 1}
#     item_2 = {"a": 9}
#     item_3 = {"a": 9, "b": 1}
#     assert group_by_dict_key_values([item_1, item_2, item_3], "a", "b") == [
#         [item_1, item_3],
#         [item_2],
#     ]
