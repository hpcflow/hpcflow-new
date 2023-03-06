import pytest
import zarr
import numpy as np

from hpcflow.sdk.core.utils import (
    bisect_slice,
    get_nested_indices,
    merge_into_zarr_column_array,
)


def test_get_nested_indices_expected_values_size_2_nest_levels_2():
    size, nest_levels = (2, 2)
    assert [
        get_nested_indices(i, size=size, nest_levels=nest_levels)
        for i in range(size**nest_levels)
    ] == [
        [0, 0],
        [0, 1],
        [1, 0],
        [1, 1],
    ]


def test_get_nested_indices_expected_values_size_2_nest_levels_4():
    size, nest_levels = (2, 4)
    assert [
        get_nested_indices(i, size=size, nest_levels=nest_levels)
        for i in range(size**nest_levels)
    ] == [
        [0, 0, 0, 0],
        [0, 0, 0, 1],
        [0, 0, 1, 0],
        [0, 0, 1, 1],
        [0, 1, 0, 0],
        [0, 1, 0, 1],
        [0, 1, 1, 0],
        [0, 1, 1, 1],
        [1, 0, 0, 0],
        [1, 0, 0, 1],
        [1, 0, 1, 0],
        [1, 0, 1, 1],
        [1, 1, 0, 0],
        [1, 1, 0, 1],
        [1, 1, 1, 0],
        [1, 1, 1, 1],
    ]


def test_get_nested_indices_expected_values_size_4_nest_levels_2():
    size, nest_levels = (4, 2)
    assert [
        get_nested_indices(i, size=size, nest_levels=nest_levels)
        for i in range(size**nest_levels)
    ] == [
        [0, 0],
        [0, 1],
        [0, 2],
        [0, 3],
        [1, 0],
        [1, 1],
        [1, 2],
        [1, 3],
        [2, 0],
        [2, 1],
        [2, 2],
        [2, 3],
        [3, 0],
        [3, 1],
        [3, 2],
        [3, 3],
    ]


def test_get_nested_indices_expected_values_size_4_nest_levels_3():
    size, nest_levels = (4, 3)
    assert [
        get_nested_indices(i, size=size, nest_levels=nest_levels)
        for i in range(size**nest_levels)
    ] == [
        [0, 0, 0],
        [0, 0, 1],
        [0, 0, 2],
        [0, 0, 3],
        [0, 1, 0],
        [0, 1, 1],
        [0, 1, 2],
        [0, 1, 3],
        [0, 2, 0],
        [0, 2, 1],
        [0, 2, 2],
        [0, 2, 3],
        [0, 3, 0],
        [0, 3, 1],
        [0, 3, 2],
        [0, 3, 3],
        [1, 0, 0],
        [1, 0, 1],
        [1, 0, 2],
        [1, 0, 3],
        [1, 1, 0],
        [1, 1, 1],
        [1, 1, 2],
        [1, 1, 3],
        [1, 2, 0],
        [1, 2, 1],
        [1, 2, 2],
        [1, 2, 3],
        [1, 3, 0],
        [1, 3, 1],
        [1, 3, 2],
        [1, 3, 3],
        [2, 0, 0],
        [2, 0, 1],
        [2, 0, 2],
        [2, 0, 3],
        [2, 1, 0],
        [2, 1, 1],
        [2, 1, 2],
        [2, 1, 3],
        [2, 2, 0],
        [2, 2, 1],
        [2, 2, 2],
        [2, 2, 3],
        [2, 3, 0],
        [2, 3, 1],
        [2, 3, 2],
        [2, 3, 3],
        [3, 0, 0],
        [3, 0, 1],
        [3, 0, 2],
        [3, 0, 3],
        [3, 1, 0],
        [3, 1, 1],
        [3, 1, 2],
        [3, 1, 3],
        [3, 2, 0],
        [3, 2, 1],
        [3, 2, 2],
        [3, 2, 3],
        [3, 3, 0],
        [3, 3, 1],
        [3, 3, 2],
        [3, 3, 3],
    ]


def test_get_nest_index_raise_on_rollover():
    size = 4
    nest_levels = 3
    with pytest.raises(ValueError):
        get_nested_indices(
            idx=size**nest_levels,
            size=size,
            nest_levels=nest_levels,
            raise_on_rollover=True,
        )


@pytest.fixture
def zarr_column_array(tmp_path):
    headers = ["a", "b", "c"]
    num_rows = 2
    fill_value = -1
    arr = zarr.open_array(
        store=f"{tmp_path}/zarr_column_array_test.zarr",
        mode="w",
        shape=(num_rows, len(headers)),
        dtype=int,
        fill_value=fill_value,
    )
    arr[:] = np.arange(np.product(arr.shape)).reshape(arr.shape)
    return arr, headers, fill_value


@pytest.fixture
def zarr_column_array_empty(tmp_path):
    headers = ["a", "b", "c"]
    num_rows = 0
    fill_value = -1
    arr = zarr.open_array(
        store=f"{tmp_path}/zarr_column_array_test.zarr",
        mode="w",
        shape=(num_rows, len(headers)),
        dtype=int,
        fill_value=fill_value,
    )
    arr[:] = np.arange(np.product(arr.shape)).reshape(arr.shape)
    return arr, headers, fill_value


def test_merge_into_zarr_column_array_raise_on_headers_length_mismatch(zarr_column_array):
    arr, headers, _ = zarr_column_array
    headers.pop(0)
    new_headers = ["a", "b", "c"]
    new_arr = zarr.ones((4, len(new_headers)), dtype=int)
    with pytest.raises(ValueError):
        merge_into_zarr_column_array(arr, headers, new_arr, new_headers)


def test_merge_into_zarr_column_array_raise_on_new_headers_length_mismatch(
    zarr_column_array,
):
    arr, headers, _ = zarr_column_array
    new_headers = ["a", "b", "c"]
    new_arr = zarr.ones((4, 2), dtype=int)
    with pytest.raises(ValueError):
        merge_into_zarr_column_array(arr, headers, new_arr, new_headers)


def test_merge_into_zarr_column_array_initial_empty(zarr_column_array_empty):
    arr, headers, fill_value = zarr_column_array_empty
    np_arr = np.array(arr)

    new_headers = ["a", "b", "c"]
    new_arr = zarr.ones((2, len(new_headers)), dtype=int)
    new_arr[:] = np.arange(np.product(new_arr.shape)).reshape(new_arr.shape) * 100
    np_new_arr = np.array(new_arr)

    expected = np.empty(
        shape=(arr.shape[0] + new_arr.shape[0], len(new_headers)),
        dtype=int,
    )

    expected[: np_arr.shape[0], : np_arr.shape[1]] = np_arr
    expected[: np_arr.shape[0], np_arr.shape[1] :] = fill_value
    expected[np_arr.shape[0] :] = np_new_arr

    headers = merge_into_zarr_column_array(arr, headers, new_arr, new_headers)

    assert np.all(arr[:] == expected) and headers == new_headers


def test_merge_into_zarr_column_array_new_empty(zarr_column_array):
    arr, headers, fill_value = zarr_column_array
    np_arr = np.array(arr)

    new_headers = ["a", "b", "c"]
    new_arr = zarr.ones((0, len(new_headers)), dtype=int)
    new_arr[:] = np.arange(np.product(new_arr.shape)).reshape(new_arr.shape) * 100
    np_new_arr = np.array(new_arr)

    expected = np.empty(
        shape=(arr.shape[0] + new_arr.shape[0], len(new_headers)),
        dtype=int,
    )

    expected[: np_arr.shape[0], : np_arr.shape[1]] = np_arr
    expected[: np_arr.shape[0], np_arr.shape[1] :] = fill_value
    expected[np_arr.shape[0] :] = np_new_arr

    headers = merge_into_zarr_column_array(arr, headers, new_arr, new_headers)

    assert np.all(arr[:] == expected) and headers == new_headers


def test_merge_into_zarr_column_array_with_column_reorder(zarr_column_array):
    arr, headers, fill_value = zarr_column_array
    np_arr = np.array(arr)

    new_headers = ["b", "a", "c"]
    new_arr = zarr.ones((2, len(new_headers)), dtype=int)
    new_arr[:] = np.arange(np.product(new_arr.shape)).reshape(new_arr.shape) * 100
    np_new_arr = np.array(new_arr)

    expected = np.empty(
        shape=(arr.shape[0] + new_arr.shape[0], len(new_headers)),
        dtype=int,
    )

    expected[: np_arr.shape[0], : np_arr.shape[1]] = np_arr
    expected[: np_arr.shape[0], np_arr.shape[1] :] = fill_value
    expected[np_arr.shape[0] :, [1, 0, 2]] = np_new_arr

    header_old = list(headers)
    headers = merge_into_zarr_column_array(arr, headers, new_arr, new_headers)

    assert np.all(arr[:] == expected) and headers == header_old


def test_merge_into_zarr_column_array_with_no_new_columns(zarr_column_array):

    arr, headers, _ = zarr_column_array
    np_arr = np.array(arr)

    new_headers = ["a", "b", "c"]
    new_arr = zarr.ones((4, len(new_headers)), dtype=int)
    new_arr[:] = np.arange(np.product(new_arr.shape)).reshape(new_arr.shape) * 100

    headers = merge_into_zarr_column_array(arr, headers, new_arr, new_headers)
    expected = np.vstack([np_arr, np.array(new_arr)])

    assert np.all(arr[:] == expected) and headers == new_headers


def test_merge_into_zarr_column_array_with_new_column(zarr_column_array):

    arr, headers, fill_value = zarr_column_array
    np_arr = np.array(arr)

    new_headers = ["a", "b", "c", "d"]
    new_arr = zarr.ones((2, len(new_headers)), dtype=int)
    new_arr[:] = np.arange(np.product(new_arr.shape)).reshape(new_arr.shape) * 100
    np_new_arr = np.array(new_arr)

    expected = np.empty(
        shape=(arr.shape[0] + new_arr.shape[0], len(new_headers)),
        dtype=int,
    )

    expected[: np_arr.shape[0], : np_arr.shape[1]] = np_arr
    expected[: np_arr.shape[0], np_arr.shape[1] :] = fill_value
    expected[np_arr.shape[0] :] = np_new_arr

    headers = merge_into_zarr_column_array(arr, headers, new_arr, new_headers)

    assert np.all(arr[:] == expected) and headers == new_headers


def test_bisect_slice():
    tot_len = 8
    tot_lst = list(range(tot_len))
    for sel_start in range(tot_len + 1):
        for sel_step in range(1, tot_len):
            for sel_stop in range(sel_start, tot_len + 1):
                for len_A in range(tot_len):
                    lst_A = tot_lst[:len_A]
                    lst_B = tot_lst[len_A:]
                    selection = slice(sel_start, sel_stop, sel_step)
                    slice_A, slice_B = bisect_slice(selection, len(lst_A))
                    sub_A = lst_A[slice_A]
                    sub_B = lst_B[slice_B]
                    assert sub_A + sub_B == tot_lst[selection]


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
