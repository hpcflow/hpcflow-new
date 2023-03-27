from functools import wraps
import contextlib
import hashlib
import json
import keyword
import os
from pathlib import Path
import random
import re
import socket
import string
from datetime import datetime, timezone
from typing import Mapping, List, Union

import zarr
import numpy.typing as npt
from ruamel.yaml import YAML
import sentry_sdk

from hpcflow.sdk.core.errors import FromSpecMissingObjectError, InvalidIdentifier
from hpcflow.sdk.typing import PathLike


def load_config(func):
    """API function decorator to ensure the configuration has been loaded, and load if not."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.is_config_loaded:
            self.load_config()
        return func(self, *args, **kwargs)

    return wrapper


def make_workflow_id():
    length = 12
    chars = string.ascii_letters + "0123456789"
    return "".join(random.choices(chars, k=length))


def get_time_stamp():
    return datetime.now(timezone.utc).astimezone().strftime("%Y.%m.%d_%H:%M:%S_%z")


def get_duplicate_items(lst):
    """Get a list of all items in an iterable that appear more than once, assuming items
    are hashable.

    Examples
    --------
    >>> get_duplicate_items([1, 1, 2, 3])
    [1]

    >>> get_duplicate_items([1, 2, 3])
    []

    >>> get_duplicate_items([1, 2, 3, 3, 3, 2])
    [2, 3, 2]

    """
    seen = []
    return list(set(x for x in lst if x in seen or seen.append(x)))


def check_valid_py_identifier(name):
    """Check a string is (roughly) a valid Python variable identifier and if so return it
    in lower-case.

    Notes
    -----
    Will be used for:
      - task objective name
      - task method
      - task implementation
      - parameter type
      - parameter name
      - loop name
      - element group name

    """
    trial_name = name[1:].replace("_", "")  # "internal" underscores are allowed
    if (
        not name
        or not (name[0].isalpha() and ((trial_name[1:] or "a").isalnum()))
        or keyword.iskeyword(name)
        or name == "add_object"  # method of `DotAccessObjectList`
    ):
        raise InvalidIdentifier(f"Invalid string for identifier: {name!r}")

    return name.lower()


def group_by_dict_key_values(lst, *keys):
    """Group a list of dicts according to specified equivalent key-values.

    Parameters
    ----------
    lst : list of dict
        The list of dicts to group together.
    keys : tuple
        Dicts that have identical values for all of these keys will be grouped together
        into a sub-list.

    Returns
    -------
    grouped : list of list of dict

    Examples
    --------
    >>> group_by_dict_key_values([{'a': 1}, {'a': 2}, {'a': 1}], 'a')
    [[{'a': 1}, {'a': 1}], [{'a': 2}]]

    """

    grouped = [[lst[0]]]
    for lst_item in lst[1:]:
        for group_idx, group in enumerate(grouped):

            try:
                is_vals_equal = all(lst_item[k] == group[0][k] for k in keys)

            except KeyError:
                # dicts that do not have all `keys` will be in their own group:
                is_vals_equal = False

            if is_vals_equal:
                grouped[group_idx].append(lst_item)
                break

        if not is_vals_equal:
            grouped.append([lst_item])

    return grouped


def get_in_container(cont, path, cast_indices=False):
    cur_data = cont
    for idx, path_comp in enumerate(path):
        if isinstance(cur_data, (list, tuple)):
            if not isinstance(path_comp, int):
                msg = (
                    f"Path component {path_comp!r} must be an integer index "
                    f"since data is a sequence: {cur_data!r}."
                )
                if cast_indices:
                    try:
                        path_comp = int(path_comp)
                    except TypeError:
                        raise TypeError(msg)
                else:
                    raise TypeError(msg)
            cur_data = cur_data[path_comp]
        elif isinstance(cur_data, Mapping):
            cur_data = cur_data[path_comp]
        else:
            raise ValueError(
                f"Data at path {path[:idx]} is not a sequence, but is of type "
                f"{type(cur_data)!r} and so sub-data cannot be extracted."
            )
    return cur_data


def set_in_container(cont, path, value, ensure_path=False):

    if ensure_path:
        num_path = len(path)
        for idx in range(1, num_path):
            try:
                get_in_container(cont, path[:idx])
            except (KeyError, ValueError):
                set_in_container(cont, path[:idx], {}, ensure_path=False)

    sub_data = get_in_container(cont, path[:-1])
    sub_data[path[-1]] = value


def get_relative_path(path1, path2):
    """Get relative path components between two paths.

    Parameters
    ----------
    path1 : tuple of (str or int or float) of length N
    path2 : tuple of (str or int or float) of length less than or equal to N

    Returns
    -------
    relative_path : tuple of (str or int or float)
        The path components in `path1` that are not in `path2`.

    Raises
    ------
    ValueError
        If the two paths do not share a common ancestor of path components, or if `path2`
        is longer than `path1`.

    Notes
    -----
    This function behaves like a simplified `PurePath(*path1).relative_to(PurePath(*path2))`
    from the `pathlib` module, but where path components can include non-strings.

    Examples
    --------
    >>> get_relative_path(('A', 'B', 'C'), ('A',))
    ('B', 'C')

    >>> get_relative_path(('A', 'B'), ('A', 'B'))
    ()

    """

    len_path2 = len(path2)
    msg = f"{path1!r} is not in the subpath of {path2!r}."

    if len(path1) < len_path2:
        raise ValueError(msg)

    for i, j in zip(path1[:len_path2], path2):
        if i != j:
            raise ValueError(msg)

    return path1[len_path2:]


def search_dir_files_by_regex(pattern, group=0, directory="."):
    vals = []
    for i in Path(directory).iterdir():
        match = re.search(pattern, i.name)
        if match:
            match_groups = match.groups()
            if match_groups:
                match = match_groups[group]
                vals.append(match)
    return vals


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class PrettyPrinter(object):
    def __str__(self):
        lines = [self.__class__.__name__ + ":"]
        for key, val in vars(self).items():
            lines += f"{key}: {val}".split("\n")
        return "\n    ".join(lines)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        elif args or kwargs:
            # if existing instance, make the point that new arguments don't do anything!
            raise ValueError(
                f"{cls.__name__!r} is a singleton class and cannot be instantiated with new "
                f"arguments. The positional arguments {args!r} and keyword-arguments "
                f"{kwargs!r} have been ignored."
            )
        return cls._instances[cls]


@contextlib.contextmanager
def sentry_wrap(name, transaction_op=None, span_op=None):
    if not transaction_op:
        transaction_op = name
    if not span_op:
        span_op = name
    try:
        with sentry_sdk.start_transaction(op=transaction_op, name=name):
            with sentry_sdk.start_span(op=span_op) as span:
                yield span
    finally:
        sentry_sdk.flush()  # avoid queue message on stdout


def capitalise_first_letter(chars):
    return chars[0].upper() + chars[1:]


def check_in_object_list(spec_name, spec_pos=1, obj_list_pos=2):
    """Decorator factory for the various `from_spec` class methods that have attributes
    that should be replaced by an object from an object list."""

    def decorator(func):
        @wraps(func)
        def wrap(*args, **kwargs):

            spec = args[spec_pos]
            obj_list = args[obj_list_pos]
            if spec[spec_name] not in obj_list:
                cls_name = args[0].__name__
                raise FromSpecMissingObjectError(
                    f"A {spec_name!r} object required to instantiate the {cls_name!r} "
                    f"object is missing."
                )
            return func(*args, **kwargs)

        return wrap

    return decorator


def read_YAML(loadable_yaml):
    yaml = YAML(typ="safe")
    return yaml.load(loadable_yaml)


def read_YAML_file(path: PathLike):
    return read_YAML(Path(path))


def read_JSON_file(path):
    with Path(path).open("rt") as fh:
        return json.load(fh)


def get_item_repeat_index(lst, distinguish_singular=False, item_callable=None):
    """Get the repeat index for each item in a list.

    Parameters
    ----------
    lst : list
        Must contain hashable items, or hashable objects that are returned via `callable`
        called on each item.
    distinguish_singular : bool, optional
        If True, items that are not repeated will have a repeat index of 0, and items that
        are repeated will have repeat indices starting from 1.
    item_callable : callable, optional
        If specified, comparisons are made on the output of this callable on each item.

    Returns
    -------
    repeat_idx : list of int
        Repeat indices of each item (see `distinguish_singular` for details).

    """

    idx = {}
    for i_idx, item in enumerate(lst):
        if item_callable:
            item = item_callable(item)
        if item not in idx:
            idx[item] = []
        idx[item] += [i_idx]

    rep_idx = [None] * len(lst)
    for k, v in idx.items():
        start = len(v) > 1 if distinguish_singular else 0
        for i_idx, i in enumerate(v, start):
            rep_idx[i] = i_idx

    return rep_idx


def get_process_stamp():
    return "{} {} {}".format(
        datetime.now(),
        socket.gethostname(),
        os.getpid(),
    )


def remove_ansi_escape_sequences(string):
    ansi_escape = re.compile(r"(\x9B|\x1B\[)[0-?]*[ -\/]*[@-~]")
    return ansi_escape.sub("", string)


def get_md5_hash(obj):
    json_str = json.dumps(obj, sort_keys=True)
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()


def get_nested_indices(idx, size, nest_levels, raise_on_rollover=False):
    """Generate the set of nested indices of length `n` that correspond to a global
    `idx`.

    Examples
    --------
    >>> for i in range(4**2): print(get_nest_index(i, nest_levels=2, size=4))
    [0, 0]
    [0, 1]
    [0, 2]
    [0, 3]
    [1, 0]
    [1, 1]
    [1, 2]
    [1, 3]
    [2, 0]
    [2, 1]
    [2, 2]
    [2, 3]
    [3, 0]
    [3, 1]
    [3, 2]
    [3, 3]

    >>> for i in range(4**3): print(get_nested_indices(i, nest_levels=3, size=4))
    [0, 0, 0]
    [0, 0, 1]
    [0, 0, 2]
    [0, 0, 3]
    [0, 1, 0]
       ...
    [3, 2, 3]
    [3, 3, 0]
    [3, 3, 1]
    [3, 3, 2]
    [3, 3, 3]
    """
    if raise_on_rollover and idx >= size**nest_levels:
        raise ValueError(
            f"`idx` ({idx}) is greater than or equal to  size**nest_levels` "
            f"({size**nest_levels})."
        )

    return [(idx // (size ** (nest_levels - (i + 1)))) % size for i in range(nest_levels)]


def ensure_in(item, lst):
    """Get the index of an item in a list and append the item if it is not in the
    list."""
    # TODO: add tests
    try:
        idx = lst.index(item)
    except ValueError:
        lst.append(item)
        idx = len(lst) - 1
    return idx


def list_to_dict(lst, exclude=None):
    # TODD: test
    exclude = exclude or []
    dct = {k: [] for k in lst[0].keys() if k not in exclude}
    for i in lst:
        for k, v in i.items():
            if k not in exclude:
                dct[k].append(v)

    return dct


def merge_into_zarr_column_array(
    arr: zarr.Array,
    headers: List[str],
    new_arr: Union[npt.NDArray, zarr.Array],
    new_headers: List[str],
) -> List[str]:
    """Append rows into a 2D array whose columns correspond to a list of string
    headers, accounting for different/new headers corresponding to the new array."""

    # TODO: support removal of columns, if a whole column is the fill value?

    if arr.dtype != new_arr.dtype:
        raise ValueError(
            f"New array (dtype {new_arr.dtype}) must have the same dtype as the array "
            f"into which the new array is to be merged (dtype {arr.dtype})."
        )

    if len(headers) != arr.shape[1]:
        raise ValueError(
            "`headers` must be a list of length equal to the number of "
            "columns in `arr`."
        )

    if len(new_headers) != new_arr.shape[1]:
        raise ValueError(
            "`new_headers` must be a list of length equal to the number of "
            "columns in `new_arr`."
        )

    length = arr.shape[0]

    # add new columns:
    add_headers = set(new_headers) - set(headers)
    if add_headers:
        headers += list(add_headers)
        arr.resize(arr.shape[0], len(headers))

    # add new rows:
    arr.resize(arr.shape[0] + new_arr.shape[0], len(headers))

    # get indices of new_headers in new columns:
    head_idx = [headers.index(i) for i in new_headers]

    # fill new row data:
    for idx, i in enumerate(head_idx):
        arr[length:, i] = new_arr[:, idx]

    return headers


def merge_into_headered_zarr_column_array(
    arr: zarr.Array,
    new_arr: zarr.Array,
    header_path: List[str],
):
    """Like `merge_into_zarr_column_array`, but the old and new headers are expected
    at a particular path within the array attributes."""

    attrs = arr.attrs.asdict()
    headers = get_in_container(attrs, header_path)

    new_attrs = new_arr.attrs.asdict()
    new_headers = get_in_container(new_attrs, header_path)

    new_headers = merge_into_zarr_column_array(
        arr=arr,
        headers=headers,
        new_arr=new_arr,
        new_headers=new_headers,
    )
    set_in_container(attrs, header_path, new_headers)
    arr.attrs.put(attrs)


def bisect_slice(selection: slice, len_A: int):
    """Given two sequences (the first of which of known length), get the two slices that
    are equivalent to a given slice if the two sequences were combined."""

    if selection.start < 0 or selection.stop < 0 or selection.step < 0:
        raise NotImplementedError("Can't do negative slices yet.")

    A_idx = selection.indices(len_A)
    B_start = selection.start - len_A
    if len_A != 0 and B_start < 0:
        B_start = B_start % selection.step
    if len_A > selection.stop:
        B_stop = B_start
    else:
        B_stop = selection.stop - len_A
    B_idx = (B_start, B_stop, selection.step)
    A_slice = slice(*A_idx)
    B_slice = slice(*B_idx)

    return A_slice, B_slice
