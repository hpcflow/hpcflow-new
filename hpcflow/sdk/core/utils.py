import copy
import enum
from functools import wraps
import contextlib
import hashlib
from itertools import accumulate
import json
import keyword
import os
from pathlib import Path, PurePath
import random
import re
import socket
import string
import subprocess
from datetime import datetime, timezone
import sys
from typing import Tuple, Type, Union, List
import fsspec
import numpy as np

from ruamel.yaml import YAML
import sentry_sdk
from watchdog.utils.dirsnapshot import DirectorySnapshot

from hpcflow.sdk.core.errors import (
    ContainerKeyError,
    FromSpecMissingObjectError,
    InvalidIdentifier,
)
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


def check_valid_py_identifier(name: str) -> str:
    """Check a string is (roughly) a valid Python variable identifier and return it.

    The rules are:
        1. `name` must not be empty
        2. `name` must not be a Python keyword
        3. `name` must begin with an alphabetic character, and all remaining characters
           must be alphanumeric.

    Notes
    -----
    The following attributes are passed through this function on object initialisation:
        - `ElementGroup.name`
        - `Executable.label`
        - `Parameter.typ`
        - `TaskObjective.name`
        - `TaskSchema.method`
        - `TaskSchema.implementation`
        - `Loop.name`

    """
    exc = InvalidIdentifier(f"Invalid string for identifier: {name!r}")
    try:
        trial_name = name[1:].replace("_", "")  # "internal" underscores are allowed
    except TypeError:
        raise exc
    if (
        not name
        or not (name[0].isalpha() and ((trial_name[1:] or "a").isalnum()))
        or keyword.iskeyword(name)
    ):
        raise exc

    return name


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


def swap_nested_dict_keys(dct, inner_key):
    """Return a copy where top-level keys have been swapped with a second-level inner key.

    Examples:
    ---------
    >>> swap_nested_dict_keys(
        dct={
            'p1': {'format': 'direct', 'all_iterations': True},
            'p2': {'format': 'json'},
            'p3': {'format': 'direct'},
        },
        inner_key="format",
    )
    {
        "direct": {"p1": {"all_iterations": True}, "p3": {}},
        "json": {"p2": {}},
    }

    """
    out = {}
    for k, v in copy.deepcopy(dct).items():
        inner_val = v.pop(inner_key)
        if inner_val not in out:
            out[inner_val] = {}
        out[inner_val][k] = v
    return out


def get_in_container(cont, path, cast_indices=False, allow_getattr=False):
    cur_data = cont
    err_msg = (
        "Data at path {path_comps!r} is not a sequence, but is of type "
        "{cur_data_type!r} and so sub-data cannot be extracted."
    )
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
        elif isinstance(cur_data, dict):
            try:
                cur_data = cur_data[path_comp]
            except KeyError:
                raise ContainerKeyError(path=path[: idx + 1])
        elif allow_getattr:
            try:
                cur_data = getattr(cur_data, path_comp)
            except AttributeError:
                raise ValueError(
                    err_msg.format(cur_data_type=type(cur_data), path_comps=path[:idx])
                )
        else:
            raise ValueError(
                err_msg.format(cur_data_type=type(cur_data), path_comps=path[:idx])
            )
    return cur_data


def set_in_container(cont, path, value, ensure_path=False, cast_indices=False):
    if ensure_path:
        num_path = len(path)
        for idx in range(1, num_path):
            try:
                get_in_container(cont, path[:idx], cast_indices=cast_indices)
            except (KeyError, ValueError):
                set_in_container(
                    cont=cont,
                    path=path[:idx],
                    value={},
                    ensure_path=False,
                    cast_indices=cast_indices,
                )

    sub_data = get_in_container(cont, path[:-1], cast_indices=cast_indices)
    path_comp = path[-1]
    if isinstance(sub_data, (list, tuple)):
        if not isinstance(path_comp, int):
            msg = (
                f"Path component {path_comp!r} must be an integer index "
                f"since data is a sequence: {sub_data!r}."
            )
            if cast_indices:
                try:
                    path_comp = int(path_comp)
                except ValueError:
                    raise ValueError(msg)
            else:
                raise ValueError(msg)
    sub_data[path_comp] = value


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


def search_dir_files_by_regex(pattern, group=0, directory=".") -> List[str]:
    """Search recursively for files in a directory by a regex pattern and return matching
    file paths, relative to the given directory."""
    vals = []
    for i in Path(directory).rglob("*"):
        match = re.search(pattern, i.name)
        if match:
            match_groups = match.groups()
            if match_groups:
                match = match_groups[group]
                vals.append(str(i.relative_to(directory)))
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


def read_YAML(loadable_yaml, typ="safe"):
    yaml = YAML(typ=typ)
    return yaml.load(loadable_yaml)


def read_YAML_file(path: PathLike, typ="safe"):
    if is_fsspec_url(str(path)):
        with fsspec.open(path, "rt") as f:
            data = f.read()
        loadable_yaml = data
    else:
        loadable_yaml = Path(path)
    return read_YAML(loadable_yaml, typ=typ)


def write_YAML_file(obj, path: PathLike, typ="safe"):
    yaml = YAML(typ=typ)
    with Path(path).open("wt") as fp:
        yaml.dump(obj, fp)


def read_JSON_string(string: str):
    return json.loads(string)


def read_JSON_file(path):
    with Path(path).open("rt") as fh:
        return json.load(fh)


def write_JSON_file(obj, path: PathLike):
    with Path(path).open("wt") as fp:
        json.dump(obj, fp)


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


def ensure_in(item, lst) -> int:
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


def replace_items(lst, start, end, repl):
    """Replaced a range of items in a list with items in another list."""
    if end <= start:
        raise ValueError(
            f"`end` ({end}) must be greater than or equal to `start` ({start})."
        )
    if start >= len(lst):
        raise ValueError(f"`start` ({start}) must be less than length ({len(lst)}).")
    if end > len(lst):
        raise ValueError(
            f"`end` ({end}) must be less than or equal to length ({len(lst)})."
        )

    lst_a = lst[:start]
    lst_b = lst[end:]
    return lst_a + repl + lst_b


def flatten(lst):
    """Flatten an arbitrarily (but of uniform depth) nested list and return shape
    information to enable un-flattening.

    Un-flattening can be performed with the `reshape` function.

    lst
        List to be flattened. Each element must contain all lists or otherwise all items
        that are considered to be at the "bottom" of the nested structure (e.g. integers).
        For example, `[[1, 2], [3]]` is permitted and flattens to `[1, 2, 3]`, but
        `[[1, 2], 3]` is not permitted because the first element is a list, but the second
        is not.

    """

    def _flatten(lst, _depth=0):
        out = []
        for i in lst:
            if isinstance(i, list):
                out += _flatten(i, _depth=_depth + 1)
                all_lens[_depth].append(len(i))
            else:
                out.append(i)
        return out

    def _get_max_depth(lst):
        lst = lst[:]
        max_depth = 0
        while isinstance(lst, list):
            max_depth += 1
            try:
                lst = lst[0]
            except IndexError:
                # empty list, assume this is max depth
                break
        return max_depth

    max_depth = _get_max_depth(lst) - 1
    all_lens = tuple([] for _ in range(max_depth))

    return _flatten(lst), all_lens


def reshape(lst, lens):
    def _reshape(lst, lens):
        lens_acc = [0] + list(accumulate(lens))
        lst_rs = [lst[lens_acc[idx] : lens_acc[idx + 1]] for idx in range(len(lens))]
        return lst_rs

    for lens_i in lens[::-1]:
        lst = _reshape(lst, lens_i)

    return lst


def is_fsspec_url(url: str) -> bool:
    return bool(re.match(r"(?:[a-z0-9]+:{1,2})+\/\/", url))


class JSONLikeDirSnapShot(DirectorySnapshot):
    """Overridden DirectorySnapshot from watchdog to allow saving and loading from JSON."""

    def __init__(self, root_path=None, data=None):
        """Create an empty snapshot or load from JSON-like data."""

        self.root_path = root_path
        self._stat_info = {}
        self._inode_to_path = {}

        if data:
            for k in list((data or {}).keys()):
                # add root path
                full_k = str(PurePath(root_path) / PurePath(k))
                stat_dat, inode_key = data[k][:-2], data[k][-2:]
                self._stat_info[full_k] = os.stat_result(stat_dat)
                self._inode_to_path[tuple(inode_key)] = full_k

    def take(self, *args, **kwargs):
        """Take the snapshot."""
        super().__init__(*args, **kwargs)

    def to_json_like(self):
        """Export to a dict that is JSON-compatible and can be later reloaded.

        The last two integers in `data` for each path are the keys in
        `self._inode_to_path`.

        """

        # first key is the root path:
        root_path = next(iter(self._stat_info.keys()))

        # store efficiently:
        inode_invert = {v: k for k, v in self._inode_to_path.items()}
        data = {}
        for k, v in self._stat_info.items():
            k_rel = str(PurePath(k).relative_to(root_path))
            data[k_rel] = list(v) + list(inode_invert[k])

        return {
            "root_path": root_path,
            "data": data,
        }


def open_file(filename):
    """Open a file or directory using the default system application."""
    if sys.platform == "win32":
        os.startfile(filename)
    else:
        opener = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, filename])


def get_enum_by_name_or_val(enum_cls: Type, key: Union[str, None]) -> enum.Enum:
    """Retrieve an enum by name or value, assuming uppercase names and integer values."""
    err = f"Unknown enum key or value {key!r} for class {enum_cls!r}"
    if key is None or isinstance(key, enum_cls):
        return key
    elif isinstance(key, (int, float)):
        return enum_cls(int(key))  # retrieve by value
    elif isinstance(key, str):
        try:
            return getattr(enum_cls, key.upper())  # retrieve by name
        except AttributeError:
            raise ValueError(err)
    else:
        raise ValueError(err)


def split_param_label(param_path: str) -> Tuple[Union[str, None]]:
    """Split a parameter path into the path and the label, if present."""
    pattern = r"((?:\w|\.)+)(?:\[(\w+)\])?"
    match = re.match(pattern, param_path)
    return match.group(1), match.group(2)


def process_string_nodes(data, str_processor):
    """Walk through a nested data structure and process string nodes using a provided
    callable."""

    if isinstance(data, dict):
        for k, v in data.items():
            data[k] = process_string_nodes(v, str_processor)

    elif isinstance(data, (list, tuple, set)):
        _data = [process_string_nodes(i, str_processor) for i in data]
        if isinstance(data, tuple):
            data = tuple(_data)
        elif isinstance(data, set):
            data = set(_data)
        else:
            data = _data

    elif isinstance(data, str):
        data = str_processor(data)

    return data


def linspace_rect(start: List[float], stop: List[float], num: List[float], **kwargs):
    """Generate a linear space around a rectangle.

    Parameters
    ----------
    start
        Two start values; one for each dimension of the rectangle.
    stop
        Two stop values; one for each dimension of the rectangle.
    num
        Two number values; one for each dimension of the rectangle.

    Returns
    -------
    rect
        Coordinates of the rectangle perimeter.

    """

    if num[0] == 1 or num[1] == 1:
        raise ValueError("Both values in `num` must be greater than 1.")

    c0_range = np.linspace(start=start[0], stop=stop[0], num=num[0], **kwargs)
    c1_range_all = np.linspace(start=start[1], stop=stop[1], num=num[1], **kwargs)
    c1_range = c1_range_all[1:-1]

    c0_range_c1_start = np.vstack([c0_range, np.repeat(start[1], num[0])])
    c0_range_c1_stop = np.vstack([c0_range, np.repeat(c1_range_all[-1], num[0])])

    c1_range_c0_start = np.vstack([np.repeat(start[0], num[1] - 2), c1_range])
    c1_range_c0_stop = np.vstack([np.repeat(c0_range[-1], num[1] - 2), c1_range])

    rect = np.hstack(
        [c0_range_c1_start, c0_range_c1_stop, c1_range_c0_start, c1_range_c0_stop]
    )
    return rect
