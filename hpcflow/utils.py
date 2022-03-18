import keyword
from pathlib import Path
import random
import re
import string
from datetime import datetime, timezone
from typing import Mapping

from hpcflow.errors import InvalidIdentifier


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


def get_in_container(cont, path):
    cur_data = cont
    for idx, path_comp in enumerate(path):
        if isinstance(cur_data, (list, tuple)):
            if not isinstance(path_comp, int):
                raise TypeError(
                    f"Path component {path_comp!r} must be an integer index "
                    f"since data is a sequence: {cur_data!r}."
                )
            cur_data = cur_data[path_comp]
        elif isinstance(cur_data, Mapping):
            cur_data = cur_data[path_comp]
        else:
            raise ValueError(
                f"Data at path {path[:idx]} is not a sequence, but is of type "
                f"{type(cur_data)!r} and so sub-data cannot be extracted."
            )
    return cur_data


def set_in_container(cont, path, value):
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
