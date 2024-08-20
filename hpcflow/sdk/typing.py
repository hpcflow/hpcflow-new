"""
Common type aliases.
"""
from typing import Tuple, TypeVar
from pathlib import Path

#: Type of a value that can be treated as a path.
PathLike = TypeVar("PathLike", str, Path, None)  # TODO: maybe don't need TypeVar?

#: Type of an element index:
#: (task_insert_ID, element_idx)
E_idx_type = Tuple[int, int]
#: Type of an element iteration index:
#: (task_insert_ID, element_idx, iteration_idx)
EI_idx_type = Tuple[int, int, int]
#: Type of an element action run index:
#: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
EAR_idx_type = Tuple[int, int, int, int, int]
