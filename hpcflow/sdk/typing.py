from typing import Tuple, TypeVar
from pathlib import Path

PathLike = TypeVar("PathLike", str, Path, None)  # TODO: maybe don't need TypeVar?

# EAR: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
E_idx_type = Tuple[int, int]
EI_idx_type = Tuple[int, int, int]
EAR_idx_type = Tuple[int, int, int, int, int]
