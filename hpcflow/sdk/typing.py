from typing import TypeVar
from pathlib import Path

PathLike = TypeVar("PathLike", str, Path, None)  # TODO: maybe don't need TypeVar?

# EAR: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
E_idx_type = tuple[int, int]
EI_idx_type = tuple[int, int, int]
EAR_idx_type = tuple[int, int, int, int, int]
