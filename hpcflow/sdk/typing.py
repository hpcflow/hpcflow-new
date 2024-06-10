from typing import Union
from pathlib import Path

PathLike = Union[str, Path, None]

# EAR: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
E_idx_type = tuple[int, int]
EI_idx_type = tuple[int, int, int]
EAR_idx_type = tuple[int, int, int, int, int]
