from typing import Union, TypeAlias
from pathlib import Path

PathLike: TypeAlias = Union[str, Path, None]

# EAR: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
E_idx_type: TypeAlias = tuple[int, int]
EI_idx_type: TypeAlias = tuple[int, int, int]
EAR_idx_type: TypeAlias = tuple[int, int, int, int, int]

ParamSource: TypeAlias = dict[str, str | int]
