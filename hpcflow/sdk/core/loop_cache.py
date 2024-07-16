from __future__ import annotations
from dataclasses import dataclass
from collections import defaultdict
from typing import TYPE_CHECKING

from hpcflow.sdk.core.utils import nth_key
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.core.cache import DependencyCache

if TYPE_CHECKING:
    from typing import TypedDict
    from ..compat.typing import Self
    from ..typing import DataIndex
    from .loop import Loop
    from .parameters import InputSource
    from .task import WorkflowTask, InputStatus
    from .workflow import Workflow

    class ElementDescriptor(TypedDict):
        input_statuses: dict[str, InputStatus]
        input_sources: dict[str, InputSource]
        task_insert_ID: int

    class _DependentDescriptor(TypedDict):
        group_names: tuple[str, ...]

    class DependentDescriptor(_DependentDescriptor, total=False):
        pass


@dataclass
class LoopCache:
    """Class to store a cache for use in `Workflow.add_empty_loop` and
    `WorkflowLoop.add_iterations`.

    Attributes
    ----------
    element_dependents
        Keys are element IDs, values are dicts whose keys are element IDs that depend on
        the key element ID (via `Element.get_dependent_elements_recursively`), and whose
        values are dicts with keys: `group_names`, which is a tuple of the string group
        names associated with the dependent element's element set.
    elements
        Keys are element IDs, values are dicts with keys: `input_statuses`,
        `input_sources`, and `task_insert_ID`.
    zeroth_iters
        Keys are element IDs, values are data associated with the zeroth iteration of that
        element, namely a tuple of iteration ID and `ElementIteration.data_idx`.
    data_idx
        Keys are element IDs, values are data associated with all iterations of that
        element, namely a dict whose keys are the iteration loop index as a tuple, and
        whose values are data indices via `ElementIteration.get_data_idx()`.
    iterations
        Keys are iteration IDs, values are tuples of element ID and iteration index within
        that element.
    task_iterations
        Keys are task insert IDs, values are list of all iteration IDs associated with
        that task.

    """

    element_dependents: dict[int, dict[int, DependentDescriptor]]
    elements: dict[int, ElementDescriptor]
    zeroth_iters: dict[int, tuple[int, DataIndex]]
    data_idx: dict[int, dict[tuple[tuple[str, int], ...], DataIndex]]
    iterations: dict[int, tuple[int, int]]
    task_iterations: dict[int, list[int]]

    @TimeIt.decorator
    def get_iter_IDs(self, loop: Loop) -> list[int]:
        """Retrieve a list of iteration IDs belonging to a given loop."""
        return [j for i in loop.task_insert_IDs for j in self.task_iterations[i]]

    @TimeIt.decorator
    def get_iter_loop_indices(self, iter_IDs: list[int]) -> list[dict[str, int]]:
        iter_loop_idx: list[dict[str, int]] = []
        for i in iter_IDs:
            elem_id, idx = self.iterations[i]
            iter_loop_idx.append(dict(nth_key(self.data_idx[elem_id], idx)))
        return iter_loop_idx

    @TimeIt.decorator
    def update_loop_indices(self, new_loop_name: str, iter_IDs: list[int]):
        elem_ids = {v[0] for k, v in self.iterations.items() if k in iter_IDs}
        for i in elem_ids:
            new_item: dict[tuple[tuple[str, int], ...], DataIndex] = {}
            for k, v in self.data_idx[i].items():
                new_k = dict(k)
                new_k[new_loop_name] = 0
                new_item[tuple(sorted(new_k.items()))] = v
            self.data_idx[i] = new_item

    @TimeIt.decorator
    def add_iteration(
        self,
        iter_ID: int,
        task_insert_ID: int,
        element_ID: int,
        loop_idx: dict[str, int],
        data_idx: DataIndex,
    ):
        """Update the cache to include a newly added iteration."""
        self.task_iterations[task_insert_ID].append(iter_ID)
        new_iter_idx = len(self.data_idx[element_ID])
        self.data_idx[element_ID][tuple(sorted(loop_idx.items()))] = data_idx
        self.iterations[iter_ID] = (element_ID, new_iter_idx)

    @classmethod
    @TimeIt.decorator
    def build(cls, workflow: Workflow, loops: list[Loop] | None = None) -> Self:
        """Build a cache of data for use in adding loops and iterations."""

        deps_cache = DependencyCache.build(workflow)

        loops = list(workflow.template.loops or []) + (loops or [])
        task_iIDs = set(j for i in loops for j in i.task_insert_IDs)
        tasks: list[WorkflowTask] = [
            workflow.tasks.get(insert_ID=i) for i in sorted(task_iIDs)
        ]
        elem_deps: dict[int, dict[int, DependentDescriptor]] = {}

        # keys: element IDs, values: dict with keys: tuple(loop_idx), values: data index
        data_idx_cache: dict[int, dict[tuple[tuple[str, int], ...], DataIndex]] = {}

        # keys: iteration IDs, values: tuple of (element ID, integer index into values
        # dict in `data_idx_cache` [accessed via `.keys()[index]`])
        iters: dict[int, tuple[int, int]] = {}

        # keys: element IDs, values: dict with keys: "input_statues", "input_sources",
        # "task_insert_ID":
        elements: dict[int, ElementDescriptor] = {}

        zeroth_iters: dict[int, tuple[int, DataIndex]] = {}
        task_iterations = defaultdict(list)
        for task in tasks:
            for elem_idx in task.element_IDs:
                element = deps_cache.elements[elem_idx]
                inp_statuses = task.template.get_input_statuses(element.element_set)
                elements[element.id_] = {
                    "input_statuses": inp_statuses,
                    "input_sources": element.input_sources,
                    "task_insert_ID": task.insert_ID,
                }
                elem_deps[element.id_] = {
                    i: {
                        "group_names": tuple(
                            j.name for j in deps_cache.elements[i].element_set.groups
                        ),
                    }
                    for i in deps_cache.elem_elem_dependents_rec[element.id_]
                }
                elem_iters: dict[tuple[tuple[str, int], ...], DataIndex] = {}
                for idx, iter_i in enumerate(element.iterations):
                    if idx == 0:
                        zeroth_iters[element.id_] = (iter_i.id_, iter_i.data_idx)
                    loop_idx_key = tuple(sorted(iter_i.loop_idx.items()))
                    elem_iters[loop_idx_key] = iter_i.get_data_idx()
                    task_iterations[task.insert_ID].append(iter_i.id_)
                    iters[iter_i.id_] = (element.id_, idx)
                data_idx_cache[element.id_] = elem_iters

        return cls(
            element_dependents=elem_deps,
            elements=elements,
            zeroth_iters=zeroth_iters,
            data_idx=data_idx_cache,
            iterations=iters,
            task_iterations=dict(task_iterations),
        )
