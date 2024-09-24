"""
Dependency resolution cache.
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Set, Dict

from hpcflow.sdk.log import TimeIt


@dataclass
class DependencyCache:
    """Class to bulk-retrieve dependencies between elements, iterations, and runs."""

    #: What EARs (by ID) a given EAR depends on.
    run_dependencies: Dict[int, Set]
    #: What EARs (by ID) are depending on a given EAR.
    run_dependents: Dict[int, Set]
    #: What EARs (by ID) a given iteration depends on.
    iter_run_dependencies: Dict[int, Set]
    #: What iterations (by ID) a given iteration depends on.
    iter_iter_dependencies: Dict[int, Set]
    #: What iterations (by ID) a given element depends on.
    elem_iter_dependencies: Dict[int, Set]
    #: What elements (by ID) a given element depends on.
    elem_elem_dependencies: Dict[int, Set]
    #: What elements (by ID) are depending on a given element.
    elem_elem_dependents: Dict[int, Set]
    #: Transitive closure of :py:attr:`elem_elem_dependents`.
    elem_elem_dependents_rec: Dict[int, Set]

    #: The elements of the workflow that this cache was built from.
    elements: Dict
    #: The iterations of the workflow that this cache was built from.
    iterations: Dict

    @classmethod
    @TimeIt.decorator
    def build(cls, workflow):
        """
        Build a cache instance.

        Parameters
        ----------
        workflow: ~hpcflow.app.Workflow
            The workflow to build the cache from.
        """
        num_iters = workflow.num_element_iterations
        num_elems = workflow.num_elements
        num_runs = workflow.num_EARs

        all_store_runs = workflow._store.get_EARs(list(range(num_runs)))
        all_store_iters = workflow._store.get_element_iterations(list(range(num_iters)))
        all_store_elements = workflow._store.get_elements(list(range(num_elems)))
        all_param_sources = workflow.get_all_parameter_sources()
        all_data_idx = [
            {
                k: v if isinstance(v, list) else [v]
                for k, v in i.data_idx.items()
                if k not in ("repeats.",)
            }
            for i in all_store_runs
        ]

        # run dependencies and dependents
        run_dependencies = {}
        run_dependents = defaultdict(set)
        for idx, i in enumerate(all_data_idx):
            run_i_sources = set()
            for j in i.values():
                for k in j:
                    run_k = all_param_sources[k].get("EAR_ID")
                    if run_k is not None and run_k != idx:
                        run_i_sources.add(run_k)
            run_dependencies[idx] = run_i_sources
            for m in run_i_sources:
                run_dependents[m].add(idx)

        # add missing:
        for k in range(num_runs):
            run_dependents[k]

        run_dependents = dict(run_dependents)

        # iteration dependencies
        all_iter_run_IDs = {
            i.id_: [k for j in i.EAR_IDs.values() for k in j] for i in all_store_iters
        }
        # for each iteration, which runs does it depend on?
        iter_run_dependencies = {
            k: set(j for i in v for j in run_dependencies[i])
            for k, v in all_iter_run_IDs.items()
        }

        # for each run, which iteration does it belong to?
        all_run_iter_IDs = {}
        for iter_ID, run_IDs in all_iter_run_IDs.items():
            for run_ID in run_IDs:
                all_run_iter_IDs[run_ID] = iter_ID

        # for each iteration, which iterations does it depend on?
        iter_iter_dependencies = {
            k: set(all_run_iter_IDs[i] for i in v)
            for k, v in iter_run_dependencies.items()
        }

        all_elem_iter_IDs = {i.id_: i.iteration_IDs for i in all_store_elements}

        elem_iter_dependencies = {
            k: set(j for i in v for j in iter_iter_dependencies[i])
            for k, v in all_elem_iter_IDs.items()
        }

        # for each iteration, which element does it belong to?
        all_iter_elem_IDs = {}
        for elem_ID, iter_IDs in all_elem_iter_IDs.items():
            for iter_ID in iter_IDs:
                all_iter_elem_IDs[iter_ID] = elem_ID

        # element dependencies
        elem_elem_dependencies = {
            k: set(all_iter_elem_IDs[i] for i in v)
            for k, v in elem_iter_dependencies.items()
        }

        # for each element, which elements depend on it (directly)?
        elem_elem_dependents = defaultdict(set)
        for k, v in elem_elem_dependencies.items():
            for i in v:
                elem_elem_dependents[i].add(k)

        # for each element, which elements depend on it (recursively)?
        elem_elem_dependents_rec = defaultdict(set)
        for k in list(elem_elem_dependents):
            for i in elem_elem_dependents[k]:
                elem_elem_dependents_rec[k].add(i)
                elem_elem_dependents_rec[k].update(
                    {m for m in elem_elem_dependents[i] if m != k}
                )

        # add missing keys:
        for k in range(num_elems):
            elem_elem_dependents[k]
            elem_elem_dependents_rec[k]

        elem_elem_dependents = dict(elem_elem_dependents)
        elem_elem_dependents_rec = dict(elem_elem_dependents_rec)

        elements = workflow.get_all_elements()
        iterations = workflow.get_all_element_iterations()

        return cls(
            run_dependencies=run_dependencies,
            run_dependents=run_dependents,
            iter_run_dependencies=iter_run_dependencies,
            iter_iter_dependencies=iter_iter_dependencies,
            elem_iter_dependencies=elem_iter_dependencies,
            elem_elem_dependencies=elem_elem_dependencies,
            elem_elem_dependents=elem_elem_dependents,
            elem_elem_dependents_rec=elem_elem_dependents_rec,
            elements=elements,
            iterations=iterations,
        )
