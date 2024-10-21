from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, Set, Dict

from hpcflow.sdk.log import TimeIt


@dataclass
class ObjectCache:
    """Class to bulk-retrieve and store elements, iterations, runs and their various
    dependencies."""

    elements: Optional[Dict] = None
    iterations: Optional[Dict] = None
    runs: Optional[Dict] = None

    run_dependencies: Optional[Dict[int, Set]] = None
    run_dependents: Optional[Dict[int, Set]] = None
    iter_run_dependencies: Optional[Dict[int, Set]] = None
    iter_iter_dependencies: Optional[Dict[int, Set]] = None
    elem_iter_dependencies: Optional[Dict[int, Set]] = None
    elem_elem_dependencies: Optional[Dict[int, Set]] = None
    elem_elem_dependents: Optional[Dict[int, Set]] = None
    elem_elem_dependents_rec: Optional[Dict[int, Set]] = None

    @classmethod
    @TimeIt.decorator
    def build(
        cls,
        workflow,
        dependencies=False,
        elements=False,
        iterations=False,
        runs=False,
    ):
        kwargs = {}
        if dependencies:
            kwargs.update(cls._get_dependencies(workflow))

        if elements:
            kwargs["elements"] = workflow.get_all_elements()

        if iterations:
            kwargs["iterations"] = workflow.get_all_element_iterations()

        if runs:
            kwargs["runs"] = workflow.get_all_EARs()

        return cls(**kwargs)

    @classmethod
    @TimeIt.decorator
    def _get_dependencies(cls, workflow):
        def _get_recursive_deps(elem_id, seen_ids=None):
            if seen_ids is None:
                seen_ids = [elem_id]
            elif elem_id in seen_ids:
                # stop recursion
                return set()
            else:
                seen_ids.append(elem_id)
            return set(elem_elem_dependents[elem_id]).union(
                [
                    j
                    for i in elem_elem_dependents[elem_id]
                    for j in _get_recursive_deps(i, seen_ids)
                    if j != elem_id
                ]
            )

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
        for i in list(elem_elem_dependents):
            elem_elem_dependents_rec[i] = _get_recursive_deps(i)

        # add missing keys:
        for k in range(num_elems):
            elem_elem_dependents[k]
            elem_elem_dependents_rec[k]

        elem_elem_dependents = dict(elem_elem_dependents)
        elem_elem_dependents_rec = dict(elem_elem_dependents_rec)

        return dict(
            run_dependencies=run_dependencies,
            run_dependents=run_dependents,
            iter_run_dependencies=iter_run_dependencies,
            iter_iter_dependencies=iter_iter_dependencies,
            elem_iter_dependencies=elem_iter_dependencies,
            elem_elem_dependencies=elem_elem_dependencies,
            elem_elem_dependents=elem_elem_dependents,
            elem_elem_dependents_rec=elem_elem_dependents_rec,
        )
