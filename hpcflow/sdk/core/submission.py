from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np
from numpy.typing import NDArray


def generate_EAR_resource_map(
    task: WorkflowTask,
) -> Tuple[List[int], List[ElementResources], NDArray]:
    """Generate an integer array whose rows represent actions and columns represent task
    elements and whose values index unique resources."""
    # TODO: work in progress
    none_val = -1
    resources = []
    resource_hashes = []
    resource_map = np.ones((task.num_actions, task.num_elements), dtype=int) * none_val
    for element in task.elements:
        for act_idx, action in element.actions.items():
            EAR = action.runs[-1]
            res_hash = hash(EAR.resources)
            if res_hash not in resource_hashes:
                resource_hashes.append(res_hash)
                resources.append(EAR.resources)
            resource_map[act_idx][EAR.element_index] = resource_hashes.index(res_hash)

    return resources, resource_hashes, resource_map


def allocate_jobscripts(resource_map: NDArray, none_val: Any = -1):
    # TODO: work in progress
    resource_map = np.asanyarray(resource_map)
    resource_idx = np.unique(resource_map)
    jobscripts = []
    allocated = np.zeros_like(resource_map)
    js_map = np.ones_like(resource_map, dtype=float) * np.nan
    nones_bool = resource_map == none_val
    stop = False
    for act_idx in range(resource_map.shape[0]):

        for res_i in resource_idx:

            if res_i == none_val:
                continue

            if res_i not in resource_map[act_idx]:
                continue

            # print(f"{act_idx=}; {res_i=}")

            resource_map[nones_bool] = res_i
            diff = np.cumsum(np.abs(np.diff(resource_map[act_idx:], axis=0)), axis=0)

            elem_bool = np.logical_and(
                resource_map[act_idx] == res_i, allocated[act_idx] == False
            )
            elem_idx = np.where(elem_bool)[0]
            act_elem_bool = np.logical_and(elem_bool, nones_bool[act_idx] == False)
            act_elem_idx = np.where(act_elem_bool)

            # print(f"\t{elem_idx=}")
            # print(f"\t{act_elem_idx=}")

            # add elements from downstream actions:
            ds_bool = np.logical_and(
                diff[:, elem_idx] == 0,
                nones_bool[act_idx + 1 :, elem_idx] == False,
            )
            ds_act_idx, ds_elem_idx = np.where(ds_bool)
            ds_act_idx += act_idx + 1
            ds_elem_idx = elem_idx[ds_elem_idx]

            # print(f"\t{ds_act_idx=}")
            # print(f"\t{ds_elem_idx=}")

            EARs_by_elem = {k.item(): [act_idx] for k in act_elem_idx[0]}
            for ds_a, ds_e in zip(ds_act_idx, ds_elem_idx):
                if ds_e not in EARs_by_elem:
                    EARs_by_elem[ds_e] = []
                EARs_by_elem[ds_e].append(ds_a)

            EARs = np.vstack([np.ones_like(act_elem_idx) * act_idx, act_elem_idx])
            EARs = np.hstack([EARs, np.array([ds_act_idx, ds_elem_idx])])

            print(f"{EARs=}")

            if not EARs.size:
                continue

            js = {
                "resources": res_i,
                "EARs": dict(sorted(EARs_by_elem.items(), key=lambda x: x[0])),
            }
            allocated[EARs[0], EARs[1]] = True
            js_map[EARs[0], EARs[1]] = len(jobscripts)
            jobscripts.append(js)

            if np.all(allocated[~nones_bool]):
                stop = True
                break

        if stop:
            break

    # jobscripts = sorted(jobscripts, key=lambda x: x["resources"])
    resource_map[nones_bool] = none_val

    return jobscripts, js_map


def collate_jobscript_EARs(EAR_resource_groups: Dict[int : Dict[int:List]]) -> List[Dict]:
    # TODO: remove this nonsense
    def _merge_downstream_actions(
        EAR_resource_groups,
        act_idx,
        elem_idx,
        res_hash,
        added_ears,
        new_js,
        depth=0,
    ):
        print(f"{'   ' * depth}_merge_downstream_actions: {act_idx=}; {elem_idx=}")
        stop_search = False
        # search for first appearance of this element in downstream actions
        for ds_act_idx, ds_res_groups in EAR_resource_groups.items():
            if stop_search:
                break
            if ds_act_idx <= act_idx:
                continue

            print(f"{'   ' * depth}  {ds_act_idx=}")

            for ds_res_hash, ds_elems in ds_res_groups.items():
                if elem_idx in ds_elems:
                    if ds_res_hash == res_hash:
                        if ds_act_idx in added_ears[elem_idx]:
                            continue
                        if elem_idx not in new_js["EARs"]:
                            new_js["EARs"][elem_idx] = []
                        new_js["EARs"][elem_idx].append(ds_act_idx)
                        added_ears[elem_idx].append(ds_act_idx)
                        print(f"{'   ' * depth}  adding act {ds_act_idx}")
                        _merge_downstream_actions(
                            EAR_resource_groups,
                            ds_act_idx,
                            elem_idx,
                            res_hash,
                            added_ears,
                            new_js,
                            depth=depth + 1,
                        )
                    stop_search = True
                    break

    added_ears = {}
    jobscripts = []
    for act_idx, res_groups in EAR_resource_groups.items():
        for res_hash, elems in res_groups.items():
            EARs = {}
            for i in elems:
                if i not in added_ears:
                    added_ears[i] = []
                if act_idx not in added_ears[i]:
                    EARs[i] = [act_idx]
                added_ears[i].append(act_idx)

            if not EARs:
                continue

            new_js = {
                "resources": res_hash,
                "EARs": EARs,
            }
            # try to merge downstream action elements with this jobscript:
            for elem_i in elems:
                _merge_downstream_actions(
                    EAR_resource_groups,
                    act_idx,
                    elem_i,
                    res_hash,
                    added_ears,
                    new_js,
                )

            jobscripts.append(new_js)
    return jobscripts
