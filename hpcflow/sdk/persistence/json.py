from __future__ import annotations
from contextlib import contextmanager
import copy
from datetime import datetime
import json
from pathlib import Path

from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from fsspec import filesystem
from hpcflow.sdk.core.errors import (
    MissingParameterData,
    MissingStoreEARError,
    MissingStoreElementError,
    MissingStoreElementIterationError,
    MissingStoreTaskError,
)
from hpcflow.sdk.persistence.base import (
    PersistentStoreFeatures,
    PersistentStore,
    StoreEAR,
    StoreElement,
    StoreElementIter,
    StoreParameter,
    StoreTask,
)
from hpcflow.sdk.persistence.pending import CommitResourceMap
from hpcflow.sdk.persistence.store_resource import JSONFileStoreResource


class JSONPersistentStore(PersistentStore):
    _name = "json"
    _features = PersistentStoreFeatures(
        create=True,
        edit=True,
        jobscript_parallelism=False,
        EAR_parallelism=False,
        schedulers=True,
        submission=True,
    )

    _meta_res = "metadata"
    _params_res = "parameters"
    _subs_res = "submissions"

    _res_file_names = {
        _meta_res: "metadata.json",
        _params_res: "parameters.json",
        _subs_res: "submissions.json",
    }

    _res_map = CommitResourceMap(
        commit_tasks=(_meta_res,),
        commit_loops=(_meta_res,),
        commit_loop_num_iters=(_meta_res,),
        commit_submissions=(_subs_res,),
        commit_submission_parts=(_subs_res,),
        commit_js_metadata=(_subs_res,),
        commit_elem_IDs=(_meta_res,),
        commit_elements=(_meta_res,),
        commit_elem_iter_IDs=(_meta_res,),
        commit_elem_iters=(_meta_res,),
        commit_loop_indices=(_meta_res,),
        commit_elem_iter_EAR_IDs=(_meta_res,),
        commit_EARs=(_meta_res,),
        commit_EAR_submission_indices=(_meta_res,),
        commit_EAR_skips=(_meta_res,),
        commit_EAR_starts=(_meta_res,),
        commit_EAR_ends=(_meta_res,),
        commit_template_components=(_meta_res,),
        commit_parameters=(_params_res,),
        commit_param_sources=(_params_res,),
    )

    def __init__(self, app, workflow, path, fs):
        self._resources = {
            self._meta_res: self._get_store_resource(app, "metadata", path, fs),
            self._params_res: self._get_store_resource(app, "parameters", path, fs),
            self._subs_res: self._get_store_resource(app, "submissions", path, fs),
        }
        super().__init__(app, workflow, path, fs)

    @contextmanager
    def cached_load(self) -> Iterator[Dict]:
        """Context manager to cache the metadata."""
        with self.using_resource("metadata", "read") as md:
            yield md

    def remove_replaced_dir(self) -> None:
        with self.using_resource("metadata", "update") as md:
            if "replaced_workflow" in md:
                self.remove_path(md["replaced_workflow"], self.fs)
                self.logger.debug("removing temporarily renamed pre-existing workflow.")
                md["replaced_workflow"] = None

    def reinstate_replaced_dir(self) -> None:
        with self.using_resource("metadata", "read") as md:
            if "replaced_workflow" in md:
                self.logger.debug(
                    "reinstating temporarily renamed pre-existing workflow."
                )
                self.rename_path(md["replaced_workflow"], self.path, self.fs)

    @classmethod
    def _get_store_resource(cls, app, name, path, fs):
        return JSONFileStoreResource(
            app=app,
            name=name,
            path=path,
            fs=fs,
            filename=cls._res_file_names[name],
        )

    @classmethod
    def write_empty_workflow(
        cls,
        app,
        template_js: Dict,
        template_components_js: Dict,
        wk_path: str,
        fs,
        fs_path: str,
        replaced_wk: str,
        creation_info: Dict,
        ts_fmt: str,
        ts_name_fmt: str,
    ) -> None:
        fs.mkdir(wk_path)
        submissions = []
        parameters = {
            "data": {},
            "sources": {},
        }
        metadata = {
            "fs_path": fs_path,
            "ts_fmt": ts_fmt,
            "ts_name_fmt": ts_name_fmt,
            "creation_info": creation_info,
            "template_components": template_components_js,
            "template": template_js,
            "tasks": [],
            "elements": [],
            "iters": [],
            "runs": [],
            "num_added_tasks": 0,
            "loops": [],
        }
        if replaced_wk:
            metadata["replaced_workflow"] = replaced_wk

        cls._get_store_resource(app, "metadata", wk_path, fs)._dump(metadata)
        cls._get_store_resource(app, "parameters", wk_path, fs)._dump(parameters)
        cls._get_store_resource(app, "submissions", wk_path, fs)._dump(submissions)

    def _append_tasks(self, tasks: List[StoreTask]):
        with self.using_resource("metadata", action="update") as md:
            for i in tasks:
                idx, wk_task_i, task_i = i.encode()
                md["tasks"].insert(idx, wk_task_i)
                md["template"]["tasks"].insert(idx, task_i)
                md["num_added_tasks"] += 1

    def _append_loops(self, loops: Dict[int, Dict]):
        with self.using_resource("metadata", action="update") as md:
            for loop_idx, loop in loops.items():
                md["loops"].append(
                    {
                        "num_added_iterations": loop["num_added_iterations"],
                        "iterable_parameters": loop["iterable_parameters"],
                    }
                )
                md["template"]["loops"].append(loop["loop_template"])

    def _append_submissions(self, subs: Dict[int, Dict]):
        with self.using_resource("submissions", action="update") as subs_res:
            for sub_idx, sub_i in subs.items():
                subs_res.append(sub_i)

    def _append_task_element_IDs(self, task_ID: int, elem_IDs: List[int]):
        with self.using_resource("metadata", action="update") as md:
            md["tasks"][task_ID]["element_IDs"].extend(elem_IDs)

    def _append_elements(self, elems: List[StoreElement]):
        with self.using_resource("metadata", action="update") as md:
            md["elements"].extend(i.encode() for i in elems)

    def _append_element_sets(self, task_id: int, es_js: List[Dict]):
        task_idx = self._get_task_id_to_idx_map()[task_id]
        with self.using_resource("metadata", "update") as md:
            md["template"]["tasks"][task_idx]["element_sets"].extend(es_js)

    def _append_elem_iter_IDs(self, elem_ID: int, iter_IDs: List[int]):
        with self.using_resource("metadata", action="update") as md:
            md["elements"][elem_ID]["iteration_IDs"].extend(iter_IDs)

    def _append_elem_iters(self, iters: List[StoreElementIter]):
        with self.using_resource("metadata", action="update") as md:
            md["iters"].extend(i.encode() for i in iters)

    def _append_elem_iter_EAR_IDs(self, iter_ID: int, act_idx: int, EAR_IDs: List[int]):
        with self.using_resource("metadata", action="update") as md:
            if md["iters"][iter_ID]["EAR_IDs"] is None:
                md["iters"][iter_ID]["EAR_IDs"] = {}
            if act_idx not in md["iters"][iter_ID]["EAR_IDs"]:
                md["iters"][iter_ID]["EAR_IDs"][act_idx] = []
            md["iters"][iter_ID]["EAR_IDs"][act_idx].extend(EAR_IDs)

    def _append_submission_parts(self, sub_parts: Dict[int, Dict[str, List[int]]]):
        with self.using_resource("submissions", action="update") as subs_res:
            for sub_idx, sub_i_parts in sub_parts.items():
                for dt_str, parts_j in sub_i_parts.items():
                    subs_res[sub_idx]["submission_parts"][dt_str] = parts_j

    def _update_loop_index(self, iter_ID: int, loop_idx: Dict):
        with self.using_resource("metadata", action="update") as md:
            md["iters"][iter_ID]["loop_idx"].update(loop_idx)

    def _update_loop_num_iters(self, index: int, num_iters: int):
        with self.using_resource("metadata", action="update") as md:
            md["loops"][index]["num_added_iterations"] = num_iters

    def _append_EARs(self, EARs: List[StoreEAR]):
        with self.using_resource("metadata", action="update") as md:
            md["runs"].extend(i.encode(self.ts_fmt) for i in EARs)

    def _update_EAR_submission_index(self, EAR_id: int, sub_idx: int):
        with self.using_resource("metadata", action="update") as md:
            md["runs"][EAR_id]["submission_idx"] = sub_idx

    def _update_EAR_start(self, EAR_id: int, s_time: datetime, s_snap: Dict):
        with self.using_resource("metadata", action="update") as md:
            md["runs"][EAR_id]["start_time"] = s_time.strftime(self.ts_fmt)
            md["runs"][EAR_id]["snapshot_start"] = s_snap

    def _update_EAR_end(
        self, EAR_id: int, e_time: datetime, e_snap: Dict, ext_code: int, success: bool
    ):
        with self.using_resource("metadata", action="update") as md:
            md["runs"][EAR_id]["end_time"] = e_time.strftime(self.ts_fmt)
            md["runs"][EAR_id]["snapshot_end"] = e_snap
            md["runs"][EAR_id]["exit_code"] = ext_code
            md["runs"][EAR_id]["success"] = success

    def _update_EAR_skip(self, EAR_id: int):
        with self.using_resource("metadata", action="update") as md:
            md["runs"][EAR_id]["skip"] = True

    def _update_js_metadata(self, js_meta: Dict):
        with self.using_resource("submissions", action="update") as sub_res:
            for sub_idx, all_js_md in js_meta.items():
                for js_idx, js_meta_i in all_js_md.items():
                    sub_res[sub_idx]["jobscripts"][js_idx].update(**js_meta_i)

    def _append_parameters(self, new_params: List[StoreParameter]):
        with self.using_resource("parameters", "update") as params:
            for param_i in new_params:
                params["data"][str(param_i.id_)] = param_i.encode()
                params["sources"][str(param_i.id_)] = param_i.source

    def _set_parameter_value(self, param_id: int, value: Any, is_file: bool):
        """Set an unset persistent parameter."""

        # the `decode` call in `_get_persistent_parameters` should be quick:
        param = self._get_persistent_parameters([param_id])[param_id]
        if is_file:
            param = param.set_file(value)
        else:
            param = param.set_data(value)

        with self.using_resource("parameters", "update") as params:
            # no need to update sources array:
            params["data"][str(param_id)] = param.encode()

    def _update_parameter_source(self, param_id: int, src: Dict):
        """Update the source of a persistent parameter."""

        param = self._get_persistent_parameters([param_id])[param_id]
        param = param.update_source(src)

        with self.using_resource("parameters", "update") as params:
            # no need to update data array:
            params["sources"][str(param_id)] = param.source

    def _update_template_components(self, tc: Dict):
        with self.using_resource("metadata", "update") as md:
            md["template_components"] = tc

    def _get_num_persistent_tasks(self) -> int:
        """Get the number of persistent tasks."""
        with self.using_resource("metadata", action="read") as md:
            return len(md["tasks"])

    def _get_num_persistent_loops(self) -> int:
        """Get the number of persistent loops."""
        with self.using_resource("metadata", action="read") as md:
            return len(md["loops"])

    def _get_num_persistent_submissions(self) -> int:
        """Get the number of persistent submissions."""
        with self.using_resource("submissions", "read") as subs_res:
            return len(subs_res)

    def _get_num_persistent_elements(self) -> int:
        """Get the number of persistent elements."""
        with self.using_resource("metadata", action="read") as md:
            return len(md["elements"])

    def _get_num_persistent_elem_iters(self) -> int:
        """Get the number of persistent element iterations."""
        with self.using_resource("metadata", action="read") as md:
            return len(md["iters"])

    def _get_num_persistent_EARs(self) -> int:
        """Get the number of persistent EARs."""
        with self.using_resource("metadata", action="read") as md:
            return len(md["runs"])

    def _get_num_persistent_parameters(self):
        with self.using_resource("parameters", "read") as params:
            return len(params["data"])

    def _get_num_persistent_added_tasks(self):
        with self.using_resource("metadata", "read") as md:
            return md["num_added_tasks"]

    @classmethod
    def make_test_store_from_spec(
        cls,
        app,
        spec,
        dir=None,
        path="test_store.json",
        overwrite=False,
    ):
        """Generate an store for testing purposes."""

        tasks, elems, elem_iters, EARs = super().prepare_test_store_from_spec(spec)

        path = Path(path).resolve()
        tasks = [StoreTask(**i).encode() for i in tasks]
        elements = [StoreElement(**i).encode() for i in elems]
        elem_iters = [StoreElementIter(**i).encode() for i in elem_iters]
        EARs = [StoreEAR(**i).encode() for i in EARs]

        persistent_data = {
            "tasks": tasks,
            "elements": elements,
            "iters": elem_iters,
            "runs": EARs,
        }

        path = Path(dir or "", path)
        with path.open("wt") as fp:
            json.dump(persistent_data, fp, indent=2)

        return cls(app=app, workflow=None, path=path, fs=filesystem("file"))

    def _get_persistent_template_components(self):
        with self.using_resource("metadata", "read") as md:
            return md["template_components"]

    def _get_persistent_template(self) -> Dict:
        with self.using_resource("metadata", "read") as md:
            return md["template"]

    def _get_persistent_tasks(
        self, id_lst: Optional[Iterable[int]] = None
    ) -> Dict[int, StoreTask]:
        with self.using_resource("metadata", action="read") as md:
            task_dat = {
                i["id_"]: StoreTask.decode({**i, "index": idx})
                for idx, i in enumerate(md["tasks"])
                if id_lst is None or i["id_"] in id_lst
            }
        return task_dat

    def _get_persistent_loops(self, id_lst: Optional[Iterable[int]] = None):
        with self.using_resource("metadata", "read") as md:
            loop_dat = {
                idx: i
                for idx, i in enumerate(md["loops"])
                if id_lst is None or idx in id_lst
            }
        return loop_dat

    def _get_persistent_submissions(self, id_lst: Optional[Iterable[int]] = None):
        with self.using_resource("submissions", "read") as sub_res:
            subs_dat = copy.deepcopy(
                {
                    idx: i
                    for idx, i in enumerate(sub_res)
                    if id_lst is None or idx in id_lst
                }
            )
            # cast jobscript submit-times and jobscript `task_elements` keys:
            for sub_idx, sub in subs_dat.items():
                for js_idx, js in enumerate(sub["jobscripts"]):
                    for key in list(js["task_elements"].keys()):
                        subs_dat[sub_idx]["jobscripts"][js_idx]["task_elements"][
                            int(key)
                        ] = subs_dat[sub_idx]["jobscripts"][js_idx]["task_elements"].pop(
                            key
                        )

        return subs_dat

    def _get_persistent_elements(self, id_lst: Iterable[int]) -> Dict[int, StoreElement]:
        # could convert `id_lst` to e.g. slices if more efficient for a given store
        with self.using_resource("metadata", action="read") as md:
            try:
                elem_dat = {i: md["elements"][i] for i in id_lst}
            except KeyError:
                raise MissingStoreElementError(id_lst) from None
            return {k: StoreElement.decode(v) for k, v in elem_dat.items()}

    def _get_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> Dict[int, StoreElementIter]:
        with self.using_resource("metadata", action="read") as md:
            try:
                iter_dat = {i: md["iters"][i] for i in id_lst}
            except KeyError:
                raise MissingStoreElementIterationError(id_lst) from None
            return {k: StoreElementIter.decode(v) for k, v in iter_dat.items()}

    def _get_persistent_EARs(self, id_lst: Iterable[int]) -> Dict[int, StoreEAR]:
        with self.using_resource("metadata", action="read") as md:
            try:
                EAR_dat = {i: md["runs"][i] for i in id_lst}
            except KeyError:
                raise MissingStoreEARError(id_lst) from None
            return {k: StoreEAR.decode(v, self.ts_fmt) for k, v in EAR_dat.items()}

    def _get_persistent_parameters(
        self,
        id_lst: Iterable[int],
    ) -> Dict[int, StoreParameter]:
        with self.using_resource("parameters", "read") as params:
            try:
                param_dat = {i: params["data"][str(i)] for i in id_lst}
                src_dat = {i: params["sources"][str(i)] for i in id_lst}
            except KeyError:
                raise MissingParameterData(id_lst) from None

        return {
            k: StoreParameter.decode(id_=k, data=v, source=src_dat[k])
            for k, v in param_dat.items()
        }

    def _get_persistent_param_sources(self, id_lst: Iterable[int]) -> Dict[int, Dict]:
        with self.using_resource("parameters", "read") as params:
            try:
                return {i: params["sources"][str(i)] for i in id_lst}
            except KeyError:
                raise MissingParameterData(id_lst) from None

    def _get_persistent_parameter_set_status(
        self, id_lst: Iterable[int]
    ) -> Dict[int, bool]:
        with self.using_resource("parameters", "read") as params:
            try:
                param_dat = {i: params["data"][str(i)] for i in id_lst}
            except KeyError:
                raise MissingParameterData(id_lst) from None
        return {k: v is not None for k, v in param_dat.items()}

    def _get_persistent_parameter_IDs(self) -> List[int]:
        with self.using_resource("parameters", "read") as params:
            return list(int(i) for i in params["data"].keys())

    def get_ts_fmt(self):
        with self.using_resource("metadata", action="read") as md:
            return md["ts_fmt"]

    def get_ts_name_fmt(self):
        with self.using_resource("metadata", action="read") as md:
            return md["ts_name_fmt"]

    def get_creation_info(self):
        with self.using_resource("metadata", action="read") as md:
            return copy.deepcopy(md["creation_info"])

    def get_fs_path(self):
        with self.using_resource("metadata", action="read") as md:
            return md["fs_path"]
