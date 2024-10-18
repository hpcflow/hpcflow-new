"""
Persistence model based on writing JSON documents.
"""

from __future__ import annotations

from contextlib import contextmanager
import copy
from datetime import datetime
import json
from pathlib import Path
from typing import cast, TYPE_CHECKING
from typing_extensions import override

from fsspec import filesystem, AbstractFileSystem  # type: ignore
from hpcflow.sdk.core.errors import (
    MissingParameterData,
    MissingStoreEARError,
    MissingStoreElementError,
    MissingStoreElementIterationError,
)
from hpcflow.sdk.persistence.base import (
    LoopDescriptor,
    PersistentStoreFeatures,
    PersistentStore,
    StoreEAR,
    StoreElement,
    StoreElementIter,
    StoreParameter,
    StoreTask,
    Metadata,
    StoreCreationInfo,
    update_param_source_dict,
    TemplateMeta,
)
from hpcflow.sdk.persistence.pending import CommitResourceMap
from hpcflow.sdk.persistence.store_resource import JSONFileStoreResource
from hpcflow.sdk.persistence.types import (
    ElemMeta,
    IterMeta,
    RunMeta,
    TaskMeta,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from typing import Any, ClassVar
    from typing_extensions import Self
    from ..app import BaseApp
    from ..core.json_like import JSONed, JSONDocument
    from ..core.workflow import Workflow
    from ..typing import ParamSource


class JsonStoreTask(StoreTask[TaskMeta]):
    """
    Persisted task that is serialized using JSON.
    """

    @override
    def encode(self) -> tuple[int, TaskMeta, dict[str, Any]]:
        """Prepare store task data for the persistent store."""
        assert self.task_template is not None
        wk_task: TaskMeta = {
            "id_": self.id_,
            "element_IDs": self.element_IDs,
            "index": self.index,
        }
        task = {"id_": self.id_, **self.task_template}
        return self.index, wk_task, task

    @override
    @classmethod
    def decode(cls, task_dat: TaskMeta) -> Self:
        """Initialise a `StoreTask` from store task data

        Note: the `task_template` is only needed for encoding because it is retrieved as
        part of the `WorkflowTemplate` so we don't need to load it when decoding.

        """
        return cls(is_pending=False, **task_dat)


class JsonStoreElement(StoreElement[ElemMeta, None]):
    """
    Persisted element that is serialized using JSON.
    """

    @override
    def encode(self, context: None) -> ElemMeta:
        """Prepare store element data for the persistent store."""
        dct = self.__dict__
        del dct["is_pending"]
        return cast(ElemMeta, dct)

    @override
    @classmethod
    def decode(cls, elem_dat: ElemMeta, context: None) -> Self:
        """Initialise a `JsonStoreElement` from store element data"""
        return cls(is_pending=False, **elem_dat)


class JsonStoreElementIter(StoreElementIter[IterMeta, None]):
    """
    Persisted element iteration that is serialized using JSON.
    """

    @override
    def encode(self, context: None) -> IterMeta:
        """Prepare store element iteration data for the persistent store."""
        dct = self.__dict__
        del dct["is_pending"]
        return cast(IterMeta, dct)

    @override
    @classmethod
    def decode(cls, iter_dat: IterMeta, context: None) -> Self:
        """Initialise a `JsonStoreElementIter` from persistent store element iteration data"""

        iter_dat = copy.deepcopy(iter_dat)  # to avoid mutating; can we avoid this?

        # cast JSON string keys to integers:
        EAR_IDs = iter_dat["EAR_IDs"]
        if EAR_IDs:
            for act_idx in list(EAR_IDs.keys()):
                EAR_IDs[int(act_idx)] = EAR_IDs.pop(act_idx)

        return cls(is_pending=False, **cast(dict, iter_dat))


class JsonStoreEAR(StoreEAR[RunMeta, None]):
    """
    Persisted element action run that is serialized using JSON.
    """

    @override
    def encode(self, ts_fmt: str, context: None) -> RunMeta:
        """Prepare store EAR data for the persistent store."""
        return {
            "id_": self.id_,
            "elem_iter_ID": self.elem_iter_ID,
            "action_idx": self.action_idx,
            "commands_idx": self.commands_idx,
            "data_idx": self.data_idx,
            "submission_idx": self.submission_idx,
            "success": self.success,
            "skip": self.skip,
            "start_time": self._encode_datetime(self.start_time, ts_fmt),
            "end_time": self._encode_datetime(self.end_time, ts_fmt),
            "snapshot_start": self.snapshot_start,
            "snapshot_end": self.snapshot_end,
            "exit_code": self.exit_code,
            "metadata": self.metadata,
            "run_hostname": self.run_hostname,
        }

    @override
    @classmethod
    def decode(cls, EAR_dat: RunMeta, ts_fmt: str, context: None) -> Self:
        """Initialise a `JsonStoreEAR` from persistent store EAR data"""
        # don't want to mutate EAR_dat:
        EAR_dat = copy.deepcopy(EAR_dat)
        start_time = cls._decode_datetime(EAR_dat.pop("start_time"), ts_fmt)
        end_time = cls._decode_datetime(EAR_dat.pop("end_time"), ts_fmt)
        return cls(
            is_pending=False,
            **cast(dict, EAR_dat),
            start_time=start_time,
            end_time=end_time,
        )


class JSONPersistentStore(
    PersistentStore[
        JsonStoreTask,
        JsonStoreElement,
        JsonStoreElementIter,
        JsonStoreEAR,
        StoreParameter,
    ]
):
    """
    A store that writes JSON files for all its state serialization.
    """

    _name: ClassVar[str] = "json"
    _features: ClassVar[PersistentStoreFeatures] = PersistentStoreFeatures(
        create=True,
        edit=True,
        jobscript_parallelism=False,
        EAR_parallelism=False,
        schedulers=True,
        submission=True,
    )

    _meta_res: ClassVar[str] = "metadata"
    _params_res: ClassVar[str] = "parameters"
    _subs_res: ClassVar[str] = "submissions"

    _res_file_names: ClassVar[Mapping[str, str]] = {
        _meta_res: "metadata.json",
        _params_res: "parameters.json",
        _subs_res: "submissions.json",
    }

    _res_map: ClassVar[CommitResourceMap] = CommitResourceMap(
        commit_tasks=(_meta_res,),
        commit_loops=(_meta_res,),
        commit_loop_num_iters=(_meta_res,),
        commit_loop_parents=(_meta_res,),
        commit_submissions=(_subs_res,),
        commit_submission_parts=(_subs_res,),
        commit_js_metadata=(_subs_res,),
        commit_elem_IDs=(_meta_res,),
        commit_elements=(_meta_res,),
        commit_elem_iter_IDs=(_meta_res,),
        commit_elem_iters=(_meta_res,),
        commit_loop_indices=(_meta_res,),
        commit_elem_iter_EAR_IDs=(_meta_res,),
        commit_EARs_initialised=(_meta_res,),
        commit_EARs=(_meta_res,),
        commit_EAR_submission_indices=(_meta_res,),
        commit_EAR_skips=(_meta_res,),
        commit_EAR_starts=(_meta_res,),
        commit_EAR_ends=(_meta_res,),
        commit_template_components=(_meta_res,),
        commit_parameters=(_params_res,),
        commit_param_sources=(_params_res,),
    )

    @classmethod
    def _store_task_cls(cls) -> type[JsonStoreTask]:
        return JsonStoreTask

    @classmethod
    def _store_elem_cls(cls) -> type[JsonStoreElement]:
        return JsonStoreElement

    @classmethod
    def _store_iter_cls(cls) -> type[JsonStoreElementIter]:
        return JsonStoreElementIter

    @classmethod
    def _store_EAR_cls(cls) -> type[JsonStoreEAR]:
        return JsonStoreEAR

    @classmethod
    def _store_param_cls(cls) -> type[StoreParameter]:
        return StoreParameter

    def __init__(
        self, app, workflow: Workflow | None, path: Path, fs: AbstractFileSystem
    ):
        self._resources = {
            self._meta_res: self._get_store_resource(app, "metadata", path, fs),
            self._params_res: self._get_store_resource(app, "parameters", path, fs),
            self._subs_res: self._get_store_resource(app, "submissions", path, fs),
        }
        super().__init__(app, workflow, path, fs)

    @contextmanager
    def cached_load(self) -> Iterator[None]:
        """Context manager to cache the metadata."""
        with self.using_resource("metadata", "read"):
            yield

    def remove_replaced_dir(self) -> None:
        """
        Remove the directory containing replaced workflow details.
        """
        with self.using_resource("metadata", "update") as md:
            if "replaced_workflow" in md:
                assert self.fs is not None
                self.remove_path(md["replaced_workflow"])
                self.logger.debug("removing temporarily renamed pre-existing workflow.")
                del md["replaced_workflow"]

    def reinstate_replaced_dir(self) -> None:
        """
        Reinstate the directory containing replaced workflow details.
        """
        with self.using_resource("metadata", "read") as md:
            if "replaced_workflow" in md:
                assert self.fs is not None
                self.logger.debug(
                    "reinstating temporarily renamed pre-existing workflow."
                )
                self.rename_path(md["replaced_workflow"], self.path)

    @classmethod
    def _get_store_resource(
        cls, app: BaseApp, name: str, path: str | Path, fs: AbstractFileSystem
    ) -> JSONFileStoreResource:
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
        app: BaseApp,
        *,
        template_js: TemplateMeta,
        template_components_js: dict[str, Any],
        wk_path: str,
        fs: AbstractFileSystem,
        name: str,
        replaced_wk: str | None,
        creation_info: StoreCreationInfo,
        ts_fmt: str,
        ts_name_fmt: str,
    ) -> None:
        """
        Write an empty persistent workflow.
        """
        fs.mkdir(wk_path)
        submissions: list[None] = []
        parameters: dict[str, dict[None, None]] = {
            "data": {},
            "sources": {},
        }
        metadata: Metadata = {
            "name": name,
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

    def _append_tasks(self, tasks: Iterable[StoreTask]):
        with self.using_resource("metadata", action="update") as md:
            assert "tasks" in md and "template" in md and "num_added_tasks" in md
            for i in tasks:
                idx, wk_task_i, task_i = i.encode()
                md["tasks"].insert(idx, cast(TaskMeta, wk_task_i))
                md["template"]["tasks"].insert(idx, task_i)
                md["num_added_tasks"] += 1

    def _append_loops(self, loops: dict[int, LoopDescriptor]):
        with self.using_resource("metadata", action="update") as md:
            assert "loops" in md and "template" in md
            for loop_idx, loop in loops.items():
                md["loops"].append(
                    {
                        "num_added_iterations": loop["num_added_iterations"],
                        "iterable_parameters": loop["iterable_parameters"],
                        "parents": loop["parents"],
                    }
                )
                md["template"]["loops"].append(loop["loop_template"])

    def _append_submissions(self, subs: dict[int, JSONDocument]):
        with self.using_resource("submissions", action="update") as subs_res:
            subs_res.extend(subs.values())

    def _append_task_element_IDs(self, task_ID: int, elem_IDs: list[int]):
        with self.using_resource("metadata", action="update") as md:
            assert "tasks" in md
            md["tasks"][task_ID]["element_IDs"].extend(elem_IDs)

    def _append_elements(self, elems: Sequence[JsonStoreElement]):
        with self.using_resource("metadata", action="update") as md:
            assert "elements" in md
            md["elements"].extend(i.encode(None) for i in elems)

    def _append_element_sets(self, task_id: int, es_js: Sequence[Mapping]):
        task_idx = self._get_task_id_to_idx_map()[task_id]
        with self.using_resource("metadata", "update") as md:
            assert "template" in md
            md["template"]["tasks"][task_idx]["element_sets"].extend(es_js)

    def _append_elem_iter_IDs(self, elem_ID: int, iter_IDs: Iterable[int]):
        with self.using_resource("metadata", action="update") as md:
            assert "elements" in md
            md["elements"][elem_ID]["iteration_IDs"].extend(iter_IDs)

    def _append_elem_iters(self, iters: Sequence[JsonStoreElementIter]):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            md["iters"].extend(i.encode(None) for i in iters)

    def _append_elem_iter_EAR_IDs(
        self, iter_ID: int, act_idx: int, EAR_IDs: Sequence[int]
    ):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            md["iters"][iter_ID].setdefault("EAR_IDs", {}).setdefault(act_idx, []).extend(
                EAR_IDs
            )

    def _update_elem_iter_EARs_initialised(self, iter_ID: int):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            md["iters"][iter_ID]["EARs_initialised"] = True

    def _append_submission_parts(self, sub_parts: dict[int, dict[str, list[int]]]):
        with self.using_resource("submissions", action="update") as subs_res:
            for sub_idx, sub_i_parts in sub_parts.items():
                sub = subs_res[sub_idx]
                assert isinstance(sub, dict)
                for dt_str, parts_j in sub_i_parts.items():
                    sub["submission_parts"][dt_str] = parts_j

    def _update_loop_index(self, iter_ID: int, loop_idx: dict[str, int]):
        with self.using_resource("metadata", action="update") as md:
            assert "iters" in md
            md["iters"][iter_ID]["loop_idx"].update(loop_idx)

    def _update_loop_num_iters(self, index: int, num_iters: list[list[list[int] | int]]):
        with self.using_resource("metadata", action="update") as md:
            assert "loops" in md
            md["loops"][index]["num_added_iterations"] = num_iters

    def _update_loop_parents(self, index: int, parents: list[str]):
        with self.using_resource("metadata", action="update") as md:
            assert "loops" in md
            md["loops"][index]["parents"] = parents

    def _append_EARs(self, EARs: Sequence[JsonStoreEAR]):
        with self.using_resource("metadata", action="update") as md:
            assert "runs" in md
            md["runs"].extend(i.encode(self.ts_fmt, None) for i in EARs)

    def _update_EAR_submission_indices(self, sub_indices: Mapping[int, int]):
        with self.using_resource("metadata", action="update") as md:
            assert "runs" in md
            for EAR_ID_i, sub_idx_i in sub_indices.items():
                md["runs"][EAR_ID_i]["submission_idx"] = sub_idx_i

    def _update_EAR_start(
        self, EAR_id: int, s_time: datetime, s_snap: dict[str, Any], s_hn: str
    ):
        with self.using_resource("metadata", action="update") as md:
            assert "runs" in md
            md["runs"][EAR_id]["start_time"] = s_time.strftime(self.ts_fmt)
            md["runs"][EAR_id]["snapshot_start"] = s_snap
            md["runs"][EAR_id]["run_hostname"] = s_hn

    def _update_EAR_end(
        self,
        EAR_id: int,
        e_time: datetime,
        e_snap: dict[str, Any],
        ext_code: int,
        success: bool,
    ):
        with self.using_resource("metadata", action="update") as md:
            assert "runs" in md
            md["runs"][EAR_id]["end_time"] = e_time.strftime(self.ts_fmt)
            md["runs"][EAR_id]["snapshot_end"] = e_snap
            md["runs"][EAR_id]["exit_code"] = ext_code
            md["runs"][EAR_id]["success"] = success

    def _update_EAR_skip(self, EAR_id: int):
        with self.using_resource("metadata", action="update") as md:
            assert "runs" in md
            md["runs"][EAR_id]["skip"] = True

    def _update_js_metadata(self, js_meta: dict[int, dict[int, dict[str, Any]]]):
        with self.using_resource("submissions", action="update") as sub_res:
            for sub_idx, all_js_md in js_meta.items():
                sub = cast("dict[str, list[dict[str, Any]]]", sub_res[sub_idx])
                for js_idx, js_meta_i in all_js_md.items():
                    sub_i = sub["jobscripts"][js_idx]
                    sub_i.update(**js_meta_i)

    def _append_parameters(self, params: Sequence[StoreParameter]):
        with self.using_resource("parameters", "update") as params_u:
            for param_i in params:
                params_u["data"][str(param_i.id_)] = param_i.encode()
                params_u["sources"][str(param_i.id_)] = param_i.source

    def _set_parameter_values(self, set_parameters: dict[int, tuple[Any, bool]]):
        """Set multiple unset persistent parameters."""
        param_ids = list(set_parameters.keys())
        param_objs = self._get_persistent_parameters(param_ids)
        with self.using_resource("parameters", "update") as params:
            for param_id, (value, is_file) in set_parameters.items():
                param_i = param_objs[param_id]
                if is_file:
                    param_i = param_i.set_file(value)
                else:
                    param_i = param_i.set_data(value)
                params["data"][str(param_id)] = param_i.encode()

    def _update_parameter_sources(self, sources: Mapping[int, ParamSource]):
        """Update the sources of multiple persistent parameters."""

        param_ids = list(sources.keys())
        param_objs = self._get_persistent_parameters(param_ids)

        with self.using_resource("parameters", "update") as params:
            # no need to update data array:
            for p_id, src_i in sources.items():
                param_i = param_objs[p_id]
                new_src_i = update_param_source_dict(param_i.source, src_i)
                params["sources"][str(p_id)] = new_src_i

    def _update_template_components(self, tc: dict[str, Any]):
        with self.using_resource("metadata", "update") as md:
            md["template_components"] = tc

    def _get_num_persistent_tasks(self) -> int:
        """Get the number of persistent tasks."""
        if self.use_cache and self.num_tasks_cache is not None:
            num = self.num_tasks_cache
        else:
            with self.using_resource("metadata", action="read") as md:
                assert "tasks" in md
                num = len(md["tasks"])
        if self.use_cache and self.num_tasks_cache is None:
            self.num_tasks_cache = num
        return num

    def _get_num_persistent_loops(self) -> int:
        """Get the number of persistent loops."""
        with self.using_resource("metadata", action="read") as md:
            assert "loops" in md
            return len(md["loops"])

    def _get_num_persistent_submissions(self) -> int:
        """Get the number of persistent submissions."""
        with self.using_resource("submissions", "read") as subs_res:
            return len(subs_res)

    def _get_num_persistent_elements(self) -> int:
        """Get the number of persistent elements."""
        with self.using_resource("metadata", action="read") as md:
            assert "elements" in md
            return len(md["elements"])

    def _get_num_persistent_elem_iters(self) -> int:
        """Get the number of persistent element iterations."""
        with self.using_resource("metadata", action="read") as md:
            assert "iters" in md
            return len(md["iters"])

    def _get_num_persistent_EARs(self) -> int:
        """Get the number of persistent EARs."""
        if self.use_cache and self.num_EARs_cache is not None:
            num = self.num_EARs_cache
        else:
            with self.using_resource("metadata", action="read") as md:
                assert "runs" in md
                num = len(md["runs"])
        if self.use_cache and self.num_EARs_cache is None:
            self.num_EARs_cache = num
        return num

    def _get_num_persistent_parameters(self) -> int:
        with self.using_resource("parameters", "read") as params:
            return len(params["data"])

    def _get_num_persistent_added_tasks(self) -> int:
        with self.using_resource("metadata", "read") as md:
            assert "num_added_tasks" in md
            return md["num_added_tasks"]

    @classmethod
    def make_test_store_from_spec(
        cls,
        app: BaseApp,
        spec,
        dir=None,
        path="test_store.json",
        overwrite=False,
        ts_fmt="%d/%m/%Y, %H:%M:%S",  # FIXME: use the right default timestamp format
    ):
        """Generate an store for testing purposes."""

        tasks_, elems, elem_iters, EARs = super().prepare_test_store_from_spec(spec)

        path_ = Path(path).resolve()
        tasks = [JsonStoreTask(**i).encode() for i in tasks_]
        elements_ = [JsonStoreElement(**i).encode(None) for i in elems]
        elem_iters_ = [JsonStoreElementIter(**i).encode(None) for i in elem_iters]
        EARs_ = [JsonStoreEAR(**i).encode(ts_fmt, None) for i in EARs]

        persistent_data = {
            "tasks": tasks,
            "elements": elements_,
            "iters": elem_iters_,
            "runs": EARs_,
        }

        path_ = Path(dir or "", path_)
        with path_.open("wt") as fp:
            json.dump(persistent_data, fp, indent=2)

        return cls(app=app, workflow=None, path=path_, fs=filesystem("file"))

    def _get_persistent_template_components(self) -> dict[str, Any]:
        with self.using_resource("metadata", "read") as md:
            assert "template_components" in md
            return md["template_components"]

    def _get_persistent_template(self) -> dict[str, JSONed]:
        with self.using_resource("metadata", "read") as md:
            assert "template" in md
            return cast("dict[str, JSONed]", md["template"])

    def _get_persistent_tasks(self, id_lst: Iterable[int]) -> dict[int, JsonStoreTask]:
        tasks, id_lst = self._get_cached_persistent_tasks(id_lst)
        if id_lst:
            with self.using_resource("metadata", action="read") as md:
                assert "tasks" in md
                new_tasks = {
                    i["id_"]: JsonStoreTask.decode({**i, "index": idx})
                    for idx, i in enumerate(md["tasks"])
                    if id_lst is None or i["id_"] in id_lst
                }
                self.task_cache.update(new_tasks)
                tasks.update(new_tasks)
        return tasks

    def _get_persistent_loops(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, LoopDescriptor]:
        with self.using_resource("metadata", "read") as md:
            assert "loops" in md
            return {
                idx: cast(LoopDescriptor, i)
                for idx, i in enumerate(md["loops"])
                if id_lst is None or idx in id_lst
            }

    def _get_persistent_submissions(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, JSONDocument]:
        with self.using_resource("submissions", "read") as sub_res:
            subs_dat = copy.deepcopy(
                {
                    idx: i
                    for idx, i in enumerate(sub_res)
                    if id_lst is None or idx in id_lst
                }
            )
            # cast jobscript submit-times and jobscript `task_elements` keys:
            for sub in subs_dat.values():
                js: dict[str, dict[str | int, Any]]
                assert isinstance(sub, dict)
                for js in sub["jobscripts"]:
                    for key in list(te := js["task_elements"]):
                        te[int(key)] = te.pop(key)

        return subs_dat

    def _get_persistent_elements(
        self, id_lst: Iterable[int]
    ) -> dict[int, JsonStoreElement]:
        elems, id_lst_ = self._get_cached_persistent_elements(id_lst)
        if id_lst_:
            # could convert `id_lst` to e.g. slices if more efficient for a given store
            with self.using_resource("metadata", action="read") as md:
                try:
                    if "elements" not in md:
                        raise KeyError
                    elem_dat = {i: md["elements"][i] for i in id_lst_}
                except KeyError:
                    raise MissingStoreElementError(id_lst_)
                new_elems = {
                    k: JsonStoreElement.decode(v, None) for k, v in elem_dat.items()
                }
                self.element_cache.update(new_elems)
                elems.update(new_elems)
        return elems

    def _get_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> dict[int, JsonStoreElementIter]:
        iters, id_lst_ = self._get_cached_persistent_element_iters(id_lst)
        if id_lst_:
            with self.using_resource("metadata", action="read") as md:
                try:
                    if "iters" not in md:
                        raise KeyError
                    iter_dat = {i: md["iters"][i] for i in id_lst_}
                except KeyError:
                    raise MissingStoreElementIterationError(id_lst_)
                new_iters = {
                    k: JsonStoreElementIter.decode(v, None) for k, v in iter_dat.items()
                }
                self.element_iter_cache.update(new_iters)
                iters.update(new_iters)
        return iters

    def _get_persistent_EARs(self, id_lst: Iterable[int]) -> dict[int, JsonStoreEAR]:
        runs, id_lst_ = self._get_cached_persistent_EARs(id_lst)
        if id_lst_:
            with self.using_resource("metadata", action="read") as md:
                try:
                    if "runs" not in md:
                        raise KeyError
                    EAR_dat = {i: md["runs"][i] for i in id_lst_}
                except KeyError:
                    raise MissingStoreEARError(id_lst_)
                new_runs = {
                    k: JsonStoreEAR.decode(v, self.ts_fmt, None)
                    for k, v in EAR_dat.items()
                }
                self.EAR_cache.update(new_runs)
                runs.update(new_runs)
        return runs

    def _get_persistent_parameters(
        self, id_lst: Iterable[int], **kwargs
    ) -> Mapping[int, StoreParameter]:
        params, id_lst_ = self._get_cached_persistent_parameters(id_lst)
        if id_lst_:
            with self.using_resource("parameters", "read") as params_:
                try:
                    param_dat = {i: params_["data"][str(i)] for i in id_lst_}
                    src_dat = {i: params_["sources"][str(i)] for i in id_lst_}
                except KeyError:
                    raise MissingParameterData(id_lst_)

            new_params = {
                k: StoreParameter.decode(id_=k, data=v, source=src_dat[k])
                for k, v in param_dat.items()
            }
            self.parameter_cache.update(new_params)
            params.update(new_params)
        return params

    def _get_persistent_param_sources(
        self, id_lst: Iterable[int]
    ) -> dict[int, ParamSource]:
        sources, id_lst_ = self._get_cached_persistent_param_sources(id_lst)
        if id_lst_:
            with self.using_resource("parameters", "read") as params:
                try:
                    new_sources = {i: params["sources"][str(i)] for i in id_lst_}
                except KeyError:
                    raise MissingParameterData(id_lst_)
            self.param_sources_cache.update(new_sources)
            sources.update(new_sources)
        return sources

    def _get_persistent_parameter_set_status(
        self, id_lst: Iterable[int]
    ) -> dict[int, bool]:
        with self.using_resource("parameters", "read") as params:
            try:
                param_dat = {i: params["data"][str(i)] for i in id_lst}
            except KeyError:
                raise MissingParameterData(id_lst)
        return {k: v is not None for k, v in param_dat.items()}

    def _get_persistent_parameter_IDs(self) -> list[int]:
        with self.using_resource("parameters", "read") as params:
            return list(int(i) for i in params["data"].keys())

    def get_ts_fmt(self) -> str:
        """
        Get the format for timestamps.
        """
        with self.using_resource("metadata", action="read") as md:
            assert "ts_fmt" in md
            return md["ts_fmt"]

    def get_ts_name_fmt(self) -> str:
        """
        Get the format for timestamps to use in names.
        """
        with self.using_resource("metadata", action="read") as md:
            assert "ts_name_fmt" in md
            return md["ts_name_fmt"]

    def get_creation_info(self) -> StoreCreationInfo:
        """
        Get information about the creation of the workflow.
        """
        with self.using_resource("metadata", action="read") as md:
            assert "creation_info" in md
            return copy.deepcopy(md["creation_info"])

    def get_name(self) -> str:
        """
        Get the name of the workflow.
        """
        with self.using_resource("metadata", action="read") as md:
            assert "name" in md
            return md["name"]

    def zip(
        self,
        path: str = ".",
        log: str | None = None,
        overwrite=False,
        include_execute=False,
        include_rechunk_backups=False,
    ) -> str:
        raise TypeError("unsupported operation: zipping-json")

    def unzip(self, path: str = ".", log: str | None = None) -> str:
        raise TypeError("unsupported operation: unzipping-json")

    def rechunk_parameter_base(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Any:
        raise TypeError("unsupported operation: rechunk-json")

    def rechunk_runs(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Any:
        raise TypeError("unsupported operation: rechunk-json")
