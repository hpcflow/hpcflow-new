# Store* classes represent the element-metadata in the store, in a store-agnostic way
from __future__ import annotations
from abc import ABC, abstractmethod

import contextlib
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import enum
from logging import Logger
import os
from pathlib import Path
import shutil
import socket
import time
from typing import Generic, TypeVar, TypedDict, cast, overload, TYPE_CHECKING

from hpcflow.sdk.core.utils import (
    flatten,
    get_in_container,
    get_relative_path,
    remap,
    reshape,
    set_in_container,
    parse_timestamp,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.persistence.pending import PendingChanges
from hpcflow.sdk.persistence.types import (
    AnySTask,
    AnySElement,
    AnySElementIter,
    AnySEAR,
    AnySParameter,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from contextlib import AbstractContextManager
    from typing import Any, Dict, ClassVar, Final, Literal
    from fsspec import AbstractFileSystem  # type: ignore
    from ..compat.typing import NotRequired, Self, TypeGuard
    from .pending import CommitResourceMap
    from .store_resource import StoreResource
    from ..app import BaseApp
    from ..typing import DataIndex, PathLike, ParamSource
    from ..core.json_like import JSONed
    from ..core.loop import IterableParam
    from ..core.parameters import ParameterValue
    from ..core.workflow import Workflow

T = TypeVar("T")
T2 = TypeVar("T2")
CTX = TypeVar("CTX")

PRIMITIVES = (
    int,
    float,
    str,
    type(None),
)

TEMPLATE_COMP_TYPES = (
    "parameters",
    "command_files",
    "environments",
    "task_schemas",
)

PARAM_DATA_NOT_SET: Final[int] = 0


def update_param_source_dict(source: ParamSource, update: ParamSource) -> ParamSource:
    return cast("ParamSource", dict(sorted({**source, **update}.items())))


class StoreCreationInfo(TypedDict):
    app_info: dict[str, Any]
    create_time: str
    id: str


class ElemMeta(TypedDict):
    """
    The kwargs supported for a StoreElement.
    """

    id_: int
    index: int
    es_idx: int
    seq_idx: dict[str, int]
    src_idx: dict[str, int]
    task_ID: int
    iteration_IDs: list[int]


class IterMeta(TypedDict):
    """
    The kwargs supported for a StoreElementIter.
    """

    data_idx: DataIndex
    EAR_IDs: dict[int, list[int]]
    EARs_initialised: bool
    element_ID: int
    loop_idx: dict[str, int]
    schema_parameters: list[str]


class LoopMeta(TypedDict):
    """
    Component of Metadata.
    """

    num_added_iterations: list[list[list[int] | int]]
    parents: list[str]
    iterable_parameters: dict[str, IterableParam]


class RunMeta(TypedDict):
    """
    The kwargs supported for StoreEAR.
    """

    id_: int
    elem_iter_ID: int
    action_idx: int
    commands_idx: list[int]
    data_idx: DataIndex
    metadata: Metadata | None
    end_time: NotRequired[str | None]
    exit_code: int | None
    start_time: NotRequired[str | None]
    snapshot_start: dict[str, Any] | None
    snapshot_end: dict[str, Any] | None
    submission_idx: int | None
    run_hostname: str | None
    success: bool | None
    skip: bool


class TaskMeta(TypedDict):
    id_: int
    index: int
    element_IDs: list[int]


class TemplateMeta(TypedDict):  # FIXME: Incomplete, see WorkflowTemplate
    loops: list[dict]
    tasks: list[dict]


class Metadata(TypedDict):
    creation_info: NotRequired[StoreCreationInfo]
    elements: NotRequired[list[ElemMeta]]
    iters: NotRequired[list[IterMeta]]
    loops: NotRequired[list[LoopMeta]]
    name: NotRequired[str]
    num_added_tasks: NotRequired[int]
    replaced_workflow: NotRequired[str]
    runs: NotRequired[list[RunMeta]]
    tasks: NotRequired[list[TaskMeta]]
    template: NotRequired[TemplateMeta]
    template_components: NotRequired[dict[str, Any]]
    ts_fmt: NotRequired[str]
    ts_name_fmt: NotRequired[str]


@dataclass
class PersistentStoreFeatures:
    """Class to represent the features provided by a persistent store.

    Parameters
    ----------
    create
        If True, a new workflow can be created using this store.
    edit
        If True, the workflow can be modified.
    jobscript_parallelism
        If True, the store supports workflows running multiple independent jobscripts
        simultaneously.
    EAR_parallelism
        If True, the store supports workflows running multiple EARs simultaneously.
    schedulers
        If True, the store supports submitting workflows to a scheduler
    submission
        If True, the store supports submission. If False, the store can be considered to
        be an archive, which would need transforming to another store type before
        submission.
    """

    create: bool = False
    edit: bool = False
    jobscript_parallelism: bool = False
    EAR_parallelism: bool = False
    schedulers: bool = False
    submission: bool = False


@dataclass
class StoreTask(Generic[T]):
    id_: int
    index: int
    is_pending: bool
    element_IDs: list[int]
    task_template: Mapping[str, Any] | None = None

    @abstractmethod
    def encode(self) -> tuple[int, T, dict[str, Any]]:
        """Prepare store task data for the persistent store."""

    @classmethod
    @abstractmethod
    def decode(cls, task_dat: T) -> Self:
        """Initialise a `StoreTask` from store task data

        Note: the `task_template` is only needed for encoding because it is retrieved as
        part of the `WorkflowTemplate` so we don't need to load it when decoding.

        """

    @TimeIt.decorator
    def append_element_IDs(self, pend_IDs: list[int]) -> Self:
        """Return a copy, with additional element IDs."""
        return self.__class__(
            id_=self.id_,
            index=self.index,
            is_pending=self.is_pending,
            element_IDs=[*self.element_IDs, *pend_IDs],
            task_template=self.task_template,
        )


@dataclass
class StoreElement(Generic[T, CTX]):
    """
    Type Parameters
    ---------------
    T
        Type of the serialized form.
    CTX
        Type of the encoding and decoding context.

    Parameters
    ----------
    index
        Index of the element within its parent task.
    iteration_IDs
        IDs of element-iterations that belong to this element.
    """

    id_: int
    is_pending: bool
    index: int
    es_idx: int
    seq_idx: dict[str, int]
    src_idx: dict[str, int]
    task_ID: int
    iteration_IDs: list[int]

    @abstractmethod
    def encode(self, context: CTX) -> T:
        """Prepare store element data for the persistent store."""

    @classmethod
    @abstractmethod
    def decode(cls, elem_dat: T, context: CTX) -> Self:
        """Initialise a `StoreElement` from store element data"""

    def to_dict(self, iters) -> dict[str, Any]:
        """Prepare data for the user-facing `Element` object."""
        return {
            "id_": self.id_,
            "is_pending": self.is_pending,
            "index": self.index,
            "es_idx": self.es_idx,
            "seq_idx": self.seq_idx,
            "src_idx": self.src_idx,
            "iteration_IDs": self.iteration_IDs,
            "task_ID": self.task_ID,
            "iterations": iters,
        }

    @TimeIt.decorator
    def append_iteration_IDs(self, pend_IDs: Iterable[int]) -> Self:
        """Return a copy, with additional iteration IDs."""
        iter_IDs = [*self.iteration_IDs, *pend_IDs]
        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            index=self.index,
            es_idx=self.es_idx,
            seq_idx=self.seq_idx,
            src_idx=self.src_idx,
            task_ID=self.task_ID,
            iteration_IDs=iter_IDs,
        )


@dataclass
class StoreElementIter(Generic[T, CTX]):
    """
    Type Parameters
    ---------------
    T
        Type of the serialized form.
    CTX
        Type of the encoding and decoding context.

    Parameters
    ----------
    data_idx
        Overall data index for the element-iteration, which maps parameter names to
        parameter data indices.
    EAR_IDs
        Maps task schema action indices to EARs by ID.
    schema_parameters
        List of parameters defined by the associated task schema.
    """

    id_: int
    is_pending: bool
    element_ID: int
    EARs_initialised: bool
    EAR_IDs: dict[int, list[int]] | None
    data_idx: DataIndex
    schema_parameters: list[str]
    loop_idx: dict[str, int] = field(default_factory=dict)

    @abstractmethod
    def encode(self, context: CTX) -> T:
        """Prepare store element iteration data for the persistent store."""

    @classmethod
    @abstractmethod
    def decode(cls, iter_dat: T, context: CTX) -> Self:
        """Initialise a `StoreElementIter` from persistent store element iteration data"""

    def to_dict(self, EARs: dict[int, dict[str, Any]] | None) -> dict[str, Any]:
        """Prepare data for the user-facing `ElementIteration` object."""
        return {
            "id_": self.id_,
            "is_pending": self.is_pending,
            "element_ID": self.element_ID,
            "EAR_IDs": self.EAR_IDs,
            "data_idx": self.data_idx,
            "schema_parameters": self.schema_parameters,
            "EARs": EARs,
            "EARs_initialised": self.EARs_initialised,
            "loop_idx": self.loop_idx,
        }

    @TimeIt.decorator
    def append_EAR_IDs(self, pend_IDs: Mapping[int, Sequence[int]]) -> Self:
        """Return a copy, with additional EAR IDs."""

        EAR_IDs = copy.deepcopy(self.EAR_IDs) or {}
        for act_idx, IDs_i in pend_IDs.items():
            EAR_IDs.setdefault(act_idx, []).extend(IDs_i)

        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            element_ID=self.element_ID,
            EAR_IDs=EAR_IDs,
            data_idx=self.data_idx,
            schema_parameters=self.schema_parameters,
            loop_idx=self.loop_idx,
            EARs_initialised=self.EARs_initialised,
        )

    @TimeIt.decorator
    def update_loop_idx(self, loop_idx: dict[str, int]) -> Self:
        """Return a copy, with the loop index updated."""
        loop_idx_new = copy.deepcopy(self.loop_idx)
        loop_idx_new.update(loop_idx)
        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            element_ID=self.element_ID,
            EAR_IDs=self.EAR_IDs,
            data_idx=self.data_idx,
            schema_parameters=self.schema_parameters,
            EARs_initialised=self.EARs_initialised,
            loop_idx=loop_idx_new,
        )

    @TimeIt.decorator
    def set_EARs_initialised(self) -> Self:
        """Return a copy with `EARs_initialised` set to `True`."""
        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            element_ID=self.element_ID,
            EAR_IDs=self.EAR_IDs,
            data_idx=self.data_idx,
            schema_parameters=self.schema_parameters,
            loop_idx=self.loop_idx,
            EARs_initialised=True,
        )


@dataclass
class StoreEAR(Generic[T, CTX]):
    """
    Type Parameters
    ---------------
    T
        Type of the serialized form.
    CTX
        Type of the encoding and decoding context.

    Parameters
    ----------
    data_idx
        Maps parameter names within this EAR to parameter data indices.
    metadata
        Metadata concerning e.g. the state of the EAR.
    action_idx
        The task schema action associated with this EAR.
    """

    id_: int
    is_pending: bool
    elem_iter_ID: int
    action_idx: int
    commands_idx: list[int]
    data_idx: DataIndex
    submission_idx: int | None = None
    skip: bool = False
    success: bool | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    snapshot_start: dict[str, Any] | None = None
    snapshot_end: dict[str, Any] | None = None
    exit_code: int | None = None
    metadata: Metadata | None = None
    run_hostname: str | None = None

    @staticmethod
    def _encode_datetime(dt: datetime | None, ts_fmt: str) -> str | None:
        return dt.strftime(ts_fmt) if dt else None

    @staticmethod
    def _decode_datetime(dt_str: str | None, ts_fmt: str) -> datetime | None:
        return parse_timestamp(dt_str, ts_fmt) if dt_str else None

    @abstractmethod
    def encode(self, ts_fmt: str, context: CTX) -> T:
        """Prepare store EAR data for the persistent store."""

    @classmethod
    @abstractmethod
    def decode(cls, EAR_dat: T, ts_fmt: str, context: CTX) -> Self:
        """Initialise a `StoreEAR` from persistent store EAR data"""

    def to_dict(self) -> dict[str, Any]:
        """Prepare data for the user-facing `ElementActionRun` object."""

        def _process_datetime(dt: datetime | None) -> datetime | None:
            """We store datetime objects implicitly in UTC, so we need to first make
            that explicit, and then convert to the local time zone."""
            return dt.replace(tzinfo=timezone.utc).astimezone() if dt else None

        return {
            "id_": self.id_,
            "is_pending": self.is_pending,
            "elem_iter_ID": self.elem_iter_ID,
            "action_idx": self.action_idx,
            "commands_idx": self.commands_idx,
            "data_idx": self.data_idx,
            "submission_idx": self.submission_idx,
            "success": self.success,
            "skip": self.skip,
            "start_time": _process_datetime(self.start_time),
            "end_time": _process_datetime(self.end_time),
            "snapshot_start": self.snapshot_start,
            "snapshot_end": self.snapshot_end,
            "exit_code": self.exit_code,
            "metadata": self.metadata,
            "run_hostname": self.run_hostname,
        }

    @TimeIt.decorator
    def update(
        self,
        submission_idx: int | None = None,
        skip: bool | None = None,
        success: bool | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        snapshot_start: dict[str, Any] | None = None,
        snapshot_end: dict[str, Any] | None = None,
        exit_code: int | None = None,
        run_hostname: str | None = None,
    ) -> Self:
        """Return a shallow copy, with specified data updated."""

        sub_idx = submission_idx if submission_idx is not None else self.submission_idx
        skip = skip if skip is not None else self.skip
        success = success if success is not None else self.success
        start_time = start_time if start_time is not None else self.start_time
        end_time = end_time if end_time is not None else self.end_time
        snap_s = snapshot_start if snapshot_start is not None else self.snapshot_start
        snap_e = snapshot_end if snapshot_end is not None else self.snapshot_end
        exit_code = exit_code if exit_code is not None else self.exit_code
        run_hn = run_hostname if run_hostname is not None else self.run_hostname

        return self.__class__(
            id_=self.id_,
            is_pending=self.is_pending,
            elem_iter_ID=self.elem_iter_ID,
            action_idx=self.action_idx,
            commands_idx=self.commands_idx,
            data_idx=self.data_idx,
            metadata=self.metadata,
            submission_idx=sub_idx,
            skip=skip,
            success=success,
            start_time=start_time,
            end_time=end_time,
            snapshot_start=snap_s,
            snapshot_end=snap_e,
            exit_code=exit_code,
            run_hostname=run_hn,
        )


class _TypeLookup(TypedDict, total=False):
    tuples: list[list[int]]
    sets: list[list[int]]


class EncodedStoreParameter(TypedDict):
    data: Any
    type_lookup: _TypeLookup


@dataclass
@hydrate
class StoreParameter:
    id_: int
    is_pending: bool
    is_set: bool
    data: ParameterValue | list | tuple | set | dict | int | float | str | None | Any
    file: Dict | None
    source: ParamSource

    _encoders: ClassVar[dict[type, Callable]] = {}
    _decoders: ClassVar[dict[str, Callable]] = {}

    def encode(self, **kwargs) -> dict[str, Any] | int:
        """Prepare store parameter data for the persistent store."""
        if self.is_set:
            if self.file:
                return {"file": self.file}
            else:
                return cast(dict, self._encode(obj=self.data, **kwargs))
        else:
            return PARAM_DATA_NOT_SET

    @staticmethod
    def __is_parameter_value(value) -> TypeGuard[ParameterValue]:
        # avoid circular import of `ParameterValue` until needed...
        from ..core.parameters import ParameterValue as PV

        return isinstance(value, PV)

    def _init_type_lookup(self) -> _TypeLookup:
        return cast(
            _TypeLookup,
            {
                "tuples": [],
                "sets": [],
                **{k: [] for k in self._decoders.keys()},
            },
        )

    def _encode(
        self,
        obj: ParameterValue | list | tuple | set | dict | int | float | str | None | Any,
        path: list[int] | None = None,
        type_lookup: _TypeLookup | None = None,
        **kwargs,
    ) -> EncodedStoreParameter:
        """Recursive encoder."""

        path = path or []
        if type_lookup is None:
            type_lookup = self._init_type_lookup()

        if len(path) > 50:
            raise RuntimeError("I'm in too deep!")

        if self.__is_parameter_value(obj):
            encoded = self._encode(
                obj=obj.to_dict(),
                path=path,
                type_lookup=type_lookup,
                **kwargs,
            )
            data, type_lookup = encoded["data"], encoded["type_lookup"]

        elif isinstance(obj, (list, tuple, set)):
            data = []
            for idx, item in enumerate(obj):
                encoded = self._encode(
                    obj=item,
                    path=path + [idx],
                    type_lookup=type_lookup,
                    **kwargs,
                )
                item, type_lookup = encoded["data"], encoded["type_lookup"]
                assert type_lookup is not None
                data.append(item)

            if isinstance(obj, tuple):
                type_lookup["tuples"].append(path)

            elif isinstance(obj, set):
                type_lookup["sets"].append(path)

        elif isinstance(obj, dict):
            assert type_lookup is not None
            data = {}
            for dct_key, dct_val in obj.items():
                encoded = self._encode(
                    obj=dct_val,
                    path=path + [dct_key],
                    type_lookup=type_lookup,
                    **kwargs,
                )
                dct_val, type_lookup = encoded["data"], encoded["type_lookup"]
                assert type_lookup is not None
                data[dct_key] = dct_val

        elif isinstance(obj, PRIMITIVES):
            data = obj

        elif type(obj) in self._encoders:
            assert type_lookup is not None
            data = self._encoders[type(obj)](
                obj=obj,
                path=path,
                type_lookup=type_lookup,
                **kwargs,
            )

        elif isinstance(obj, enum.Enum):
            data = obj.value

        else:
            raise ValueError(
                f"Parameter data with type {type(obj)} cannot be serialised into a "
                f"{self.__class__.__name__}: {obj}."
            )

        return {"data": data, "type_lookup": type_lookup}

    @classmethod
    def decode(
        cls,
        id_: int,
        data: dict[str, Any] | Literal[0] | None,
        source: ParamSource,
        *,
        path: list[str] | None = None,
        **kwargs,
    ) -> Self:
        """Initialise from persistent store parameter data."""
        if data and "file" in data:
            return cls(
                id_=id_,
                data=None,
                file=data["file"],
                is_set=True,
                source=source,
                is_pending=False,
            )
        elif not isinstance(data, dict):
            # parameter is not set
            return cls(
                id_=id_,
                data=None,
                file=None,
                is_set=False,
                source=source,
                is_pending=False,
            )

        data_ = cast(EncodedStoreParameter, data)
        path = path or []

        obj = get_in_container(data_["data"], path)

        for tuple_path in data_["type_lookup"]["tuples"]:
            try:
                rel_path = get_relative_path(tuple_path, path)
            except ValueError:
                continue
            if rel_path:
                set_in_container(obj, rel_path, tuple(get_in_container(obj, rel_path)))
            else:
                obj = tuple(obj)

        for set_path in data_["type_lookup"]["sets"]:
            try:
                rel_path = get_relative_path(set_path, path)
            except ValueError:
                continue
            if rel_path:
                set_in_container(obj, rel_path, set(get_in_container(obj, rel_path)))
            else:
                obj = set(obj)

        for data_type in cls._decoders:
            obj = cls._decoders[data_type](
                obj=obj,
                type_lookup=data_["type_lookup"],
                path=path,
                **kwargs,
            )

        return cls(
            id_=id_,
            data=obj,
            file=None,
            is_set=True,
            source=source,
            is_pending=False,
        )

    def set_data(self, value: Any) -> Self:
        """Return a copy, with data set."""
        if self.is_set:
            raise RuntimeError(f"Parameter ID {self.id_!r} is already set!")
        return self.__class__(
            id_=self.id_,
            is_set=True,
            is_pending=self.is_pending,
            data=value,
            file=None,
            source=self.source,
        )

    def set_file(self, value: Any) -> Self:
        """Return a copy, with file set."""
        if self.is_set:
            raise RuntimeError(f"Parameter ID {self.id_!r} is already set!")
        return self.__class__(
            id_=self.id_,
            is_set=True,
            is_pending=self.is_pending,
            data=None,
            file=value,
            source=self.source,
        )

    def update_source(self, src: ParamSource) -> Self:
        """Return a copy, with updated source."""
        return self.__class__(
            id_=self.id_,
            is_set=self.is_set,
            is_pending=self.is_pending,
            data=self.data,
            file=self.file,
            source=update_param_source_dict(self.source, src),
        )


class PersistentStore(
    ABC, Generic[AnySTask, AnySElement, AnySElementIter, AnySEAR, AnySParameter]
):
    _name: ClassVar[str]

    @classmethod
    @abstractmethod
    def _store_task_cls(cls) -> type[AnySTask]:
        ...

    @classmethod
    @abstractmethod
    def _store_elem_cls(cls) -> type[AnySElement]:
        ...

    @classmethod
    @abstractmethod
    def _store_iter_cls(cls) -> type[AnySElementIter]:
        ...

    @classmethod
    @abstractmethod
    def _store_EAR_cls(cls) -> type[AnySEAR]:
        ...

    @classmethod
    @abstractmethod
    def _store_param_cls(cls) -> type[AnySParameter]:
        ...

    _resources: dict[str, StoreResource]
    _features: ClassVar[PersistentStoreFeatures]
    _res_map: ClassVar[CommitResourceMap]

    def __init__(
        self,
        app: BaseApp,
        workflow: Workflow | None,
        path: Path | str,
        fs: AbstractFileSystem | None = None,
    ):
        self.app = app
        self.__workflow = workflow
        self.path = str(path)
        self.fs = fs

        self._pending: PendingChanges[
            AnySTask, AnySElement, AnySElementIter, AnySEAR, AnySParameter
        ] = PendingChanges(app=app, store=self, resource_map=self._res_map)

        self._resources_in_use: set[tuple[str, str]] = set()
        self._in_batch_mode = False

        self._use_cache = False
        self._reset_cache()

    @abstractmethod
    def cached_load(self) -> contextlib.AbstractContextManager[None]:
        ...

    @abstractmethod
    def get_name(self) -> str:
        ...

    @abstractmethod
    def get_creation_info(self) -> StoreCreationInfo:
        ...

    @abstractmethod
    def get_ts_fmt(self) -> str:
        ...

    @abstractmethod
    def get_ts_name_fmt(self) -> str:
        ...

    @abstractmethod
    def remove_replaced_dir(self) -> None:
        ...

    @abstractmethod
    def reinstate_replaced_dir(self) -> None:
        ...

    @abstractmethod
    def zip(
        self,
        path: str = ".",
        log: str | None = None,
        overwrite=False,
        include_execute=False,
        include_rechunk_backups=False,
    ) -> str:
        ...

    @abstractmethod
    def unzip(self, path: str = ".", log: str | None = None) -> str:
        ...

    @abstractmethod
    def rechunk_parameter_base(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Any:
        ...

    @abstractmethod
    def rechunk_runs(
        self,
        chunk_size: int | None = None,
        backup: bool = True,
        status: bool = True,
    ) -> Any:
        ...

    @classmethod
    @abstractmethod
    def write_empty_workflow(
        cls,
        app: BaseApp,
        *,
        template_js: TemplateMeta,
        template_components_js: Dict,
        wk_path: str,
        fs: AbstractFileSystem,
        name: str,
        replaced_wk: str | None,
        creation_info: StoreCreationInfo,
        ts_fmt: str,
        ts_name_fmt: str,
    ) -> None:
        ...

    @property
    def workflow(self) -> Workflow:
        assert self.__workflow is not None
        return self.__workflow

    @property
    def logger(self) -> Logger:
        return self.app.persistence_logger

    @property
    def ts_fmt(self) -> str:
        return self.workflow.ts_fmt

    @property
    def has_pending(self) -> bool:
        return bool(self._pending)

    @property
    def is_submittable(self) -> bool:
        """Does this store support workflow submission?"""
        return self.fs.__class__.__name__ == "LocalFileSystem"

    @property
    def use_cache(self) -> bool:
        return self._use_cache

    @property
    def task_cache(self) -> dict[int, AnySTask]:
        """Cache for persistent tasks."""
        return self._cache["tasks"]

    @property
    def element_cache(self) -> dict[int, AnySElement]:
        """Cache for persistent elements."""
        return self._cache["elements"]

    @property
    def element_iter_cache(self) -> dict[int, AnySElementIter]:
        """Cache for persistent element iterations."""
        return self._cache["element_iters"]

    @property
    def EAR_cache(self) -> dict[int, AnySEAR]:
        """Cache for persistent EARs."""
        return self._cache["EARs"]

    @property
    def num_tasks_cache(self) -> int | None:
        """Cache for number of persistent tasks."""
        return self._cache["num_tasks"]

    @num_tasks_cache.setter
    def num_tasks_cache(self, value: int | None):
        self._cache["num_tasks"] = value

    @property
    def num_EARs_cache(self) -> int | None:
        """Cache for total number of persistent EARs."""
        return self._cache["num_EARs"]

    @num_EARs_cache.setter
    def num_EARs_cache(self, value: int | None):
        self._cache["num_EARs"] = value

    @property
    def param_sources_cache(self) -> dict[int, ParamSource]:
        """Cache for persistent parameter sources."""
        return self._cache["param_sources"]

    @property
    def parameter_cache(self) -> dict[int, AnySParameter]:
        """Cache for persistent parameters."""
        return self._cache["parameters"]

    def _reset_cache(self):
        self._cache = {
            "tasks": {},
            "elements": {},
            "element_iters": {},
            "EARs": {},
            "param_sources": {},
            "num_tasks": None,
            "parameters": {},
            "num_EARs": None,
        }

    @contextlib.contextmanager
    def cache_ctx(self):
        """Context manager for using the persistent element/iteration/run cache."""
        self._use_cache = True
        try:
            yield
        finally:
            self._use_cache = False
            self._reset_cache()

    @staticmethod
    def prepare_test_store_from_spec(
        task_spec: Sequence[
            Mapping[str, Sequence[Mapping[str, Sequence[Mapping[str, Sequence]]]]]
        ]
    ) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
        """Generate a valid store from a specification in terms of nested
        elements/iterations/EARs.

        """
        tasks: list[dict] = []
        elements: list[dict] = []
        elem_iters: list[dict] = []
        EARs: list[dict] = []

        for task_idx, task_i in enumerate(task_spec):
            elems_i = task_i.get("elements", [])
            elem_IDs = list(range(len(elements), len(elements) + len(elems_i)))

            for elem_idx, elem_j in enumerate(elems_i):
                iters_j = elem_j.get("iterations", [])
                iter_IDs = list(range(len(elem_iters), len(elem_iters) + len(iters_j)))

                for iter_k in iters_j:
                    EARs_k = iter_k.get("EARs", [])
                    EAR_IDs = list(range(len(EARs), len(EARs) + len(EARs_k)))
                    EAR_IDs_dct = {0: EAR_IDs} if EAR_IDs else {}

                    for _ in EARs_k:
                        EARs.append(
                            {
                                "id_": len(EARs),
                                "is_pending": False,
                                "elem_iter_ID": len(elem_iters),
                                "action_idx": 0,
                                "data_idx": {},
                                "metadata": {},
                            }
                        )

                    elem_iters.append(
                        {
                            "id_": len(elem_iters),
                            "is_pending": False,
                            "element_ID": len(elements),
                            "EAR_IDs": EAR_IDs_dct,
                            "data_idx": {},
                            "schema_parameters": [],
                        }
                    )
                elements.append(
                    {
                        "id_": len(elements),
                        "is_pending": False,
                        "element_idx": elem_idx,
                        "seq_idx": {},
                        "src_idx": {},
                        "task_ID": task_idx,
                        "iteration_IDs": iter_IDs,
                    }
                )
            tasks.append(
                {
                    "id_": len(tasks),
                    "is_pending": False,
                    "element_IDs": elem_IDs,
                }
            )
        return (tasks, elements, elem_iters, EARs)

    def remove_path(self, path: str | Path) -> None:
        """Try very hard to delete a directory or file.

        Dropbox (on Windows, at least) seems to try to re-sync files if the parent directory
        is deleted soon after creation, which is the case on a failed workflow creation (e.g.
        missing inputs), so in addition to catching PermissionErrors generated when
        Dropbox has a lock on files, we repeatedly try deleting the directory tree.

        """

        fs = self.fs
        assert fs is not None

        @self.app.perm_error_retry()
        def _remove_path(_path: str) -> None:
            self.logger.debug(f"_remove_path: path={_path}")
            while fs.exists(_path):
                fs.rm(_path, recursive=True)
                time.sleep(0.5)

        return _remove_path(str(path))

    def rename_path(self, replaced: str, original: str | Path) -> None:
        """Revert the replaced workflow path to its original name.

        This happens when new workflow creation fails and there is an existing workflow
        with the same name; the original workflow which was renamed, must be reverted."""

        fs = self.fs
        assert fs is not None

        @self.app.perm_error_retry()
        def _rename_path(_replaced: str, _original: str) -> None:
            self.logger.debug(f"_rename_path: {_replaced!r} --> {_original!r}.")
            try:
                fs.rename(
                    _replaced, _original, recursive=True
                )  # TODO: why need recursive?
            except TypeError:
                # `SFTPFileSystem.rename` has no  `recursive` argument:
                fs.rename(_replaced, _original)

        return _rename_path(str(replaced), str(original))

    @abstractmethod
    def _get_num_persistent_tasks(self) -> int:
        ...

    def _get_num_total_tasks(self) -> int:
        """Get the total number of persistent and pending tasks."""
        return self._get_num_persistent_tasks() + len(self._pending.add_tasks)

    @abstractmethod
    def _get_num_persistent_loops(self) -> int:
        ...

    def _get_num_total_loops(self) -> int:
        """Get the total number of persistent and pending loops."""
        return self._get_num_persistent_loops() + len(self._pending.add_loops)

    @abstractmethod
    def _get_num_persistent_submissions(self) -> int:
        ...

    def _get_num_total_submissions(self) -> int:
        """Get the total number of persistent and pending submissions."""
        return self._get_num_persistent_submissions() + len(self._pending.add_submissions)

    @abstractmethod
    def _get_num_persistent_elements(self) -> int:
        ...

    def _get_num_total_elements(self) -> int:
        """Get the total number of persistent and pending elements."""
        return self._get_num_persistent_elements() + len(self._pending.add_elements)

    @abstractmethod
    def _get_num_persistent_elem_iters(self) -> int:
        ...

    def _get_num_total_elem_iters(self) -> int:
        """Get the total number of persistent and pending element iterations."""
        return self._get_num_persistent_elem_iters() + len(self._pending.add_elem_iters)

    @abstractmethod
    def _get_num_persistent_EARs(self) -> int:
        ...

    @TimeIt.decorator
    def _get_num_total_EARs(self) -> int:
        """Get the total number of persistent and pending EARs."""
        return self._get_num_persistent_EARs() + len(self._pending.add_EARs)

    def _get_task_total_num_elements(self, task_ID: int) -> int:
        """Get the total number of persistent and pending elements of a given task."""
        return len(self.get_task(task_ID).element_IDs)

    @abstractmethod
    def _get_num_persistent_parameters(self) -> int:
        ...

    def _get_num_total_parameters(self) -> int:
        """Get the total number of persistent and pending parameters."""
        return self._get_num_persistent_parameters() + len(self._pending.add_parameters)

    def _get_num_total_input_files(self) -> int:
        """Get the total number of persistent and pending user-supplied input files."""
        num_pend_inp_files = len([i for i in self._pending.add_files if i["is_input"]])
        return self._get_num_persistent_input_files() + num_pend_inp_files

    @abstractmethod
    def _get_num_persistent_added_tasks(self) -> int:
        ...

    def _get_num_total_added_tasks(self) -> int:
        """Get the total number of tasks ever added to the workflow."""
        return self._get_num_persistent_added_tasks() + len(self._pending.add_tasks)

    def _get_num_persistent_input_files(self) -> int:
        return len(list(self.workflow.input_files_path.glob("*")))

    def save(self) -> None:
        """Commit pending changes to disk, if not in batch-update mode."""
        if not self.workflow._in_batch_mode:
            self._pending.commit_all()

    def add_template_components(
        self, temp_comps: Mapping[str, Any], save: bool = True
    ) -> None:
        all_tc = self.get_template_components()
        for name, dat in temp_comps.items():
            if name in all_tc:
                for hash_i, dat_i in dat.items():
                    if hash_i not in all_tc[name]:
                        self._pending.add_template_components[name][hash_i] = dat_i
            else:
                self._pending.add_template_components[name] = dat

        if save:
            self.save()

    def add_task(self, idx: int, task_template: Mapping, save: bool = True):
        """Add a new task to the workflow."""
        self.logger.debug(f"Adding store task.")
        new_ID = self._get_num_total_added_tasks()
        self._pending.add_tasks[new_ID] = self._store_task_cls()(
            id_=new_ID,
            index=idx,
            task_template=task_template,
            is_pending=True,
            element_IDs=[],
        )
        if save:
            self.save()
        return new_ID

    def add_loop(
        self,
        loop_template: Mapping[str, Any],
        iterable_parameters,
        parents: list[str],
        num_added_iterations: dict[tuple[int, ...], int],
        iter_IDs: list[int],
        save: bool = True,
    ):
        """Add a new loop to the workflow."""
        self.logger.debug(f"Adding store loop.")
        new_idx = self._get_num_total_loops()
        added_iters = [[list(k), v] for k, v in num_added_iterations.items()]
        self._pending.add_loops[new_idx] = {
            "loop_template": dict(loop_template),
            "iterable_parameters": iterable_parameters,
            "parents": parents,
            "num_added_iterations": added_iters,
        }

        for i in iter_IDs:
            self._pending.update_loop_indices[i][loop_template["name"]] = 0

        if save:
            self.save()

    @TimeIt.decorator
    def add_submission(self, sub_idx: int, sub_js: Dict, save: bool = True):
        """Add a new submission."""
        self.logger.debug(f"Adding store submission.")
        self._pending.add_submissions[sub_idx] = sub_js
        if save:
            self.save()

    def add_element_set(self, task_id: int, es_js: Mapping, save: bool = True):
        self._pending.add_element_sets[task_id].append(es_js)
        if save:
            self.save()

    def add_element(
        self,
        task_ID: int,
        es_idx: int,
        seq_idx: dict[str, int],
        src_idx: dict[str, int],
        save: bool = True,
    ) -> int:
        """Add a new element to a task."""
        self.logger.debug(f"Adding store element.")
        new_ID = self._get_num_total_elements()
        new_elem_idx = self._get_task_total_num_elements(task_ID)
        self._pending.add_elements[new_ID] = self._store_elem_cls()(
            id_=new_ID,
            is_pending=True,
            index=new_elem_idx,
            es_idx=es_idx,
            seq_idx=seq_idx,
            src_idx=src_idx,
            task_ID=task_ID,
            iteration_IDs=[],
        )
        self._pending.add_elem_IDs[task_ID].append(new_ID)
        if save:
            self.save()
        return new_ID

    def add_element_iteration(
        self,
        element_ID: int,
        data_idx: DataIndex,
        schema_parameters: list[str],
        loop_idx: dict[str, int] | None = None,
        save: bool = True,
    ) -> int:
        """Add a new iteration to an element."""
        self.logger.debug(f"Adding store element-iteration.")
        new_ID = self._get_num_total_elem_iters()
        self._pending.add_elem_iters[new_ID] = self._store_iter_cls()(
            id_=new_ID,
            element_ID=element_ID,
            is_pending=True,
            EARs_initialised=False,
            EAR_IDs=None,
            data_idx=data_idx,
            schema_parameters=schema_parameters,
            loop_idx=loop_idx or {},
        )
        self._pending.add_elem_iter_IDs[element_ID].append(new_ID)
        if save:
            self.save()
        return new_ID

    @TimeIt.decorator
    def add_EAR(
        self,
        elem_iter_ID: int,
        action_idx: int,
        commands_idx: list[int],
        data_idx: DataIndex,
        metadata: Metadata | None = None,
        save: bool = True,
    ) -> int:
        """Add a new EAR to an element iteration."""
        self.logger.debug(f"Adding store EAR.")
        new_ID = self._get_num_total_EARs()
        self._pending.add_EARs[new_ID] = self._store_EAR_cls()(
            id_=new_ID,
            is_pending=True,
            elem_iter_ID=elem_iter_ID,
            action_idx=action_idx,
            commands_idx=commands_idx,
            data_idx=data_idx,
            metadata=metadata or {},
        )
        self._pending.add_elem_iter_EAR_IDs[elem_iter_ID][action_idx].append(new_ID)
        if save:
            self.save()
        return new_ID

    def add_submission_part(
        self, sub_idx: int, dt_str: str, submitted_js_idx: list[int], save: bool = True
    ):
        self._pending.add_submission_parts[sub_idx][dt_str] = submitted_js_idx
        if save:
            self.save()

    @TimeIt.decorator
    def set_EAR_submission_index(
        self, EAR_ID: int, sub_idx: int, save: bool = True
    ) -> None:
        self._pending.set_EAR_submission_indices[EAR_ID] = sub_idx
        if save:
            self.save()

    def set_EAR_start(self, EAR_ID: int, save: bool = True) -> datetime:
        dt = datetime.utcnow()
        ss_js = self.app.RunDirAppFiles.take_snapshot()
        run_hostname = socket.gethostname()
        self._pending.set_EAR_starts[EAR_ID] = (dt, ss_js, run_hostname)
        if save:
            self.save()
        return dt

    def set_EAR_end(
        self, EAR_ID: int, exit_code: int, success: bool, save: bool = True
    ) -> datetime:
        # TODO: save output files
        dt = datetime.utcnow()
        ss_js = self.app.RunDirAppFiles.take_snapshot()
        self._pending.set_EAR_ends[EAR_ID] = (dt, ss_js, exit_code, success)
        if save:
            self.save()
        return dt

    def set_EAR_skip(self, EAR_ID: int, save: bool = True) -> None:
        self._pending.set_EAR_skips.append(EAR_ID)
        if save:
            self.save()

    def set_EARs_initialised(self, iter_ID: int, save: bool = True) -> None:
        self._pending.set_EARs_initialised.append(iter_ID)
        if save:
            self.save()

    def set_jobscript_metadata(
        self,
        sub_idx: int,
        js_idx: int,
        version_info: dict[str, str | list[str]] | None = None,
        submit_time: str | None = None,
        submit_hostname: str | None = None,
        submit_machine: str | None = None,
        submit_cmdline: list[str] | None = None,
        os_name: str | None = None,
        shell_name: str | None = None,
        scheduler_name: str | None = None,
        scheduler_job_ID: str | None = None,
        process_ID: int | None = None,
        save: bool = True,
    ):
        entry = self._pending.set_js_metadata[sub_idx][js_idx]
        if version_info:
            entry["version_info"] = version_info
        if submit_time:
            entry["submit_time"] = submit_time
        if submit_hostname:
            entry["submit_hostname"] = submit_hostname
        if submit_machine:
            entry["submit_machine"] = submit_machine
        if submit_cmdline:
            entry["submit_cmdline"] = submit_cmdline
        if os_name:
            entry["os_name"] = os_name
        if shell_name:
            entry["shell_name"] = shell_name
        if scheduler_name:
            entry["scheduler_name"] = scheduler_name
        if scheduler_job_ID:
            entry["scheduler_job_ID"] = scheduler_job_ID
        if process_ID:
            entry["process_ID"] = process_ID
        if save:
            self.save()

    def _add_parameter(
        self,
        is_set: bool,
        source: ParamSource,
        data: (
            ParameterValue | list | tuple | set | dict | int | float | str | None | Any
        ) = None,
        file: Dict | None = None,
        save: bool = True,
    ) -> int:
        self.logger.debug(f"Adding store parameter{f' (unset)' if not is_set else ''}.")
        new_idx = self._get_num_total_parameters()
        self._pending.add_parameters[new_idx] = self._store_param_cls()(
            id_=new_idx,
            is_pending=True,
            is_set=is_set,
            data=PARAM_DATA_NOT_SET if not is_set else data,
            file=file,
            source=source,
        )
        if save:
            self.save()
        return new_idx

    def _prepare_set_file(
        self,
        store_contents: bool,
        is_input: bool,
        path: Path | str,
        contents: str | None = None,
        filename: str | None = None,
        clean_up: bool = False,
    ):
        if filename is None:
            filename = Path(path).name

        if store_contents:
            if is_input:
                new_idx = self._get_num_total_input_files()
                dst_dir = Path(self.workflow.input_files_path, str(new_idx))
                dst_path = dst_dir / filename
            else:
                # assume path is inside the EAR execution directory; transform that to the
                # equivalent artifacts directory:
                exec_sub_path = Path(path).relative_to(self.path)
                dst_path = Path(
                    self.workflow.task_artifacts_path, *exec_sub_path.parts[1:]
                )
            if dst_path.is_file():
                dst_path = dst_path.with_suffix(dst_path.suffix + "_2")  # TODO: better!
        else:
            dst_path = Path(path)

        file_param_dat = {
            "store_contents": store_contents,
            "path": str(dst_path.relative_to(self.path)),
        }
        self._pending.add_files.append(
            {
                "store_contents": store_contents,
                "is_input": is_input,
                "dst_path": str(dst_path),
                "path": str(path),
                "contents": contents,
                "clean_up": clean_up,
            }
        )

        return file_param_dat

    def set_file(
        self,
        store_contents: bool,
        is_input: bool,
        param_id: int | None,
        path: Path | str,
        contents: str | None = None,
        filename: str | None = None,
        clean_up: bool = False,
        save: bool = True,
    ):
        self.logger.debug(f"Setting new file")
        file_param_dat = self._prepare_set_file(
            store_contents=store_contents,
            is_input=is_input,
            path=path,
            contents=contents,
            filename=filename,
            clean_up=clean_up,
        )
        if param_id is not None:
            self.set_parameter_value(
                param_id, value=file_param_dat, is_file=True, save=save
            )
        if save:
            self.save()

    def add_file(
        self,
        store_contents: bool,
        is_input: bool,
        source: ParamSource,
        path: Path | str,
        contents: str | None = None,
        filename: str | None = None,
        save: bool = True,
    ):
        self.logger.debug(f"Adding new file")
        file_param_dat = self._prepare_set_file(
            store_contents=store_contents,
            is_input=is_input,
            path=path,
            contents=contents,
            filename=filename,
        )
        p_id = self._add_parameter(
            file=file_param_dat,
            is_set=True,
            source=source,
            save=save,
        )
        if save:
            self.save()
        return p_id

    def _append_files(self, files: list[dict[str, Any]]):
        """Add new files to the files or artifacts directories."""
        for dat in files:
            if dat["store_contents"]:
                dst_path = Path(dat["dst_path"])
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                if dat["path"] is not None:
                    # copy from source path to destination:
                    shutil.copy(dat["path"], dst_path)
                    if dat["clean_up"]:
                        self.logger.info(f"deleting file {dat['path']}")
                        os.remove(dat["path"])
                else:
                    # write out text file:
                    with dst_path.open("wt") as fp:
                        fp.write(dat["contents"])

    def add_set_parameter(
        self,
        data: ParameterValue | list | tuple | set | dict | int | float | str | Any,
        source: ParamSource,
        save: bool = True,
    ) -> int:
        return self._add_parameter(data=data, is_set=True, source=source, save=save)

    def add_unset_parameter(self, source: ParamSource, save: bool = True) -> int:
        return self._add_parameter(data=None, is_set=False, source=source, save=save)

    @abstractmethod
    def _set_parameter_values(self, set_parameters: dict[int, tuple[Any, bool]]):
        ...

    def set_parameter_value(
        self, param_id: int, value: Any, is_file: bool = False, save: bool = True
    ):
        self.logger.debug(
            f"Setting store parameter ID {param_id} value with type: {type(value)!r})."
        )
        self._pending.set_parameters[param_id] = (value, is_file)
        if save:
            self.save()

    @TimeIt.decorator
    def update_param_source(
        self, param_sources: Mapping[int, ParamSource], save: bool = True
    ) -> None:
        self.logger.debug(f"Updating parameter sources with {param_sources!r}.")
        self._pending.update_param_sources.update(param_sources)
        if save:
            self.save()

    def update_loop_num_iters(
        self,
        index: int,
        num_added_iters: Mapping[tuple[int, ...], int],
        save: bool = True,
    ) -> None:
        self.logger.debug(
            f"Updating loop {index!r} num added iterations to {num_added_iters!r}."
        )
        self._pending.update_loop_num_iters[index] = [
            [list(k), v] for k, v in num_added_iters.items()
        ]
        if save:
            self.save()

    def update_loop_parents(
        self,
        index: int,
        num_added_iters: Mapping[tuple[int, ...], int],
        parents: list[str],
        save: bool = True,
    ) -> None:
        self.logger.debug(
            f"Updating loop {index!r} parents to {parents!r}, and num added iterations "
            f"to {num_added_iters}."
        )
        self._pending.update_loop_num_iters[index] = [
            [list(k), v] for k, v in num_added_iters.items()
        ]
        self._pending.update_loop_parents[index] = parents
        if save:
            self.save()

    def get_template_components(self) -> dict[str, Any]:
        """Get all template components, including pending."""
        tc = copy.deepcopy(self._get_persistent_template_components())
        for typ in TEMPLATE_COMP_TYPES:
            for hash_i, dat_i in self._pending.add_template_components[typ].items():
                tc.setdefault(typ, {})[hash_i] = dat_i

        return tc

    @abstractmethod
    def _get_persistent_template_components(self) -> dict[str, Any]:
        ...

    def get_template(self) -> dict[str, JSONed]:
        return self._get_persistent_template()

    @abstractmethod
    def _get_persistent_template(self) -> dict[str, JSONed]:
        ...

    def _get_task_id_to_idx_map(self) -> dict[int, int]:
        return {i.id_: i.index for i in self.get_tasks()}

    @TimeIt.decorator
    def get_task(self, task_idx: int) -> AnySTask:
        return self.get_tasks()[task_idx]

    def __process_retrieved_tasks(self, tasks: Iterable[AnySTask]) -> list[AnySTask]:
        """Add pending data to retrieved tasks."""
        tasks_new: list[AnySTask] = []
        for task_i in tasks:
            # consider pending element IDs:
            pend_elems = self._pending.add_elem_IDs.get(task_i.id_)
            if pend_elems:
                task_i = task_i.append_element_IDs(pend_elems)
            tasks_new.append(task_i)
        return tasks_new

    def __process_retrieved_loops(
        self, loops: dict[int, dict[str, Any]]
    ) -> dict[int, dict[str, Any]]:
        """Add pending data to retrieved loops."""
        loops_new: dict[int, dict[str, Any]] = {}
        for id_, loop_i in loops.items():
            if "num_added_iterations" not in loop_i:
                loop_i["num_added_iterations"] = 1
            # consider pending changes to num added iterations:
            pend_num_iters = self._pending.update_loop_num_iters.get(id_)
            if pend_num_iters:
                loop_i["num_added_iterations"] = pend_num_iters
            # consider pending change to parents:
            pend_parents = self._pending.update_loop_parents.get(id_)
            if pend_parents:
                loop_i["parents"] = pend_parents

            loops_new[id_] = loop_i
        return loops_new

    @abstractmethod
    def _get_persistent_tasks(self, id_lst: Iterable[int]) -> dict[int, AnySTask]:
        ...

    def get_tasks_by_IDs(self, ids: Iterable[int]) -> Sequence[AnySTask]:
        # separate pending and persistent IDs:
        id_lst = list(ids)
        id_set = set(id_lst)
        all_pending = set(self._pending.add_tasks)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        tasks = self._get_persistent_tasks(id_pers) if id_pers else {}
        tasks.update((i, self._pending.add_tasks[i]) for i in id_pend)

        # order as requested:
        return self.__process_retrieved_tasks(tasks[id_] for id_ in id_lst)

    @TimeIt.decorator
    def get_tasks(self) -> list[AnySTask]:
        """Retrieve all tasks, including pending."""
        tasks = self._get_persistent_tasks(range(self._get_num_persistent_tasks()))
        tasks.update(self._pending.add_tasks)

        # order by index:
        task_list = sorted(tasks.values(), key=lambda x: x.index)

        return self.__process_retrieved_tasks(task_list)

    @abstractmethod
    def _get_persistent_loops(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, dict[str, Any]]:
        ...

    def get_loops_by_IDs(self, ids: Iterable[int]) -> dict[int, dict[str, Any]]:
        """Retrieve loops by index (ID), including pending."""

        # separate pending and persistent IDs:
        id_lst = list(ids)
        id_set = set(id_lst)
        all_pending = set(self._pending.add_loops)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        loops = self._get_persistent_loops(id_pers) if id_pers else {}
        loops.update((i, self._pending.add_loops[i]) for i in id_pend)

        # order as requested:
        return self.__process_retrieved_loops({id_: loops[id_] for id_ in id_lst})

    def get_loops(self) -> dict[int, dict[str, Any]]:
        """Retrieve all loops, including pending."""

        loops = self._get_persistent_loops()
        loops.update(self._pending.add_loops)

        # order by index/ID:
        loops = dict(sorted(loops.items()))

        return self.__process_retrieved_loops(loops)

    @abstractmethod
    def _get_persistent_submissions(
        self, id_lst: Iterable[int] | None = None
    ) -> dict[int, Any]:
        ...

    @TimeIt.decorator
    def get_submissions(self) -> dict[int, Dict]:
        """Retrieve all submissions, including pending."""

        subs = self._get_persistent_submissions()
        subs.update(self._pending.add_submissions)

        # order by index/ID
        return dict(sorted(subs.items()))

    @TimeIt.decorator
    def get_submissions_by_ID(self, ids: Iterable[int]) -> dict[int, Dict]:
        id_lst = list(ids)
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_submissions)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        subs = self._get_persistent_submissions(id_pers) if id_pers else {}
        subs.update((i, self._pending.add_submissions[i]) for i in id_pend)

        # order by index/ID
        return dict(sorted(subs.items()))

    @abstractmethod
    def _get_persistent_elements(self, id_lst: Iterable[int]) -> dict[int, AnySElement]:
        ...

    @TimeIt.decorator
    def get_elements(self, ids: Iterable[int]) -> Sequence[AnySElement]:
        id_lst = list(ids)
        self.logger.debug(f"PersistentStore.get_elements: id_lst={id_lst!r}")

        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_elements)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        elems = self._get_persistent_elements(id_pers) if id_pers else {}
        elems.update((i, self._pending.add_elements[i]) for i in id_pend)

        elems_new: list[AnySElement] = []
        # order as requested:
        for elem_i in (elems[id_] for id_ in id_lst):
            # consider pending iteration IDs:
            # TODO: does this consider pending iterations from new loop iterations?
            pend_iters = self._pending.add_elem_iter_IDs.get(elem_i.id_)
            if pend_iters:
                elem_i = elem_i.append_iteration_IDs(pend_iters)
            elems_new.append(elem_i)

        return elems_new

    @abstractmethod
    def _get_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> dict[int, AnySElementIter]:
        ...

    @TimeIt.decorator
    def get_element_iterations(self, ids: Iterable[int]) -> Sequence[AnySElementIter]:
        id_lst = list(ids)
        self.logger.debug(f"PersistentStore.get_element_iterations: id_lst={id_lst!r}")

        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_elem_iters)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        iters = self._get_persistent_element_iters(id_pers) if id_pers else {}
        iters.update((i, self._pending.add_elem_iters[i]) for i in id_pend)

        iters_new: list[AnySElementIter] = []
        # order as requested:
        for iter_i in (iters[id_] for id_ in id_lst):
            # consider pending EAR IDs:
            pend_EARs = self._pending.add_elem_iter_EAR_IDs.get(iter_i.id_)
            if pend_EARs:
                iter_i = iter_i.append_EAR_IDs(pend_EARs)

            # consider pending loop idx
            pend_loop_idx = self._pending.update_loop_indices.get(iter_i.id_)
            if pend_loop_idx:
                iter_i = iter_i.update_loop_idx(pend_loop_idx)

            # consider pending `EARs_initialised`:
            if iter_i.id_ in self._pending.set_EARs_initialised:
                iter_i = iter_i.set_EARs_initialised()

            iters_new.append(iter_i)

        return iters_new

    @abstractmethod
    def _get_persistent_EARs(self, id_lst: Iterable[int]) -> dict[int, AnySEAR]:
        ...

    @TimeIt.decorator
    def get_EARs(self, ids: Iterable[int]) -> Sequence[AnySEAR]:
        id_lst = list(ids)
        self.logger.debug(f"PersistentStore.get_EARs: id_lst={id_lst!r}")

        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_EARs)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        EARs = self._get_persistent_EARs(id_pers) if id_pers else {}
        EARs.update((i, self._pending.add_EARs[i]) for i in id_pend)

        EARs_new: list[AnySEAR] = []
        # order as requested:
        for EAR_i in (EARs[id_] for id_ in id_lst):
            # consider updates:
            updates: dict[str, Any] = {
                "submission_idx": self._pending.set_EAR_submission_indices.get(EAR_i.id_)
            }
            if EAR_i.id_ in self._pending.set_EAR_skips:
                updates["skip"] = True
            (
                updates["start_time"],
                updates["snapshot_start"],
                updates["run_hostname"],
            ) = self._pending.set_EAR_starts.get(EAR_i.id_, (None, None, None))
            (
                updates["end_time"],
                updates["snapshot_end"],
                updates["exit_code"],
                updates["success"],
            ) = self._pending.set_EAR_ends.get(EAR_i.id_, (None, None, None, None))
            if any(i is not None for i in updates.values()):
                EAR_i = EAR_i.update(**updates)

            EARs_new.append(EAR_i)

        return EARs_new

    @TimeIt.decorator
    def _get_cached_persistent_items(
        self, id_lst: Iterable[int], cache: dict[int, T]
    ) -> tuple[dict[int, T], list[int]]:
        if self.use_cache:
            id_set = set(id_lst)
            all_cached = set(cache.keys())
            id_cached = id_set.intersection(all_cached)
            id_non_cached = list(id_set.difference(all_cached))
            items = {k: cache[k] for k in id_cached}
        else:
            items = {}
            id_non_cached = list(id_lst)
        return items, id_non_cached

    def _get_cached_persistent_EARs(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySEAR], list[int]]:
        return self._get_cached_persistent_items(id_lst, self.EAR_cache)

    def _get_cached_persistent_element_iters(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySElementIter], list[int]]:
        return self._get_cached_persistent_items(id_lst, self.element_iter_cache)

    def _get_cached_persistent_elements(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySElement], list[int]]:
        return self._get_cached_persistent_items(id_lst, self.element_cache)

    def _get_cached_persistent_tasks(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySTask], list[int]]:
        return self._get_cached_persistent_items(id_lst, self.task_cache)

    def _get_cached_persistent_param_sources(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, ParamSource], list[int]]:
        return self._get_cached_persistent_items(id_lst, self.param_sources_cache)

    def _get_cached_persistent_parameters(
        self, id_lst: Iterable[int]
    ) -> tuple[dict[int, AnySParameter], list[int]]:
        return self._get_cached_persistent_items(id_lst, self.parameter_cache)

    def get_EAR_skipped(self, EAR_ID: int) -> bool:
        self.logger.debug(f"PersistentStore.get_EAR_skipped: EAR_ID={EAR_ID!r}")
        return self.get_EARs([EAR_ID])[0].skip

    @TimeIt.decorator
    def get_parameters(self, id_lst: Iterable[int], **kwargs) -> list[AnySParameter]:
        """
        Parameters
        ----------
        kwargs :
            dataset_copy : bool
                For Zarr stores only. If True, copy arrays as NumPy arrays.
        """
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_parameters)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        params = (
            dict(self._get_persistent_parameters(id_pers, **kwargs)) if id_pers else {}
        )
        params.update((i, self._pending.add_parameters[i]) for i in id_pend)

        # order as requested:
        return [params[id_] for id_ in id_lst]

    @abstractmethod
    def _get_persistent_parameters(
        self, id_lst: Iterable[int], **kwargs
    ) -> Mapping[int, AnySParameter]:
        ...

    @TimeIt.decorator
    def get_parameter_set_statuses(self, id_lst: Iterable[int]) -> list[bool]:
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_parameters)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        set_status = self._get_persistent_parameter_set_status(id_pers) if id_pers else {}
        set_status.update((i, self._pending.add_parameters[i].is_set) for i in id_pend)

        # order as requested:
        return [set_status[id_] for id_ in id_lst]

    @abstractmethod
    def _get_persistent_parameter_set_status(
        self, id_lst: Iterable[int]
    ) -> dict[int, bool]:
        ...

    @TimeIt.decorator
    def get_parameter_sources(self, id_lst: Iterable[int]) -> list[ParamSource]:
        # separate pending and persistent IDs:
        id_set = set(id_lst)
        all_pending = set(self._pending.add_parameters)
        id_pers = id_set.difference(all_pending)
        id_pend = id_set.intersection(all_pending)

        src = self._get_persistent_param_sources(id_pers) if id_pers else {}
        src.update((i, self._pending.add_parameters[i].source) for i in id_pend)

        # order as requested, and consider pending source updates:
        return [
            self.__merge_param_source(
                src[id_i], self._pending.update_param_sources.get(id_i)
            )
            for id_i in id_lst
        ]

    @staticmethod
    def __merge_param_source(
        src_i: ParamSource, pend_src: ParamSource | None
    ) -> ParamSource:
        """
        Helper to merge a second dict in if it is provided.
        """
        return {**src_i, **pend_src} if pend_src else src_i

    @abstractmethod
    def _get_persistent_param_sources(
        self, id_lst: Iterable[int]
    ) -> dict[int, ParamSource]:
        ...

    @TimeIt.decorator
    def get_task_elements(
        self,
        task_id: int,
        idx_lst: Iterable[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Get element data by an indices within a given task.

        Element iterations and EARs belonging to the elements are included.

        """

        all_elem_IDs = self.get_task(task_id).element_IDs
        store_elements = self.get_elements(
            all_elem_IDs if idx_lst is None else (all_elem_IDs[i] for i in idx_lst)
        )
        iter_IDs_flat, iter_IDs_lens = flatten([i.iteration_IDs for i in store_elements])
        store_iters = self.get_element_iterations(iter_IDs_flat)

        # retrieve EARs:
        EARs_dcts = remap(
            [list((i.EAR_IDs or {}).values()) for i in store_iters],
            lambda ears: [ear.to_dict() for ear in self.get_EARs(ears)],
        )

        # add EARs to iterations:
        iters: list[dict[str, Any]] = []
        for idx, i in enumerate(store_iters):
            EARs: dict[int, dict[str, Any]] | None = None
            if i.EAR_IDs is not None:
                EARs = dict(zip(i.EAR_IDs.keys(), cast("Any", EARs_dcts[idx])))
            iters.append(i.to_dict(EARs))

        # reshape iterations:
        iters_rs = reshape(iters, iter_IDs_lens)

        # add iterations to elements:
        return [
            element.to_dict(iters_rs[idx]) for idx, element in enumerate(store_elements)
        ]

    @abstractmethod
    def _get_persistent_parameter_IDs(self) -> Iterable[int]:
        ...

    def check_parameters_exist(self, id_lst: Iterable[int]) -> list[bool]:
        """For each parameter ID, return True if it exists, else False"""

        id_set = set(id_lst)
        all_pending = set(self._pending.add_parameters)
        id_not_pend = id_set.difference(all_pending)
        id_miss = set()
        if id_not_pend:
            id_miss = id_not_pend.difference(self._get_persistent_parameter_IDs())

        return [i not in id_miss for i in id_lst]

    @abstractmethod
    def _append_tasks(self, tasks: Iterable[AnySTask]) -> None:
        ...

    @abstractmethod
    def _append_loops(self, loops: dict[int, dict[str, Any]]) -> None:
        ...

    @abstractmethod
    def _append_submissions(self, subs: dict[int, Dict]) -> None:
        ...

    @abstractmethod
    def _append_submission_parts(
        self, sub_parts: dict[int, dict[str, list[int]]]
    ) -> None:
        ...

    @abstractmethod
    def _append_elements(self, elems: Sequence[AnySElement]) -> None:
        ...

    @abstractmethod
    def _append_element_sets(self, task_id: int, es_js: Sequence[Mapping]) -> None:
        ...

    @abstractmethod
    def _append_elem_iter_IDs(self, elem_ID: int, iter_IDs: Iterable[int]) -> None:
        ...

    @abstractmethod
    def _append_elem_iters(self, iters: Sequence[AnySElementIter]) -> None:
        ...

    @abstractmethod
    def _append_elem_iter_EAR_IDs(
        self, iter_ID: int, act_idx: int, EAR_IDs: Sequence[int]
    ) -> None:
        ...

    @abstractmethod
    def _append_EARs(self, EARs: Sequence[AnySEAR]) -> None:
        ...

    @abstractmethod
    def _update_elem_iter_EARs_initialised(self, iter_ID: int) -> None:
        ...

    @abstractmethod
    def _update_EAR_submission_indices(self, sub_indices: Mapping[int, int]) -> None:
        ...

    @abstractmethod
    def _update_EAR_start(
        self, EAR_id: int, s_time: datetime, s_snap: dict[str, Any], s_hn: str
    ) -> None:
        ...

    @abstractmethod
    def _update_EAR_end(
        self,
        EAR_id: int,
        e_time: datetime,
        e_snap: dict[str, Any],
        ext_code: int,
        success: bool,
    ) -> None:
        ...

    @abstractmethod
    def _update_EAR_skip(self, EAR_id: int) -> None:
        ...

    @abstractmethod
    def _update_js_metadata(self, js_meta: Dict) -> None:
        ...

    @abstractmethod
    def _append_parameters(self, params: Sequence[AnySParameter]) -> None:
        ...

    @abstractmethod
    def _update_template_components(self, tc: Dict) -> None:
        ...

    @abstractmethod
    def _update_parameter_sources(self, sources: Mapping[int, ParamSource]) -> None:
        ...

    @abstractmethod
    def _update_loop_index(self, iter_ID: int, loop_idx: dict[str, int]) -> None:
        ...

    @abstractmethod
    def _update_loop_num_iters(
        self, index: int, num_iters: list[list[list[int] | int]]
    ) -> None:
        ...

    @abstractmethod
    def _update_loop_parents(self, index: int, parents: list[str]) -> None:
        ...

    @overload
    def using_resource(
        self, res_label: Literal["metadata"], action: str
    ) -> AbstractContextManager[Metadata]:
        ...

    @overload
    def using_resource(
        self, res_label: Literal["submissions"], action: str
    ) -> AbstractContextManager[list[Dict]]:
        ...

    @overload
    def using_resource(
        self, res_label: Literal["parameters"], action: str
    ) -> AbstractContextManager[dict[str, dict[str, Any]]]:
        ...

    @overload
    def using_resource(
        self, res_label: Literal["attrs"], action: str
    ) -> AbstractContextManager[Dict]:
        ...

    @contextlib.contextmanager
    def using_resource(self, res_label: str, action: str) -> Iterator[Any]:
        """Context manager for managing `StoreResource` objects associated with the store."""

        try:
            res = self._resources[res_label]
        except KeyError:
            raise RuntimeError(
                f"{self.__class__.__name__!r} has no resource named {res_label!r}."
            ) from None

        key = (res_label, action)
        if key in self._resources_in_use:
            # retrieve existing data for this action:
            yield res.data[action]

        else:
            try:
                # "open" the resource, which assigns data for this action, which we yield:
                res.open(action)
                self._resources_in_use.add(key)
                yield res.data[action]

            except Exception as exc:
                self._resources_in_use.remove(key)
                raise exc

            else:
                # "close" the resource, clearing cached data for this action:
                res.close(action)
                self._resources_in_use.remove(key)

    def copy(self, path: PathLike = None) -> Path:
        """Copy the workflow store.

        This does not work on remote filesystems.

        """
        assert self.fs is not None
        if path is None:
            _path = Path(self.path)
            path = _path.parent / Path(_path.stem + "_copy" + _path.suffix)

        if self.fs.exists(str(path)):
            raise ValueError(f"Path already exists: {path}.")
        else:
            path = str(path)

        self.fs.copy(self.path, path)

        return Path(self.workflow._store.path).replace(path)

    def delete(self) -> None:
        """Delete the persistent workflow."""
        confirm = input(
            f"Permanently delete the workflow at path {self.path!r}; [y]es or [n]o?"
        )
        if confirm.strip().lower() == "y":
            self.delete_no_confirm()

    def delete_no_confirm(self) -> None:
        """Permanently delete the workflow data with no confirmation."""

        fs = self.fs
        assert fs is not None

        @self.app.perm_error_retry()
        def _delete_no_confirm() -> None:
            self.logger.debug(f"_delete_no_confirm: {self.path!r}.")
            fs.rm(self.path, recursive=True)

        return _delete_no_confirm()

    @abstractmethod
    def _append_task_element_IDs(self, task_ID: int, elem_IDs: list[int]):
        raise NotImplementedError
