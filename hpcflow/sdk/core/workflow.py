from __future__ import annotations
from contextlib import contextmanager
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pathlib import Path
import random
import string
from threading import Thread
import time
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union
from uuid import uuid4
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.zip import ZipFileSystem
from platformdirs import user_data_dir
import numpy as np
from fsspec.core import url_to_fs
import rich.console

from hpcflow.sdk import app
from hpcflow.sdk.core import (
    ALL_TEMPLATE_FORMATS,
    DEFAULT_TEMPLATE_FORMAT,
    ABORT_EXIT_CODE,
)
from hpcflow.sdk.persistence import store_cls_from_str, DEFAULT_STORE_FORMAT
from hpcflow.sdk.persistence.base import TEMPLATE_COMP_TYPES, AnySEAR
from hpcflow.sdk.persistence.utils import ask_pw_on_auth_exc, infer_store
from hpcflow.sdk.submission.jobscript import (
    generate_EAR_resource_map,
    group_resource_map_into_jobscripts,
    jobscripts_to_list,
    merge_jobscripts_across_tasks,
    resolve_jobscript_dependencies,
)
from hpcflow.sdk.submission.schedulers.direct import DirectScheduler
from hpcflow.sdk.typing import PathLike
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from .utils import (
    read_JSON_file,
    read_JSON_string,
    read_YAML,
    read_YAML_file,
    replace_items,
)
from hpcflow.sdk.core.errors import (
    LoopAlreadyExistsError,
    SubmissionFailure,
    WorkflowSubmissionFailure,
)


class _DummyPersistentWorkflow:
    """An object to pass to ResourceSpec.make_persistent that pretends to be a
    Workflow object, so we can pretend to make template-level inputs/resources
    persistent before the workflow exists."""

    def __init__(self):
        self._parameters = []
        self._sources = []
        self._data_ref = []

    def _add_parameter_data(self, data, source: Dict) -> int:
        self._parameters.append(data)
        self._sources.append(source)
        self._data_ref.append(len(self._data_ref))
        return self._data_ref[-1]

    def get_parameter_data(self, data_idx):
        return self._parameters[self._data_ref.index(data_idx)]

    def make_persistent(self, workflow: app.Workflow):
        for dat_i, source_i in zip(self._parameters, self._sources):
            workflow._add_parameter_data(dat_i, source_i)


@dataclass
class WorkflowTemplate(JSONLike):
    """Class to represent initial parametrisation of a {app_name} workflow, with limited
    validation logic."""

    _app_attr = "app"

    _child_objects = (
        ChildObjectSpec(
            name="tasks",
            class_name="Task",
            is_multiple=True,
            parent_ref="workflow_template",
        ),
        ChildObjectSpec(
            name="loops",
            class_name="Loop",
            is_multiple=True,
            parent_ref="_workflow_template",
        ),
        ChildObjectSpec(
            name="resources",
            class_name="ResourceList",
            parent_ref="_workflow_template",
        ),
    )

    name: str
    tasks: Optional[List[app.Task]] = field(default_factory=lambda: [])
    loops: Optional[List[app.Loop]] = field(default_factory=lambda: [])
    workflow: Optional[app.Workflow] = None
    resources: Optional[Dict[str, Dict]] = None

    def __post_init__(self):
        if isinstance(self.resources, dict):
            self.resources = self.app.ResourceList.from_json_like(self.resources)
        elif isinstance(self.resources, list):
            self.resources = self.app.ResourceList(self.resources)
        elif not self.resources:
            self.resources = self.app.ResourceList([self.app.ResourceSpec()])

        self._set_parent_refs()

    @classmethod
    def _from_data(cls, data: Dict) -> app.WorkflowTemplate:
        # use element_sets if not already:
        for task_idx, task_dat in enumerate(data["tasks"]):
            if "element_sets" not in task_dat:
                # add a single element set:
                schemas = task_dat.pop("schemas")
                data["tasks"][task_idx] = {
                    "schemas": schemas,
                    "element_sets": [task_dat],
                }

        # extract out any template components:
        params_dat = data.pop("parameters", [])
        if params_dat:
            parameters = cls.app.ParametersList.from_json_like(
                params_dat, shared_data=cls.app.template_components
            )
            cls.app.parameters.add_objects(parameters, skip_duplicates=True)

        cmd_files_dat = data.pop("command_files", [])
        if cmd_files_dat:
            cmd_files = cls.app.CommandFilesList.from_json_like(
                cmd_files_dat, shared_data=cls.app.template_components
            )
            cls.app.command_files.add_objects(cmd_files, skip_duplicates=True)

        envs_dat = data.pop("environments", [])
        if envs_dat:
            envs = cls.app.EnvironmentsList.from_json_like(
                envs_dat, shared_data=cls.app.template_components
            )
            cls.app.envs.add_objects(envs, skip_duplicates=True)

        ts_dat = data.pop("task_schemas", [])
        if ts_dat:
            task_schemas = cls.app.TaskSchemasList.from_json_like(
                ts_dat, shared_data=cls.app.template_components
            )
            cls.app.task_schemas.add_objects(task_schemas, skip_duplicates=True)

        return cls.from_json_like(data, shared_data=cls.app.template_components)

    @classmethod
    def from_YAML_string(cls, string: str) -> app.WorkflowTemplate:
        """Load from a YAML string.

        Parameters
        ----------
        string
            The YAML string containing the workflow template parametrisation.

        """
        return cls._from_data(read_YAML(string))

    @classmethod
    def from_YAML_file(cls, path: PathLike) -> app.WorkflowTemplate:
        """Load from a YAML file.

        Parameters
        ----------
        path
            The path to the YAML file containing the workflow template parametrisation.

        """
        return cls._from_data(read_YAML_file(path))

    @classmethod
    def from_JSON_string(cls, string: str) -> app.WorkflowTemplate:
        """Load from a JSON string.

        Parameters
        ----------
        string
            The JSON string containing the workflow template parametrisation.

        """
        return cls._from_data(read_JSON_string(string))

    @classmethod
    def from_JSON_file(cls, path: PathLike) -> app.WorkflowTemplate:
        """Load from a JSON file.

        Parameters
        ----------
        path
            The path to the JSON file containing the workflow template parametrisation.

        """
        return cls._from_data(read_JSON_file(path))

    @classmethod
    def from_file(
        cls,
        path: PathLike,
        template_format: Optional[str] = DEFAULT_TEMPLATE_FORMAT,
    ) -> app.WorkflowTemplate:
        """Load from either a YAML or JSON file, depending on the file extension.

        Parameters
        ----------
        path
            The path to the file containing the workflow template parametrisation.
        template_format
            The file format to expect at `path`. One of "json" or "yaml", if specified. By
            default, "yaml".

        """
        path = Path(path)
        fmt = template_format.lower()
        if fmt == "yaml" or path.suffix in (".yaml", ".yml"):
            return cls.from_YAML_file(path)
        elif fmt == "json" or path.suffix == ".json":
            return cls.from_JSON_file(path)
        else:
            raise ValueError(
                f"Unknown workflow template file extension {path.suffix!r}. Supported "
                f"template formats are {ALL_TEMPLATE_FORMATS!r}."
            )

    def _add_empty_task(self, task: app.Task, new_index: int, insert_ID: int) -> None:
        """Called by `Workflow._add_empty_task`."""
        new_task_name = self.workflow._get_new_task_unique_name(task, new_index)

        task._insert_ID = insert_ID
        task._dir_name = f"task_{task.insert_ID}_{new_task_name}"
        task._element_sets = []  # element sets are added to the Task during add_elements

        task.workflow_template = self
        self.tasks.insert(new_index, task)

    def _add_empty_loop(self, loop: app.Loop) -> None:
        """Called by `Workflow._add_empty_loop`."""

        if not loop.name:
            existing = [i.name for i in self.loops]
            new_idx = len(self.loops)
            name = f"loop_{new_idx}"
            while name in existing:
                new_idx += 1
                name = f"loop_{new_idx}"
            loop._name = name
        elif loop.name in self.workflow.loops.list_attrs():
            raise LoopAlreadyExistsError(
                f"A loop with the name {loop.name!r} already exists in the workflow: "
                f"{getattr(self.workflow.loops, loop.name)!r}."
            )

        loop._workflow_template = self
        self.loops.append(loop)


def resolve_fsspec(path: PathLike, **kwargs) -> Tuple[Any, str, str]:
    """
    Parameters
    ----------
    kwargs
        This can include a `password` key, for connections via SSH.

    """

    path = str(path)
    if path.endswith(".zip"):
        # `url_to_fs` does not seem to work for zip combos e.g. `zip::ssh://`, so we
        # construct a `ZipFileSystem` ourselves and assume it is signified only by the
        # file extension:
        fs, pw = ask_pw_on_auth_exc(
            ZipFileSystem,
            fo=path,
            mode="r",
            target_options=kwargs or {},
            add_pw_to="target_options",
        )
        path = ""

    else:
        (fs, path), pw = ask_pw_on_auth_exc(url_to_fs, str(path), **kwargs)
        path = str(Path(path).as_posix())
        if isinstance(fs, LocalFileSystem):
            path = str(Path(path).resolve())

    return fs, path, pw


class Workflow:
    _app_attr = "app"
    _default_ts_fmt = r"%Y-%m-%d %H:%M:%S.%f"
    _default_ts_name_fmt = r"%Y-%m-%d_%H%M%S"
    _input_files_dir_name = "input_files"
    _exec_dir_name = "execute"

    def __init__(
        self,
        path: Union[str, Path],
        store_fmt: Optional[str] = None,
        fs_kwargs: Optional[Dict] = None,
    ):
        """
        Parameters
        ----------

        path :

        """

        self.app.logger.info(f"loading workflow from path: {path}")
        fs_path = str(path)
        fs, path, _ = resolve_fsspec(fs_path or "", **(fs_kwargs or {}))
        store_fmt = store_fmt or infer_store(fs_path, fs)
        store_cls = store_cls_from_str(store_fmt)

        self.path = path

        # assigned on first access:
        self._ts_fmt = None
        self._ts_name_fmt = None
        self._creation_info = None
        self._fs_path = None
        self._template = None
        self._template_components = None
        self._tasks = None
        self._loops = None
        self._submissions = None

        self._store = store_cls(self.app, self, self.path, fs)
        self._in_batch_mode = False  # flag to track when processing batch updates

        # store indices of updates during batch update, so we can revert on failure:
        self._pending = self._get_empty_pending()

    @property
    def name(self):
        """The workflow name may be different from the template name, as it includes the
        creation date-timestamp if generated."""
        # TODO: this won't work for zip stores
        return str(Path(self.fs_path).parts[-1])

    @property
    def store_format(self):
        return self._store._name

    @property
    def num_tasks(self) -> int:
        return len(self.tasks)

    @classmethod
    def from_template(
        cls,
        template: WorkflowTemplate,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> app.Workflow:
        """Generate from a `WorkflowTemplate` object.

        Parameters
        ----------
        template
            The WorkflowTemplate object to make persistent.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        """
        wk = cls._write_empty_workflow(
            template=template,
            path=path,
            name=name,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
        )
        with wk._store.cached_load():
            with wk.batch_update(is_workflow_creation=True):
                for task in template.tasks:
                    wk._add_task(task)
                for loop in template.loops:
                    wk._add_loop(loop)
        return wk

    @classmethod
    def from_YAML_file(
        cls,
        YAML_path: PathLike,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> app.Workflow:
        """Generate from a YAML file.

        Parameters
        ----------
        YAML_path
            The path to a workflow template in the YAML file format.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        """
        template = cls.app.WorkflowTemplate.from_YAML_file(YAML_path)
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
        )

    @classmethod
    def from_YAML_string(
        cls,
        YAML_str: PathLike,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> app.Workflow:
        """Generate from a YAML string.

        Parameters
        ----------
        YAML_str
            The YAML string containing a workflow template parametrisation.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        """
        template = cls.app.WorkflowTemplate.from_YAML_string(YAML_str)
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
        )

    @classmethod
    def from_JSON_file(
        cls,
        JSON_path: PathLike,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> app.Workflow:
        """Generate from a JSON file.

        Parameters
        ----------
        JSON_path
            The path to a workflow template in the JSON file format.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        """
        template = cls.app.WorkflowTemplate.from_JSON_file(JSON_path)
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
        )

    @classmethod
    def from_JSON_string(
        cls,
        JSON_str: PathLike,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> app.Workflow:
        """Generate from a JSON string.

        Parameters
        ----------
        JSON_str
            The JSON string containing a workflow template parametrisation.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        """
        template = cls.app.WorkflowTemplate.from_JSON_string(JSON_str)
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
        )

    @classmethod
    def from_file(
        cls,
        template_path: PathLike,
        template_format: Optional[str] = None,
        path: Optional[str] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> app.Workflow:
        """Generate from either a YAML or JSON file, depending on the file extension.

        Parameters
        ----------
        template_path
            The path to a template file in YAML or JSON format, and with a ".yml",
            ".yaml", or ".json" extension.
        template_format
            If specified, one of "json" or "yaml". This forces parsing from a particular
            format regardless of the file extension.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the `WorkflowTemplate` name will be used,
            in combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        """
        template = cls.app.WorkflowTemplate.from_file(template_path, template_format)
        return cls.from_template(
            template,
            path,
            name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
        )

    @classmethod
    def from_template_data(
        cls,
        template_name: str,
        tasks: Optional[List[app.Task]] = None,
        loops: Optional[List[app.Loop]] = None,
        resources: Optional[Dict[str, Dict]] = None,
        path: Optional[PathLike] = None,
        workflow_name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> app.Workflow:
        """Generate from the data associated with a WorkflowTemplate object.

        Parameters
        ----------
        template_name
            Name of the new workflow template, from which the new workflow will be
            generated.
        tasks
            List of Task objects to add to the new workflow.
        loops
            List of Loop objects to add to the new workflow.
        resources
            Mapping of action scopes to resource requirements, to be applied to all
            element sets in the workflow. `resources` specified in an element set take
            precedence of those defined here for the whole workflow.
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        workflow_name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified `template_name` will be used, in
            combination with a date-timestamp.
        overwrite
            If True and the workflow directory (`path` + `name`) already exists, the
            existing directory will be overwritten.
        store
            The persistent store to use for this workflow.
        ts_fmt
            The datetime format to use for storing datetimes. Datetimes are always stored
            in UTC (because Numpy does not store time zone info), so this should not
            include a time zone name.
        ts_name_fmt
            The datetime format to use when generating the workflow name, where it
            includes a timestamp.
        """
        template = cls.app.WorkflowTemplate(
            template_name,
            tasks=tasks or [],
            loops=loops or [],
            resources=resources,
        )
        return cls.from_template(
            template,
            path,
            workflow_name,
            overwrite,
            store,
            ts_fmt,
            ts_name_fmt,
        )

    def _add_empty_task(
        self,
        task: app.Task,
        new_index: Optional[int] = None,
    ) -> app.WorkflowTask:
        if new_index is None:
            new_index = self.num_tasks

        insert_ID = self.num_added_tasks

        # make a copy with persistent schema inputs:
        task_c, _ = task.to_persistent(self, insert_ID)

        # add to the WorkflowTemplate:
        self.template._add_empty_task(task_c, new_index, insert_ID)

        # create and insert a new WorkflowTask:
        self.tasks.add_object(
            self.app.WorkflowTask.new_empty_task(self, task_c, new_index),
            index=new_index,
        )

        # update persistent store:
        task_js, temp_comps_js = task_c.to_json_like()
        self._store.add_template_components(temp_comps_js)
        self._store.add_task(new_index, task_js)

        # update in-memory workflow template components:
        temp_comps = self.app.template_components_from_json_like(temp_comps_js)
        for comp_type, comps in temp_comps.items():
            for comp in comps:
                comp._set_hash()
                if comp not in self.template_components[comp_type]:
                    idx = self.template_components[comp_type].add_object(comp)
                    self._pending["template_components"][comp_type].append(idx)

        self._pending["tasks"].append(new_index)

        return self.tasks[new_index]

    def _add_task(self, task: app.Task, new_index: Optional[int] = None) -> None:
        new_wk_task = self._add_empty_task(task=task, new_index=new_index)
        new_wk_task._add_elements(element_sets=task.element_sets)  # TODO

    def add_task(self, task: app.Task, new_index: Optional[int] = None) -> None:
        with self._store.cached_load():
            with self.batch_update():
                self._add_task(task, new_index=new_index)

    def add_task_after(self, new_task: app.Task, task_ref: app.Task = None) -> None:
        """Add a new task after the specified task.

        Parameters
        ----------
        task_ref
            If not given, the new task will be added at the end of the workflow.

        """
        new_index = task_ref.index + 1 if task_ref else None
        self.add_task(new_task, new_index)
        # TODO: add new downstream elements?

    def add_task_before(self, new_task: app.Task, task_ref: app.Task = None) -> None:
        """Add a new task before the specified task.

        Parameters
        ----------
        task_ref
            If not given, the new task will be added at the beginning of the workflow.

        """
        new_index = task_ref.index if task_ref else 0
        self.add_task(new_task, new_index)
        # TODO: add new downstream elements?

    def _add_empty_loop(self, loop: app.Loop) -> app.WorkflowLoop:
        """Add a new loop (zeroth iterations only) to the workflow."""

        new_index = self.num_loops

        # don't modify passed object:
        loop_c = copy.deepcopy(loop)

        # add to the WorkflowTemplate:
        self.template._add_empty_loop(loop_c)

        # create and insert a new WorkflowLoop:
        self.loops.add_object(
            self.app.WorkflowLoop.new_empty_loop(
                index=new_index,
                workflow=self,
                template=loop_c,
            )
        )
        wk_loop = self.loops[new_index]

        loop_js, _ = loop_c.to_json_like()

        # all these element iterations will be initialised for the new loop:
        iter_IDs = [
            i.id_ for i in self.get_element_iterations_of_tasks(loop_c.task_insert_IDs)
        ]

        # update persistent store:
        self._store.add_loop(
            loop_template=loop_js,
            iterable_parameters=wk_loop.iterable_parameters,
            iter_IDs=iter_IDs,
        )

        self._pending["loops"].append(new_index)

        return wk_loop

    def _add_loop(self, loop: app.Loop, parent_loop_indices: Dict = None) -> None:
        new_wk_loop = self._add_empty_loop(loop)
        if loop.num_iterations is not None:
            # fixed number of iterations, so add remaining N > 0 iterations:
            for _ in range(loop.num_iterations - 1):
                new_wk_loop.add_iteration(parent_loop_indices=parent_loop_indices)

    def add_loop(self, loop: app.Loop, parent_loop_indices: Dict = None) -> None:
        """Add a loop to a subset of workflow tasks."""
        with self._store.cached_load():
            with self.batch_update():
                self._add_loop(loop, parent_loop_indices)

    @property
    def fs_path(self):
        if not self._fs_path:
            self._fs_path = self._store.get_fs_path()
        return self._fs_path

    @property
    def creation_info(self):
        if not self._creation_info:
            info = self._store.get_creation_info()
            info["create_time"] = (
                datetime.strptime(info["create_time"], self.ts_fmt)
                .replace(tzinfo=timezone.utc)
                .astimezone()
            )
            self._creation_info = info
        return self._creation_info

    @property
    def id_(self):
        return self.creation_info["id"]

    @property
    def ts_fmt(self):
        if not self._ts_fmt:
            self._ts_fmt = self._store.get_ts_fmt()
        return self._ts_fmt

    @property
    def ts_name_fmt(self):
        if not self._ts_name_fmt:
            self._ts_name_fmt = self._store.get_ts_name_fmt()
        return self._ts_name_fmt

    @property
    def template_components(self) -> Dict:
        if self._template_components is None:
            with self._store.cached_load():
                tc_js = self._store.get_template_components()
            self._template_components = self.app.template_components_from_json_like(tc_js)
        return self._template_components

    @property
    def template(self) -> app.WorkflowTemplate:
        if self._template is None:
            with self._store.cached_load():
                temp_js = self._store.get_template()

                # TODO: insert_ID and id_ are the same thing:
                for task in temp_js["tasks"]:
                    task.pop("id_", None)

                template = self.app.WorkflowTemplate.from_json_like(
                    temp_js, self.template_components
                )
                template.workflow = self
            self._template = template

        return self._template

    @property
    def tasks(self) -> app.WorkflowTaskList:
        if self._tasks is None:
            with self._store.cached_load():
                all_tasks = self._store.get_tasks()
                wk_tasks = []
                for i in all_tasks:
                    wk_task = self.app.WorkflowTask(
                        workflow=self,
                        template=self.template.tasks[i.index],
                        index=i.index,
                        element_IDs=i.element_IDs,
                    )
                    wk_tasks.append(wk_task)
                self._tasks = self.app.WorkflowTaskList(wk_tasks)

        return self._tasks

    @property
    def loops(self) -> app.WorkflowLoopList:
        if self._loops is None:
            with self._store.cached_load():
                wk_loops = []
                for idx, loop_dat in self._store.get_loops().items():
                    wk_loop = self.app.WorkflowLoop(
                        index=idx,
                        workflow=self,
                        template=self.template.loops[idx],
                        num_added_iterations=loop_dat["num_added_iterations"],
                        iterable_parameters=loop_dat["iterable_parameters"],
                    )
                    wk_loops.append(wk_loop)
                self._loops = self.app.WorkflowLoopList(wk_loops)
        return self._loops

    @property
    def submissions(self) -> List[app.Submission]:
        if self._submissions is None:
            with self._store.cached_load():
                subs = []
                for idx, sub_dat in self._store.get_submissions().items():
                    sub_js = {"index": idx, **sub_dat}
                    sub = self.app.Submission.from_json_like(sub_js)
                    sub.workflow = self
                    subs.append(sub)
                self._submissions = subs
        return self._submissions

    @property
    def num_added_tasks(self) -> int:
        return self._store._get_num_total_added_tasks()

    def get_store_EARs(self, id_lst: Iterable[int]) -> List[AnySEAR]:
        return self._store.get_EARs(id_lst)

    def get_store_element_iterations(
        self, id_lst: Iterable[int]
    ) -> List[AnySElementIter]:
        return self._store.get_element_iterations(id_lst)

    def get_store_elements(self, id_lst: Iterable[int]) -> List[AnySElement]:
        return self._store.get_elements(id_lst)

    def get_store_tasks(self, id_lst: Iterable[int]) -> List[AnySTask]:
        return self._store.get_tasks_by_IDs(id_lst)

    def get_element_iteration_IDs_from_EAR_IDs(self, id_lst: Iterable[int]) -> List[int]:
        return [i.elem_iter_ID for i in self.get_store_EARs(id_lst)]

    def get_element_IDs_from_EAR_IDs(self, id_lst: Iterable[int]) -> List[int]:
        iter_IDs = self.get_element_iteration_IDs_from_EAR_IDs(id_lst)
        return [i.element_ID for i in self.get_store_element_iterations(iter_IDs)]

    def get_task_IDs_from_element_IDs(self, id_lst: Iterable[int]) -> List[int]:
        return [i.task_ID for i in self.get_store_elements(id_lst)]

    def get_EAR_IDs_of_tasks(self, id_lst: int) -> List[int]:
        """Get EAR IDs belonging to multiple tasks"""
        return [i.id_ for i in self.get_EARs_of_tasks(id_lst)]

    def get_EARs_of_tasks(self, id_lst: Iterable[int]) -> List[app.ElementActionRun]:
        """Get EARs belonging to multiple tasks"""
        EARs = []
        for i in id_lst:
            task = self.tasks.get(insert_ID=i)
            for elem in task.elements[:]:
                for iter_ in elem.iterations:
                    for run in iter_.action_runs:
                        EARs.append(run)
        return EARs

    def get_element_iterations_of_tasks(
        self, id_lst: Iterable[int]
    ) -> List[app.ElementIteration]:
        """Get element iterations belonging to multiple tasks"""
        iters = []
        for i in id_lst:
            task = self.tasks.get(insert_ID=i)
            for elem in task.elements[:]:
                for iter_i in elem.iterations:
                    iters.append(iter_i)
        return iters

    def get_elements_from_IDs(self, id_lst: Iterable[int]) -> List[app.Element]:
        """Return element objects from a list of IDs."""

        store_elems = self._store.get_elements(id_lst)

        task_IDs = [i.task_ID for i in store_elems]
        store_tasks = self._store.get_tasks_by_IDs(task_IDs)

        index_paths = []
        for el, tk in zip(store_elems, store_tasks):
            elem_idx = tk.element_IDs.index(el.id_)
            index_paths.append(
                {
                    "elem_idx": elem_idx,
                    "task_idx": tk.index,
                }
            )

        objs = []
        for idx_dat in index_paths:
            task = self.tasks[idx_dat["task_idx"]]
            elem = task.elements[idx_dat["elem_idx"]]
            objs.append(elem)

        return objs

    def get_element_iterations_from_IDs(
        self, id_lst: Iterable[int]
    ) -> List[app.ElementIteration]:
        """Return element iteration objects from a list of IDs."""

        store_iters = self._store.get_element_iterations(id_lst)

        elem_IDs = [i.element_ID for i in store_iters]
        store_elems = self._store.get_elements(elem_IDs)

        task_IDs = [i.task_ID for i in store_elems]
        store_tasks = self._store.get_tasks_by_IDs(task_IDs)

        index_paths = []
        for it, el, tk in zip(store_iters, store_elems, store_tasks):
            iter_idx = el.iteration_IDs.index(it.id_)
            elem_idx = tk.element_IDs.index(el.id_)
            index_paths.append(
                {
                    "iter_idx": iter_idx,
                    "elem_idx": elem_idx,
                    "task_idx": tk.index,
                }
            )

        objs = []
        for idx_dat in index_paths:
            task = self.tasks[idx_dat["task_idx"]]
            elem = task.elements[idx_dat["elem_idx"]]
            iter_ = elem.iterations[idx_dat["iter_idx"]]
            objs.append(iter_)

        return objs

    def get_EARs_from_IDs(self, id_lst: Iterable[int]) -> List[app.ElementActionRun]:
        """Return element action run objects from a list of IDs."""

        store_EARs = self._store.get_EARs(id_lst)

        elem_iter_IDs = [i.elem_iter_ID for i in store_EARs]
        store_iters = self._store.get_element_iterations(elem_iter_IDs)

        elem_IDs = [i.element_ID for i in store_iters]
        store_elems = self._store.get_elements(elem_IDs)

        task_IDs = [i.task_ID for i in store_elems]
        store_tasks = self._store.get_tasks_by_IDs(task_IDs)

        index_paths = []
        for rn, it, el, tk in zip(store_EARs, store_iters, store_elems, store_tasks):
            act_idx = rn.action_idx
            run_idx = it.EAR_IDs[act_idx].index(rn.id_)
            iter_idx = el.iteration_IDs.index(it.id_)
            elem_idx = tk.element_IDs.index(el.id_)
            index_paths.append(
                {
                    "run_idx": run_idx,
                    "action_idx": act_idx,
                    "iter_idx": iter_idx,
                    "elem_idx": elem_idx,
                    "task_idx": tk.index,
                }
            )

        objs = []
        for idx_dat in index_paths:
            task = self.tasks[idx_dat["task_idx"]]
            elem = task.elements[idx_dat["elem_idx"]]
            iter_ = elem.iterations[idx_dat["iter_idx"]]
            run = iter_.actions[idx_dat["action_idx"]].runs[idx_dat["run_idx"]]
            objs.append(run)

        return objs

    def get_all_elements(self) -> List[app.Element]:
        return self.get_elements_from_IDs(range(self.num_elements))

    def get_all_element_iterations(self) -> List[app.ElementIteration]:
        return self.get_element_iterations_from_IDs(range(self.num_element_iterations))

    def get_all_EARs(self) -> List[app.ElementActionRun]:
        return self.get_EARs_from_IDs(range(self.num_EARs))

    @contextmanager
    def batch_update(self, is_workflow_creation: bool = False) -> Iterator[None]:
        """A context manager that batches up structural changes to the workflow and
        commits them to disk all together when the context manager exits."""

        if self._in_batch_mode:
            yield
        else:
            try:
                self.app.persistence_logger.info(
                    f"entering batch update (is_workflow_creation={is_workflow_creation!r})"
                )
                self._in_batch_mode = True
                yield

            except Exception as err:
                self.app.persistence_logger.error("batch update exception!")
                self._in_batch_mode = False
                self._store._pending.reset()

                for task in self.tasks:
                    task._reset_pending_element_IDs()
                    task.template._reset_pending_element_sets()

                for loop in self.loops:
                    loop._reset_pending_num_added_iters()

                self._reject_pending()

                if is_workflow_creation:
                    # creation failed, so no need to keep the newly generated workflow:
                    self._store.delete_no_confirm()
                    self._store.reinstate_replaced_dir()

                raise err

            else:
                if self._store._pending:
                    # is_diff = self._store.is_modified_on_disk()
                    # if is_diff:
                    #     raise WorkflowBatchUpdateFailedError(
                    #         f"Workflow modified on disk since it was loaded!"
                    #     )

                    for task in self.tasks:
                        task._accept_pending_element_IDs()
                        task.template._accept_pending_element_sets()

                    for loop in self.loops:
                        loop._accept_pending_num_added_iters()

                    if is_workflow_creation:
                        self._store.remove_replaced_dir()

                    # TODO: handle errors in commit pending?
                    self._store._pending.commit_all()

                    self._accept_pending()
                    self.app.persistence_logger.info("exiting batch update")
                    self._in_batch_mode = False

    @classmethod
    def temporary_rename(cls, path: str, fs) -> List[str]:
        """Rename an existing same-path workflow (directory) so we can restore it if
        workflow creation fails.

        Renaming will occur until the successfully completed. This means multiple new
        paths may be created, where only the final path should be considered the
        successfully renamed workflow. Other paths will be deleted."""

        all_replaced = []

        @cls.app.perm_error_retry()
        def _temp_rename(path: str, fs) -> str:
            temp_ext = "".join(random.choices(string.ascii_letters, k=10))
            replaced = str(Path(f"{path}.{temp_ext}").as_posix())
            cls.app.persistence_logger.debug(
                f"temporary_rename: _temp_rename: {path!r} --> {replaced!r}."
            )
            all_replaced.append(replaced)
            try:
                fs.rename(path, replaced, recursive=True)
            except TypeError:
                # `SFTPFileSystem.rename` has no `recursive` argument:
                fs.rename(path, replaced)
            return replaced

        @cls.app.perm_error_retry()
        def _remove_path(path: str, fs) -> None:
            cls.app.persistence_logger.debug(f"temporary_rename: _remove_path: {path!r}.")
            while fs.exists(path):
                fs.rm(path, recursive=True)
                time.sleep(0.5)

        _temp_rename(path, fs)

        for i in all_replaced[:-1]:
            _remove_path(i, fs)

        return all_replaced[-1]

    @classmethod
    def _write_empty_workflow(
        cls,
        template: app.WorkflowTemplate,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
        fs_kwargs: Optional[Dict] = None,
    ) -> app.Workflow:
        """
        Parameters
        ----------
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.

        """
        ts = datetime.now()

        # store all times in UTC, since NumPy doesn't support time zone info:
        ts_utc = ts.astimezone(tz=timezone.utc)

        ts_name_fmt = ts_name_fmt or cls._default_ts_name_fmt
        ts_fmt = ts_fmt or cls._default_ts_fmt

        name = name or f"{template.name}_{ts.strftime(ts_name_fmt)}"

        fs_path = f"{path or '.'}/{name}"
        fs_kwargs = fs_kwargs or {}
        fs, path, pw = resolve_fsspec(path or "", **fs_kwargs)
        wk_path = f"{path}/{name}"

        replaced_wk = None
        if fs.exists(wk_path):
            cls.app.logger.debug("workflow path exists")
            if overwrite:
                cls.app.logger.debug("renaming existing workflow path")
                replaced_wk = cls.temporary_rename(wk_path, fs)
            else:
                raise ValueError(
                    f"Path already exists: {wk_path} on file system " f"{fs!r}."
                )

        # make template-level inputs/resources think they are persistent:
        wk_dummy = _DummyPersistentWorkflow()
        param_src = {"type": "workflow_resources"}
        for res_i in template.resources:
            res_i.make_persistent(wk_dummy, param_src)

        template_js, template_sh = template.to_json_like(exclude=["tasks", "loops"])
        template_js["tasks"] = []
        template_js["loops"] = []

        creation_info = {
            "app_info": cls.app.get_info(),
            "create_time": ts_utc.strftime(ts_fmt),
            "id": str(uuid4()),
        }

        store_cls = store_cls_from_str(store)
        store_cls.write_empty_workflow(
            app=cls.app,
            template_js=template_js,
            template_components_js=template_sh,
            wk_path=wk_path,
            fs=fs,
            fs_path=fs_path,
            replaced_wk=replaced_wk,
            creation_info=creation_info,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
        )

        fs_kwargs = {"password": pw, **fs_kwargs}
        wk = cls(fs_path, store_fmt=store, fs_kwargs=fs_kwargs)

        # actually make template inputs/resources persistent, now the workflow exists:
        wk_dummy.make_persistent(wk)

        return wk

    def to_zip(self, log=None) -> str:
        return self._store.to_zip(log=log)

    def copy(self, path=None) -> str:
        """Copy the workflow to a new path and return the copied workflow path."""
        return self._store.copy(path)

    def delete(self):
        self._store.delete()

    def _delete_no_confirm(self):
        self._store.delete_no_confirm()

    def get_parameters(
        self, id_lst: Iterable[int], **kwargs: Dict
    ) -> List[AnySParameter]:
        return self._store.get_parameters(id_lst, **kwargs)

    def get_parameter_sources(self, id_lst: Iterable[int]) -> List[Dict]:
        return self._store.get_parameter_sources(id_lst)

    def get_parameter_set_statuses(self, id_lst: Iterable[int]) -> List[bool]:
        return self._store.get_parameter_set_statuses(id_lst)

    def get_parameter(self, index: int, **kwargs: Dict) -> AnySParameter:
        return self.get_parameters([index], **kwargs)[0]

    def get_parameter_data(self, index: int, **kwargs: Dict) -> Any:
        return (
            self.get_parameter(index, **kwargs).data
            or self.get_parameter(index, **kwargs).file
        )

    def get_parameter_source(self, index: int) -> Dict:
        return self.get_parameter_sources([index])[0]

    def is_parameter_set(self, index: int) -> bool:
        return self.get_parameter_set_statuses([index])[0]

    def get_all_parameters(self, **kwargs: Dict) -> List[AnySParameter]:
        """Retrieve all store parameters."""
        num_params = self._store._get_num_total_parameters()
        id_lst = list(range(num_params))
        return self._store.get_parameters(id_lst, **kwargs)

    def get_all_parameter_data(self, **kwargs: Dict) -> Dict[int, Any]:
        """Retrieve all workflow parameter data."""
        params = self.get_all_parameters(**kwargs)
        return {i.id_: (i.data or i.file) for i in params}

    def check_parameters_exist(
        self, id_lst: Union[int, List[int]]
    ) -> Union[bool, List[bool]]:
        is_multi = True
        if isinstance(id_lst, int):
            is_multi = False
            id_lst = [id_lst]
        exists = self._store.check_parameters_exist(id_lst)
        if not is_multi:
            exists = exists[0]
        return exists

    def _add_unset_parameter_data(self, source: Dict) -> int:
        # TODO: use this for unset files as well
        return self._store.add_unset_parameter(source)

    def _add_parameter_data(self, data, source: Dict) -> int:
        return self._store.add_set_parameter(data, source)

    def _add_file(
        self,
        store_contents: bool,
        is_input: bool,
        source: Dict,
        path=None,
        contents=None,
        filename: str = None,
    ) -> int:
        return self._store.add_file(
            store_contents=store_contents,
            is_input=is_input,
            source=source,
            path=path,
            contents=contents,
            filename=filename,
        )

    def _set_file(
        self,
        param_id: int,
        store_contents: bool,
        is_input: bool,
        path=None,
        contents=None,
        filename: str = None,
        clean_up: bool = False,
    ) -> int:
        self._store.set_file(
            param_id=param_id,
            store_contents=store_contents,
            is_input=is_input,
            path=path,
            contents=contents,
            filename=filename,
            clean_up=clean_up,
        )

    def get_task_unique_names(
        self, map_to_insert_ID: bool = False
    ) -> Union[List[str], Dict[str, int]]:
        """Return the unique names of all workflow tasks.

        Parameters
        ----------
        map_to_insert_ID : bool, optional
            If True, return a dict whose values are task insert IDs, otherwise return a
            list.

        """
        names = self.app.Task.get_task_unique_names(self.template.tasks)
        if map_to_insert_ID:
            insert_IDs = (i.insert_ID for i in self.template.tasks)
            return dict(zip(names, insert_IDs))
        else:
            return names

    def _get_new_task_unique_name(self, new_task: app.Task, new_index: int) -> str:
        task_templates = list(self.template.tasks)
        task_templates.insert(new_index, new_task)
        uniq_names = self.app.Task.get_task_unique_names(task_templates)

        return uniq_names[new_index]

    def _get_empty_pending(self) -> Dict:
        return {
            "template_components": {k: [] for k in TEMPLATE_COMP_TYPES},
            "tasks": [],  # list of int
            "loops": [],  # list of int
            "submissions": [],  # list of int
        }

    def _accept_pending(self) -> None:
        self._reset_pending()

    def _reset_pending(self) -> None:
        self._pending = self._get_empty_pending()

    def _reject_pending(self) -> None:
        """Revert pending changes to the in-memory representation of the workflow.

        This deletes new tasks, new template component data, new loops, and new
        submissions. Element additions to existing (non-pending) tasks are separately
        rejected/accepted by the WorkflowTask object.

        """
        for task_idx in self._pending["tasks"][::-1]:
            # iterate in reverse so the index references are correct
            self.tasks._remove_object(task_idx)
            self.template.tasks.pop(task_idx)

        for comp_type, comp_indices in self._pending["template_components"].items():
            for comp_idx in comp_indices[::-1]:
                # iterate in reverse so the index references are correct
                self.template_components[comp_type]._remove_object(comp_idx)

        for loop_idx in self._pending["loops"][::-1]:
            # iterate in reverse so the index references are correct
            self.loops._remove_object(loop_idx)
            self.template.loops.pop(loop_idx)

        for sub_idx in self._pending["submissions"][::-1]:
            # iterate in reverse so the index references are correct
            self._submissions.pop(sub_idx)

        self._reset_pending()

    @property
    def num_tasks(self):
        return self._store._get_num_total_tasks()

    @property
    def num_submissions(self):
        return self._store._get_num_total_submissions()

    @property
    def num_elements(self):
        return self._store._get_num_total_elements()

    @property
    def num_element_iterations(self):
        return self._store._get_num_total_elem_iters()

    @property
    def num_EARs(self):
        return self._store._get_num_total_EARs()

    @property
    def num_loops(self) -> int:
        return self._store._get_num_total_loops()

    @property
    def artifacts_path(self):
        # TODO: allow customisation of artifacts path at submission and resources level
        return Path(self.path) / "artifacts"

    @property
    def input_files_path(self):
        return self.artifacts_path / self._input_files_dir_name

    @property
    def submissions_path(self):
        return self.artifacts_path / "submissions"

    @property
    def task_artifacts_path(self):
        return self.artifacts_path / "tasks"

    @property
    def execution_path(self):
        return Path(self.path) / self._exec_dir_name

    def get_task_elements(self, task: app.Task, selection: slice) -> List[app.Element]:
        return [
            self.app.Element(task=task, **{k: v for k, v in i.items() if k != "task_ID"})
            for i in self._store.get_task_elements(task.insert_ID, selection)
        ]

    def set_EAR_submission_index(self, EAR_ID: int, sub_idx: int) -> None:
        """Set the submission index of an EAR."""
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EAR_submission_index(EAR_ID, sub_idx)

    def set_EAR_start(self, EAR_ID: int) -> None:
        """Set the start time on an EAR."""
        self.app.logger.debug(f"Setting start for EAR ID {EAR_ID!r}")
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EAR_start(EAR_ID)

    def set_EAR_end(self, EAR_ID: int, exit_code: int) -> None:
        """Set the end time and exit code on an EAR.

        If the exit code is non-zero, also set all downstream dependent EARs to be
        skipped. Also save any generated input/output files.

        """
        self.app.logger.debug(
            f"Setting end for EAR ID {EAR_ID!r} with exit code {exit_code!r}."
        )
        with self._store.cached_load():
            EAR = self.get_EARs_from_IDs([EAR_ID])[0]
            with self.batch_update():
                success = exit_code == 0  # TODO  more sophisticated success heuristics
                if EAR.action.abortable and exit_code == ABORT_EXIT_CODE:
                    # the point of aborting an EAR is to continue with the workflow:
                    success = True
                self._store.set_EAR_end(EAR_ID, exit_code, success)

                for IFG_i in EAR.action.input_file_generators:
                    inp_file = IFG_i.input_file
                    self.app.logger.debug(
                        f"Saving EAR input file: {inp_file.label!r} for EAR ID "
                        f"{EAR_ID!r}."
                    )
                    param_id = EAR.data_idx[f"input_files.{inp_file.label}"]

                    file_paths = inp_file.value()
                    if not isinstance(file_paths, list):
                        file_paths = [file_paths]

                    for path_i in file_paths:
                        self._set_file(
                            param_id=param_id,
                            store_contents=True,  # TODO: make optional according to IFG
                            is_input=False,
                            path=Path(path_i).resolve(),
                        )

                for OFP_i in EAR.action.output_file_parsers:
                    for save_file_j in OFP_i.save_files:
                        self.app.logger.debug(
                            f"Saving EAR output file: {save_file_j.label!r} for EAR ID "
                            f"{EAR_ID!r}."
                        )
                        try:
                            param_id = EAR.data_idx[f"output_files.{save_file_j.label}"]
                        except KeyError:
                            # We might be saving a file that is not a defined
                            # "output file"; this will avoid saving a reference in the
                            # parameter data:
                            param_id = None

                        file_paths = save_file_j.value()
                        if not isinstance(file_paths, list):
                            file_paths = [file_paths]

                        for path_i in file_paths:
                            self._set_file(
                                param_id=param_id,
                                store_contents=True,  # TODO: make optional according to OFP
                                is_input=False,
                                path=Path(path_i).resolve(),
                                clean_up=(save_file_j in OFP_i.clean_up),
                            )

                if not success:
                    for EAR_dep_ID in EAR.get_dependent_EARs(as_objects=False):
                        # TODO: this needs to be recursive?
                        self.app.logger.debug(
                            f"Setting EAR ID {EAR_dep_ID!r} to skip because it depends on"
                            f" EAR ID {EAR_ID!r}, which exited with a non-zero exit code:"
                            f" {exit_code!r}."
                        )
                        self._store.set_EAR_skip(EAR_dep_ID)

    def set_EAR_skip(self, EAR_ID: int) -> None:
        """Record that an EAR is to be skipped due to an upstream failure."""
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EAR_skip(EAR_ID)

    def get_EAR_skipped(self, EAR_ID: int) -> None:
        """Check if an EAR is to be skipped."""
        with self._store.cached_load():
            return self._store.get_EAR_skipped(EAR_ID)

    def set_parameter_value(self, param_id: int, value: Any) -> None:
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_parameter_value(param_id, value)

    def elements(self) -> Iterator[app.Element]:
        for task in self.tasks:
            for element in task.elements[:]:
                yield element

    def get_iteration_task_pathway(self):
        pathway = []
        for task in self.tasks:
            loop_idx = {}
            pathway.append((task.insert_ID, loop_idx))

        for loop in self.loops:  # TODO: order by depth (inner loops first?)
            task_subset = loop.task_insert_IDs
            subset_idx = [idx for idx, i in enumerate(pathway) if i[0] in task_subset]
            looped_pathway = []
            for iter_i in range(loop.num_added_iterations):
                for j in subset_idx:
                    item_j = copy.deepcopy(pathway[j])
                    item_j[1][loop.name] = iter_i
                    looped_pathway.append(item_j)

            # replaced pathway `sub_idx` items with `looped_pathway` items:
            pathway = replace_items(
                pathway, subset_idx[0], subset_idx[-1] + 1, looped_pathway
            )

        return pathway

    def _submit(
        self,
        status,
        ignore_errors: Optional[bool] = False,
        JS_parallelism: Optional[bool] = None,
        print_stdout: Optional[bool] = False,
    ) -> Tuple[List[Exception], Dict[int, int]]:
        """Submit outstanding EARs for execution."""

        # generate a new submission if there are no pending submissions:
        pending = [i for i in self.submissions if i.needs_submit]
        if not pending:
            status.update("Adding new submission...")
            new_sub = self._add_submission(JS_parallelism=JS_parallelism)
            if not new_sub:
                status.stop()
                raise ValueError("No pending element action runs to submit!")
            pending = [new_sub]

        self.submissions_path.mkdir(exist_ok=True, parents=True)
        self.execution_path.mkdir(exist_ok=True, parents=True)
        self.task_artifacts_path.mkdir(exist_ok=True, parents=True)

        # for direct execution the submission must be persistent at submit-time, because
        # it will be read by a new instance of the app:
        status.update("Committing to the store...")
        self._store._pending.commit_all()

        # submit all pending submissions:
        exceptions = []
        submitted_js = {}
        for sub in pending:
            try:
                status.update(f"Preparing submission {sub.index}...")
                sub_js_idx = sub.submit(
                    status=status,
                    ignore_errors=ignore_errors,
                    print_stdout=print_stdout,
                )
                submitted_js[sub.index] = sub_js_idx
            except SubmissionFailure as exc:
                exceptions.append(exc)

        return exceptions, submitted_js

    def submit(
        self,
        ignore_errors: Optional[bool] = False,
        JS_parallelism: Optional[bool] = None,
        print_stdout: Optional[bool] = False,
        wait: bool = False,
    ) -> Dict[int, int]:
        """"""

        console = rich.console.Console()
        status = console.status("Submitting workflow...")
        status.start()

        with self._store.cached_load():
            if not self._store.is_submittable:
                status.stop()
                raise NotImplementedError("The workflow is not submittable.")
            with self.batch_update():
                # commit updates before raising exception:
                exceptions, submitted_js = self._submit(
                    ignore_errors=ignore_errors,
                    JS_parallelism=JS_parallelism,
                    print_stdout=print_stdout,
                    status=status,
                )

        if exceptions:
            msg = "\n" + "\n\n".join([i.message for i in exceptions])
            status.stop()
            raise WorkflowSubmissionFailure(msg)

        status.stop()

        if wait:
            self.wait(submitted_js)

        return submitted_js

    def wait(self, sub_js: Optional[Dict] = None):
        """Wait for the completion of specified/all submitted jobscripts."""

        # TODO: think about how this might work with remote workflow submission (via SSH)

        def wait_for_direct_jobscripts(jobscripts: List[app.Jobscript]):
            """Wait for the passed direct (i.e. non-scheduled) jobscripts to finish."""

            def callback(proc):
                js = js_pids[proc.pid]
                # TODO sometimes proc.returncode is None; maybe because multiple wait
                # calls?
                print(
                    f"Jobscript {js.index} from submission {js.submission.index} "
                    f"finished with exit code {proc.returncode}."
                )

            js_pids = {i.process_ID: i for i in jobscripts}
            process_refs = [(i.process_ID, i.submit_cmdline) for i in jobscripts]
            DirectScheduler.wait_for_jobscripts(process_refs, callback=callback)

        def wait_for_scheduled_jobscripts(jobscripts: List[app.Jobscript]):
            """Wait for the passed scheduled jobscripts to finish."""
            schedulers = app.Submission.get_unique_schedulers_of_jobscripts(jobscripts)
            threads = []
            for js_indices, sched in schedulers.items():
                jobscripts = [
                    self.submissions[sub_idx].jobscripts[js_idx]
                    for sub_idx, js_idx in js_indices
                ]
                job_IDs = [i.scheduler_job_ID for i in jobscripts]
                threads.append(Thread(target=sched.wait_for_jobscripts, args=(job_IDs,)))

            for i in threads:
                i.start()

            for i in threads:
                i.join()

        # TODO: add a log file to the submission dir where we can log stuff (e.g starting
        # a thread...)

        if not sub_js:
            # find any active jobscripts first:
            sub_js = {}
            for sub in self.submissions:
                for js_idx in sub.get_active_jobscripts():
                    sub_js[sub.index].append(js_idx)

        js_direct = []
        js_sched = []
        for sub_idx, all_js_idx in sub_js.items():
            for js_idx in all_js_idx:
                try:
                    js = self.submissions[sub_idx].jobscripts[js_idx]
                except IndexError:
                    raise ValueError(
                        f"No jobscript with submission index {sub_idx!r} and/or "
                        f"jobscript index {js_idx!r}."
                    )
                if js.process_ID is not None:
                    js_direct.append(js)
                elif js.scheduler_job_ID is not None:
                    js_sched.append(js)
                else:
                    raise RuntimeError(
                        f"Process ID nor scheduler job ID is set for {js!r}."
                    )

        if js_direct or js_sched:
            # TODO: use a rich console status? how would that appear in stdout though?
            print("Waiting for workflow submissions to finish...")
        else:
            print("No running jobscripts.")
            return

        try:
            t_direct = Thread(target=wait_for_direct_jobscripts, args=(js_direct,))
            t_sched = Thread(target=wait_for_scheduled_jobscripts, args=(js_sched,))
            t_direct.start()
            t_sched.start()

            # without these, KeyboardInterrupt seems to not be caught:
            while t_direct.is_alive():
                t_direct.join(timeout=1)

            while t_sched.is_alive():
                t_sched.join(timeout=1)

        except KeyboardInterrupt:
            print("No longer waiting (workflow execution will continue).")
        else:
            print("Specified submissions have finished.")

    def cancel(self, hard=False):
        """Cancel any running jobscripts."""
        for sub in self.submissions:
            sub.cancel()

    def add_submission(self, JS_parallelism: Optional[bool] = None) -> app.Submission:
        with self._store.cached_load():
            with self.batch_update():
                return self._add_submission(JS_parallelism)

    def _add_submission(self, JS_parallelism: Optional[bool] = None) -> app.Submission:
        new_idx = self.num_submissions
        _ = self.submissions  # TODO: just to ensure `submissions` is loaded
        sub_obj = self.app.Submission(
            index=new_idx,
            workflow=self,
            jobscripts=self.resolve_jobscripts(),
            JS_parallelism=JS_parallelism,
        )
        all_EAR_ID = [i for js in sub_obj.jobscripts for i in js.EAR_ID.flatten()]
        # EAR_indices = sub_obj.prepare_EAR_submission_idx_update()
        if not all_EAR_ID:
            print(
                f"There are no pending element action runs, so a new submission was not "
                f"added."
            )
            return

        with self._store.cached_load():
            with self.batch_update():
                for i in all_EAR_ID:
                    self._store.set_EAR_submission_index(EAR_ID=i, sub_idx=new_idx)

        # self.set_EAR_submission_indices(sub_idx=new_idx, EAR_indices=EAR_indices)

        sub_obj_js, _ = sub_obj.to_json_like()
        self._submissions.append(sub_obj)
        self._pending["submissions"].append(new_idx)
        with self._store.cached_load():
            with self.batch_update():
                self._store.add_submission(new_idx, sub_obj_js)

        return self.submissions[new_idx]

    def resolve_jobscripts(self) -> List[app.Jobscript]:
        js, element_deps = self._resolve_singular_jobscripts()
        js_deps = resolve_jobscript_dependencies(js, element_deps)

        for js_idx in js:
            if js_idx in js_deps:
                js[js_idx]["dependencies"] = js_deps[js_idx]

        js = merge_jobscripts_across_tasks(js)
        js = jobscripts_to_list(js)
        js_objs = [self.app.Jobscript(**i) for i in js]

        return js_objs

    def _resolve_singular_jobscripts(self) -> Tuple[Dict[int, Dict], Dict]:
        """
        We arrange EARs into `EARs` and `elements` so we can quickly look up membership
        by EAR idx in the `EARs` dict.

        Returns
        -------
        submission_jobscripts
        all_element_deps
            For a given jobscript index, for a given jobscript element index within that
            jobscript, this is a list of EAR IDs dependencies of that element.

        """

        submission_jobscripts = {}
        all_element_deps = {}

        for task_iID, loop_idx_i in self.get_iteration_task_pathway():
            task = self.tasks.get(insert_ID=task_iID)
            res, res_hash, res_map, EAR_map = generate_EAR_resource_map(task, loop_idx_i)
            jobscripts, _ = group_resource_map_into_jobscripts(res_map)

            for js_dat in jobscripts:
                # (insert ID, action_idx, index into task_loop_idx):
                task_actions = [
                    [task.insert_ID, i, 0]
                    for i in sorted(
                        set(
                            act_idx_i
                            for act_idx in js_dat["elements"].values()
                            for act_idx_i in act_idx
                        )
                    )
                ]
                task_elements = {task.insert_ID: list(js_dat["elements"].keys())}
                EAR_idx_arr_shape = (
                    len(task_actions),
                    len(task_elements[task.insert_ID]),
                )
                EAR_ID_arr = np.empty(EAR_idx_arr_shape, dtype=np.int32)
                EAR_ID_arr[:] = -1

                new_js_idx = len(submission_jobscripts)

                js_i = {
                    "task_insert_IDs": [task.insert_ID],
                    "task_loop_idx": [loop_idx_i],
                    "task_actions": task_actions,  # map jobscript actions to task actions
                    "task_elements": task_elements,  # map jobscript elements to task elements
                    # "EARs": {},  # keys are (task insert ID, elem_idx, EAR_idx)
                    "EAR_ID": EAR_ID_arr,
                    "resources": res[js_dat["resources"]],
                    "resource_hash": res_hash[js_dat["resources"]],
                    "dependencies": {},
                }
                for elem_idx, act_indices in js_dat["elements"].items():
                    js_elem_idx = task_elements[task.insert_ID].index((elem_idx))
                    all_EAR_IDs = []
                    for act_idx in act_indices:
                        EAR_ID_i = EAR_map[act_idx, elem_idx].item()
                        # EAR_idx, run_idx, iter_idx = (
                        #     i.item() for i in EAR_map[act_idx, elem_idx]
                        # )
                        # construct EAR_ID object so we can retrieve the EAR objects and
                        # so their dependencies:
                        # EAR_id = EAR_ID(
                        #     task_insert_ID=task.insert_ID,
                        #     element_idx=elem_idx,
                        #     iteration_idx=iter_idx,
                        #     action_idx=act_idx,
                        #     run_idx=run_idx,
                        #     EAR_idx=EAR_idx,
                        # )
                        all_EAR_IDs.append(EAR_ID_i)
                        # js_i["EARs"][(task.insert_ID, elem_idx, EAR_idx)] = (
                        #     iter_idx,
                        #     act_idx,
                        #     run_idx,
                        # )

                        js_act_idx = task_actions.index([task.insert_ID, act_idx, 0])
                        # js_i["EAR_idx"][js_act_idx][js_elem_idx] = EAR_idx
                        js_i["EAR_ID"][js_act_idx][js_elem_idx] = EAR_ID_i

                    # get indices of EARs that this element depends on:
                    EAR_objs = self.get_EARs_from_IDs(all_EAR_IDs)
                    EAR_deps = [i.get_EAR_dependencies() for i in EAR_objs]
                    EAR_deps_flat = [j for i in EAR_deps for j in i]

                    # print(f"{EAR_deps=}")
                    # print(f"{EAR_deps_flat=}")

                    # represent EAR dependencies of this jobscripts using the same key
                    # format as in the "EARs" dict, to allow for quick lookup when
                    # resolving dependencies between jobscripts; also, no need to include
                    # EAR dependencies that are in this jobscript:
                    # EAR_deps_EAR_idx = [
                    #     (i.task_insert_ID, i.element_idx, i.EAR_idx)
                    #     for i in EAR_deps_flat
                    #     if (i.task_insert_ID, i.element_idx, i.EAR_idx)
                    #     not in js_i["EARs"]
                    # ]
                    EAR_deps_EAR_idx = [
                        i for i in EAR_deps_flat if i not in js_i["EAR_ID"]
                    ]
                    if EAR_deps_EAR_idx:
                        if new_js_idx not in all_element_deps:
                            all_element_deps[new_js_idx] = {}

                        all_element_deps[new_js_idx][js_elem_idx] = EAR_deps_EAR_idx

                submission_jobscripts[new_js_idx] = js_i

        return submission_jobscripts, all_element_deps

    def write_commands(
        self,
        submission_idx: int,
        jobscript_idx: int,
        JS_action_idx: int,
        EAR_ID: int,
    ) -> None:
        """Write run-time commands for a given EAR."""
        with self._store.cached_load():
            jobscript = self.submissions[submission_idx].jobscripts[jobscript_idx]
            EAR = self.get_EARs_from_IDs([EAR_ID])[0]
            commands, shell_vars = EAR.compose_commands(jobscript)
            for param_name, shell_var_name in shell_vars:
                commands += jobscript.shell.format_save_parameter(
                    workflow_app_alias=jobscript.workflow_app_alias,
                    param_name=param_name,
                    shell_var_name=shell_var_name,
                    EAR_ID=EAR_ID,
                )
            commands = jobscript.shell.wrap_in_subshell(commands, EAR.action.abortable)
            cmd_file_name = jobscript.get_commands_file_name(JS_action_idx)
            with Path(cmd_file_name).open("wt", newline="\n") as fp:
                # (assuming we have CD'd correctly to the element run directory)
                fp.write(commands)

    def save_parameter(
        self,
        name: str,
        value: Any,
        EAR_ID: int,
    ):
        with self._store.cached_load():
            with self.batch_update():
                EAR = self.get_EARs_from_IDs([EAR_ID])[0]
                param_id = EAR.data_idx[name]
                self.set_parameter_value(param_id, value)

    def show_all_EAR_statuses(self):
        print(
            f"{'task':8s} {'element':8s} {'iteration':8s} {'action':8s} "
            f"{'run':8s} {'sub.':8s} {'exitcode':8s} {'success':8s} {'skip':8s}"
        )
        for task in self.tasks:
            for element in task.elements[:]:
                for iter_idx, iteration in enumerate(element.iterations):
                    for act_idx, action_runs in iteration.actions.items():
                        for run_idx, EAR in enumerate(action_runs.runs):
                            suc = EAR.success if EAR.success is not None else "-"
                            if EAR.exit_code is not None:
                                exc = f"{EAR.exit_code:^8d}"
                            else:
                                exc = f"{'-':^8}"
                            print(
                                f"{task.insert_ID:^8d} {element.index:^8d} "
                                f"{iter_idx:^8d} {act_idx:^8d} {run_idx:^8d} "
                                f"{EAR.status.name.lower():^8s}"
                                f"{exc}"
                                f"{suc:^8}"
                                f"{EAR.skip:^8}"
                            )


@dataclass
class WorkflowBlueprint:
    """Pre-built workflow templates that are simpler to parametrise (e.g. fitting workflows)."""

    workflow_template: WorkflowTemplate
