from __future__ import annotations
from contextlib import contextmanager
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, Union
from warnings import warn

import numpy as np

from hpcflow.sdk.core.actions import EAR_ID
from hpcflow.sdk.submission.jobscript import (
    generate_EAR_resource_map,
    group_resource_map_into_jobscripts,
    jobscripts_to_list,
    merge_jobscripts_across_tasks,
    resolve_jobscript_dependencies,
)
from hpcflow.sdk.typing import PathLike

from .json_like import ChildObjectSpec, JSONLike
from .parameters import InputSource
from .task import ElementSet, Task
from .utils import (
    read_JSON_file,
    read_JSON_string,
    read_YAML,
    read_YAML_file,
    replace_items,
)
from .errors import (
    InvalidInputSourceTaskReference,
    LoopAlreadyExistsError,
    SubmissionFailure,
    WorkflowBatchUpdateFailedError,
    WorkflowNotFoundError,
    WorkflowSubmissionFailure,
)
from hpcflow.sdk.persistence import (
    store_cls_from_path,
    store_cls_from_str,
    temporary_workflow_rename,
    DEFAULT_STORE_FORMAT,
)

DEFAULT_TEMPLATE_FORMAT = "yaml"
ALL_TEMPLATE_FORMATS = ("yaml", "json")


class _DummyPersistentWorkflow:
    """An object to pass to ResourceSpec.make_persistent that pretends to be a
    Workflow object, so we can pretend to make template-level inputs/resources
    persistent before the workflow exists."""

    def __init__(self):
        self._parameters = []
        self._sources = []

    def _add_parameter_data(self, data, source: Dict) -> int:
        self._parameters.append(data)
        self._sources.append(source)
        data_ref = len(self._parameters) - 1
        return data_ref

    def make_persistent(self, workflow: Workflow):
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
    tasks: Optional[List[Task]] = field(default_factory=lambda: [])
    loops: Optional[List[Loop]] = field(default_factory=lambda: [])
    workflow: Optional[Workflow] = None
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
    def _from_data(cls, data: Dict) -> WorkflowTemplate:
        # use element_sets if not already:
        for task_idx, task_dat in enumerate(data["tasks"]):
            if "element_sets" not in task_dat:
                # add a single element set:
                elem_set = {}
                for chd_obj in ElementSet._child_objects:
                    if chd_obj.name in task_dat:
                        elem_set[chd_obj.name] = task_dat.pop(chd_obj.name)
                data["tasks"][task_idx]["element_sets"] = [elem_set]

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
    def from_YAML_string(cls, string: str) -> WorkflowTemplate:
        """Load from a YAML string.

        Parameters
        ----------
        string
            The YAML string containing the workflow template parametrisation.

        """
        return cls._from_data(read_YAML(string))

    @classmethod
    def from_YAML_file(cls, path: PathLike) -> WorkflowTemplate:
        """Load from a YAML file.

        Parameters
        ----------
        path
            The path to the YAML file containing the workflow template parametrisation.

        """
        return cls._from_data(read_YAML_file(path))

    @classmethod
    def from_JSON_string(cls, string: str) -> WorkflowTemplate:
        """Load from a JSON string.

        Parameters
        ----------
        string
            The JSON string containing the workflow template parametrisation.

        """
        return cls._from_data(read_JSON_string(string))

    @classmethod
    def from_JSON_file(cls, path: PathLike) -> WorkflowTemplate:
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
    ) -> WorkflowTemplate:
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

    def _add_empty_task(self, task: Task, new_index: int, insert_ID: int) -> None:
        """Called by `Workflow._add_empty_task`."""
        new_task_name = self.workflow._get_new_task_unique_name(task, new_index)

        task._insert_ID = insert_ID
        task._dir_name = f"task_{task.insert_ID}_{new_task_name}"
        task._element_sets = []  # element sets are added to the Task during add_elements

        task.workflow_template = self
        self.tasks.insert(new_index, task)

    def _add_empty_loop(self, loop: Loop) -> None:
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


class Workflow:
    """Class to represent a persistent {app_name} workflow."""

    _app_attr = "app"

    _default_ts_fmt = r"%Y-%m-%d %H:%M:%S.%f"
    _default_ts_name_fmt = r"%Y-%m-%d_%H%M%S"

    def __init__(self, path: PathLike) -> None:

        self.path = Path(path).resolve()
        if not self.path.is_dir():
            raise WorkflowNotFoundError(f"No workflow found at path: {self.path}")

        # assigned on first access to corresponding properties:
        self._ts_fmt = None
        self._ts_name_fmt = None
        self._creation_info = None

        self._template = None
        self._template_components = None
        self._tasks = None
        self._loops = None
        self._submissions = None

        self._store = store_cls_from_path(self.path)(self)

        self._in_batch_mode = False  # flag to track when processing batch updates

        # store indices of updates during batch update, so we can revert on failure:
        self._pending = self._get_empty_pending()

    @property
    def name(self):
        """The workflow name may be different from the template name, as it includes the
        creation date-timestamp if generated."""
        return self.path.parts[-1]

    def _get_empty_pending(self) -> Dict:
        return {
            "template_components": {k: [] for k in self.app._template_component_types},
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
    def store_format(self):
        return self._store.store_name

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
    ) -> Workflow:
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
    ) -> Workflow:
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
    ) -> Workflow:
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
    ) -> Workflow:
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
    ) -> Workflow:
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
    ) -> Workflow:
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
        tasks: Optional[List[Task]] = field(default_factory=lambda: []),
        loops: Optional[List[Loop]] = field(default_factory=lambda: []),
        resources: Optional[Dict[str, Dict]] = None,
        path: Optional[PathLike] = None,
        workflow_name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> Workflow:
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
            tasks=tasks,
            loops=loops,
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

    @contextmanager
    def batch_update(self, is_workflow_creation: bool = False) -> Iterator[None]:
        """A context manager that batches up structural changes to the workflow and
        commits them to disk all together when the context manager exits."""

        if self._in_batch_mode:
            yield
        else:
            try:
                self._in_batch_mode = True
                yield

            except Exception as err:

                print("batch update exception!")

                self._in_batch_mode = False
                self._store.reject_pending()

                for task in self.tasks:
                    task._reset_pending_elements()

                for loop in self.loops:
                    loop._reset_pending_num_added_iters()

                self._reject_pending()

                if is_workflow_creation:
                    # creation failed, so no need to keep the newly generated workflow:
                    self._store.delete_no_confirm()
                    self._store.reinstate_replaced_dir()

                raise err

            else:

                if self._store.has_pending:

                    is_diff = self._store.is_modified_on_disk()
                    if is_diff:
                        raise WorkflowBatchUpdateFailedError(
                            f"Workflow modified on disk since it was loaded!"
                        )

                    for task in self.tasks:
                        task._accept_pending_elements()

                    for loop in self.loops:
                        loop._accept_pending_num_added_iters()

                    self._store.remove_replaced_dir()
                    # TODO: handle errors in commit pending?
                    self._store.commit_pending()
                    self._accept_pending()
                    self._in_batch_mode = False

    @classmethod
    def _write_empty_workflow(
        cls,
        template: WorkflowTemplate,
        path: Optional[PathLike] = None,
        name: Optional[str] = None,
        overwrite: Optional[bool] = False,
        store: Optional[str] = DEFAULT_STORE_FORMAT,
        ts_fmt: Optional[str] = None,
        ts_name_fmt: Optional[str] = None,
    ) -> Workflow:

        """
        Parameters
        ----------
        path
            The directory in which the workflow will be generated. The current directory
            if not specified.
        name
            The name of the workflow. If specified, the workflow directory will be `path`
            joined with `name`. If not specified the WorkflowTemplate name will be used,
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

        ts = datetime.now()

        # store all times in UTC, since Numpy doesn't support time zone info:
        ts_utc = ts.astimezone(tz=timezone.utc)

        ts_name_fmt = ts_name_fmt or cls._default_ts_name_fmt
        ts_fmt = ts_fmt or cls._default_ts_fmt

        path = Path(path or "").resolve()
        name = name or f"{template.name}_{ts.strftime(ts_name_fmt)}"
        workflow_path = path.joinpath(name)

        replaced_dir = None
        if workflow_path.exists():
            if overwrite:
                replaced_dir = temporary_workflow_rename(workflow_path)
            else:
                raise ValueError(f"Path already exists: {workflow_path}.")

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
            "ts_fmt": ts_fmt,
            "ts_name_fmt": ts_name_fmt,
        }

        store_cls = store_cls_from_str(store)
        store_cls.write_empty_workflow(
            template_js=template_js,
            template_components_js=template_sh,
            workflow_path=workflow_path,
            replaced_dir=replaced_dir,
            creation_info=creation_info,
        )
        wk = cls(workflow_path)

        # actually make template inputs/resources persistent, now the workflow exists:
        wk_dummy.make_persistent(wk)

        return wk

    @property
    def ts_fmt(self):
        if not self._ts_fmt:
            self._ts_fmt = self._store.get_creation_info()["ts_fmt"]
        return self._ts_fmt

    @property
    def ts_name_fmt(self):
        if not self._ts_name_fmt:
            self._ts_name_fmt = self._store.get_creation_info()["ts_name_fmt"]
        return self._ts_name_fmt

    @property
    def creation_info(self):
        if not self._creation_info:
            with self._store.cached_load():
                info = self._store.get_creation_info()
                info["create_time"] = datetime.strptime(
                    info["create_time"], info["ts_fmt"]
                ).replace(tzinfo=timezone.utc)
                self._creation_info = info
        return self._creation_info

    @property
    def num_tasks(self) -> int:
        return len(self.tasks)

    @property
    def num_added_tasks(self) -> int:
        with self._store.cached_load():
            return self._store.get_num_added_tasks()

    @property
    def num_elements(self) -> int:
        return sum(task.num_elements for task in self.tasks)

    @property
    def num_element_iterations(self) -> int:
        return sum(task.num_element_iterations for task in self.tasks)

    @property
    def num_loops(self) -> int:
        return len(self.loops)

    @property
    def num_submissions(self) -> int:
        return len(self.submissions)

    @property
    def template_components(self) -> Dict:
        if self._template_components is None:
            with self._store.cached_load():
                tc_js = self._store.get_template_components()
            self._template_components = self.app.template_components_from_json_like(tc_js)
        return self._template_components

    @property
    def template(self) -> WorkflowTemplate:
        if self._template is None:
            with self._store.cached_load():
                temp_js = self._store.get_template()
                template = self.app.WorkflowTemplate.from_json_like(
                    temp_js, self.template_components
                )
                template.workflow = self
            self._template = template

        return self._template

    @property
    def tasks(self) -> WorkflowTaskList:
        if self._tasks is None:
            with self._store.cached_load():
                tasks_meta = self._store.get_all_tasks_metadata()
                wk_tasks = []
                for idx, i in enumerate(tasks_meta):
                    wk_task = self.app.WorkflowTask(
                        workflow=self,
                        template=self.template.tasks[idx],
                        index=idx,
                        num_elements=i["num_elements"],
                        num_element_iterations=i["num_element_iterations"],
                        num_EARs=i["num_EARs"],
                    )
                    wk_tasks.append(wk_task)
                self._tasks = self.app.WorkflowTaskList(wk_tasks)
        return self._tasks

    @property
    def loops(self) -> WorkflowLoopList:
        if self._loops is None:
            with self._store.cached_load():
                wk_loops = []
                for idx, loop_dat in enumerate(self._store.get_loops()):
                    wk_loop = self.app.WorkflowLoop(
                        index=idx,
                        workflow=self,
                        template=self.template.loops[idx],
                        **loop_dat,
                    )
                    wk_loops.append(wk_loop)
                self._loops = self.app.WorkflowLoopList(wk_loops)
        return self._loops

    @property
    def submissions(self) -> List[Submission]:
        if self._submissions is None:
            with self._store.cached_load():
                subs = []
                for idx, sub_dat in enumerate(self._store.get_submissions()):
                    sub_js = {"index": idx, "workflow": self, **sub_dat}
                    sub = self.app.Submission.from_json_like(sub_js)
                    subs.append(sub)
                self._submissions = subs
        return self._submissions

    @property
    def artifacts_path(self):
        # TODO: allow customisation of artifacts path at submission and resources level
        return self.path / "artifacts"

    @property
    def submissions_path(self):
        return self.artifacts_path / "submissions"

    @property
    def task_artifacts_path(self):
        return self.artifacts_path / "tasks"

    def elements(self) -> Iterator[Element]:
        for task in self.tasks:
            for element in task.elements:
                yield element

    def copy(self, path=None) -> Workflow:
        """Copy the workflow to a new path and return the copied workflow."""
        if path is None:
            path = self.path.parent / Path(self.path.stem + "_copy" + self.path.suffix)
        if path.exists():
            raise ValueError(f"Path already exists: {path}.")
        self._store.copy(path=path)
        return self.app.Workflow(path=path)

    def delete(self):
        self._store.delete()

    def _delete_no_confirm(self):
        self._store.delete_no_confirm()

    def rename(self, new_name: str):
        raise NotImplementedError

    def _submit(
        self,
        ignore_errors: Optional[bool] = False,
        JS_parallelism: Optional[bool] = None,
    ):
        """Submit outstanding EARs for execution."""

        # generate a new submission if there are no pending submissions:
        pending = [i for i in self.submissions if i.needs_submit]
        if not pending:
            new_sub = self.add_submission(JS_parallelism=JS_parallelism)
            if not new_sub:
                raise ValueError("No pending element action runs to submit!")
            pending = [new_sub]

        self.submissions_path.mkdir(exist_ok=True, parents=True)
        self.task_artifacts_path.mkdir(exist_ok=True, parents=True)

        # for direct execution the submission must be persistent at submit-time, because
        # it will be read by a new instance of the app:
        self._store.commit_pending()

        # submit all pending submissions:
        exceptions = []
        submitted_js = {}
        for sub in pending:
            try:
                sub_js_idx = sub.submit(
                    self.task_artifacts_path,
                    ignore_errors=ignore_errors,
                )
                submitted_js[sub.index] = sub_js_idx
            except SubmissionFailure as exc:
                exceptions.append(exc)

        return exceptions, submitted_js

    def submit(
        self,
        ignore_errors: Optional[bool] = False,
        JS_parallelism: Optional[bool] = None,
    ) -> None:
        with self._store.cached_load():
            with self.batch_update():
                # commit updates before raising exception:
                exceptions, submitted_js = self._submit(
                    ignore_errors=ignore_errors,
                    JS_parallelism=JS_parallelism,
                )

        if exceptions:
            msg = "\n" + "\n\n".join([i.message for i in exceptions])
            raise WorkflowSubmissionFailure(msg)

        return submitted_js

    def add_submission(self, JS_parallelism: Optional[bool] = None) -> Submission:
        new_idx = self.num_submissions
        sub_obj = self.app.Submission(
            index=new_idx,
            workflow=self,
            jobscripts=self.resolve_jobscripts(),
            JS_parallelism=JS_parallelism,
        )
        EAR_indices = sub_obj.prepare_EAR_submission_idx_update()
        if not EAR_indices:
            print(
                f"There are no pending element action runs, so a new submission was not "
                f"added."
            )
            return

        self.set_EAR_submission_indices(sub_idx=new_idx, EAR_indices=EAR_indices)
        sub_obj_js, _ = sub_obj.to_json_like()
        self._submissions.append(sub_obj)
        self._pending["submissions"].append(new_idx)
        with self._store.cached_load():
            with self.batch_update():
                self._store.add_submission(sub_obj_js)

        return self.submissions[new_idx]

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
        names = Task.get_task_unique_names(self.template.tasks)
        if map_to_insert_ID:
            insert_IDs = (i.insert_ID for i in self.template.tasks)
            return dict(zip(names, insert_IDs))
        else:
            return names

    def _get_new_task_unique_name(self, new_task: Task, new_index: int) -> str:

        task_templates = list(self.template.tasks)
        task_templates.insert(new_index, new_task)
        uniq_names = Task.get_task_unique_names(task_templates)

        return uniq_names[new_index]

    def _add_empty_task(
        self,
        task: Task,
        new_index: Optional[int] = None,
    ) -> WorkflowTask:

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
        self._store.add_empty_task(new_index, task_js)

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

    def _add_empty_loop(self, loop: Loop) -> WorkflowLoop:
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

        # update persistent store:
        loop_js, _ = loop_c.to_json_like()
        task_indices = [self.tasks.get(insert_ID=i).index for i in loop_c.task_insert_IDs]
        self._store.add_loop(
            task_indices=task_indices,
            loop_js=loop_js,
            iterable_parameters=wk_loop.iterable_parameters,
        )

        self._pending["loops"].append(new_index)

        return wk_loop

    def _add_loop(self, loop: Loop, parent_loop_indices: Dict = None) -> None:
        new_wk_loop = self._add_empty_loop(loop)
        if loop.num_iterations is not None:
            # fixed number of iterations, so add remaining N > 0 iterations:
            for _ in range(loop.num_iterations - 1):
                new_wk_loop.add_iteration(parent_loop_indices=parent_loop_indices)

    def add_loop(self, loop: Loop, parent_loop_indices: Dict = None) -> None:
        """Add a loop to a subset of workflow tasks."""
        with self._store.cached_load():
            with self.batch_update():
                self._add_loop(loop, parent_loop_indices)

    def _add_task(self, task: Task, new_index: Optional[int] = None) -> None:
        new_wk_task = self._add_empty_task(task=task, new_index=new_index)
        new_wk_task._add_elements(element_sets=task.element_sets)

    def add_task(self, task: Task, new_index: Optional[int] = None) -> None:
        with self._store.cached_load():
            with self.batch_update():
                self._add_task(task, new_index=new_index)

    def add_task_after(self, new_task: Task, task_ref=None):
        """Adds the given new_task after the task specified in task_ref.

        Parameters
        ----------
        new_task : Task, required.
        task_ref : Task, optional.
            If no `task_ref` is given, the new task will be added at the end.

        """
        new_index = task_ref.index + 1 if task_ref else None
        self.add_task(new_task, new_index)
        # TODO: add new downstream elements?

    def add_task_before(self, new_task: Task, task_ref=None):
        """Adds the given new_task before the task specified in task_ref.

        Parameters
        ----------
        new_task : Task, required.
        task_ref : Task, optional.
            If no task_ref is given, the new task will be added at the beginning.

        """
        new_index = task_ref.index if task_ref else 0
        self.add_task(new_task, new_index)
        # TODO: add new downstream elements?

    def get_parameter_data(self, index: int) -> Tuple[bool, Any]:
        return self._store.get_parameter_data(index)

    def get_parameter_source(self, index: int) -> Dict:
        return self._store.get_parameter_source(index)

    def get_all_parameter_data(self) -> Dict[int, Any]:
        return self._store.get_all_parameter_data()

    def is_parameter_set(self, index: int) -> bool:
        return self._store.is_parameter_set(index)

    def check_parameters_exist(
        self, indices: Union[int, List[int]]
    ) -> Union[bool, List[bool]]:
        return self._store.check_parameters_exist(indices)

    def _add_unset_parameter_data(self, source: Dict) -> int:
        return self._store.add_unset_parameter_data(source)

    def _add_parameter_data(self, data, source: Dict) -> int:
        return self._store.add_parameter_data(data, source)

    def _resolve_input_source_task_reference(
        self, input_source: InputSource, new_task_name: str
    ) -> None:
        """Normalise the input source task reference and convert a source to a local type
        if required."""

        # TODO: test thoroughly!

        if isinstance(input_source.task_ref, str):
            if input_source.task_ref == new_task_name:
                if input_source.task_source_type is self.app.TaskSourceType.OUTPUT:
                    raise InvalidInputSourceTaskReference(
                        f"Input source {input_source.to_string()!r} cannot refer to the "
                        f"outputs of its own task!"
                    )
                else:
                    warn(
                        f"Changing input source {input_source.to_string()!r} to a local "
                        f"type, since the input source task reference refers to its own "
                        f"task."
                    )
                    # TODO: add an InputSource source_type setter to reset
                    # task_ref/source_type?
                    input_source.source_type = self.app.InputSourceType.LOCAL
                    input_source.task_ref = None
                    input_source.task_source_type = None
            else:
                try:
                    uniq_names_cur = self.get_task_unique_names(map_to_insert_ID=True)
                    input_source.task_ref = uniq_names_cur[input_source.task_ref]
                except KeyError:
                    raise InvalidInputSourceTaskReference(
                        f"Input source {input_source.to_string()!r} refers to a missing "
                        f"or inaccessible task: {input_source.task_ref!r}."
                    )

    def get_task_elements(self, task: Task, selection: slice) -> List[Element]:
        return [
            self.app.Element(task=task, **i)
            for i in self._store.get_task_elements(task.index, task.insert_ID, selection)
        ]

    def get_task_elements_islice(self, task: Task, selection: slice) -> Iterator[Element]:
        for i in self._store.get_task_elements_islice(
            task.index, task.insert_ID, selection
        ):
            yield self.app.Element(task=task, **i)

    def get_EARs_from_IDs(self, indices: List[EAR_ID]) -> List[ElementActionRun]:
        """Return element action run objects from a list of five-tuples, representing the
        task insert ID, element index, iteration index, action index, and run index,
        respectively.
        """
        objs = []
        for _EAR_ID in indices:
            task = self.tasks.get(insert_ID=_EAR_ID.task_insert_ID)
            elem_iters = task.elements[_EAR_ID.element_idx].iterations
            for i in elem_iters:
                if i.index == _EAR_ID.iteration_idx:
                    iter_i = i
                    break
            EAR_i = iter_i.actions[_EAR_ID.action_idx].runs[_EAR_ID.run_idx]
            objs.append(EAR_i)
        return objs

    def get_element_iterations_from_IDs(
        self, indices: List[IterationID]
    ) -> List[ElementIteration]:
        """Return element iteration objects from a list of three-tuples, representing the
        task insert ID, element index, and iteration index, respectively.
        """
        objs = []
        for iter_idx in indices:
            iter_i = (
                self.tasks.get(insert_ID=iter_idx.task_insert_ID)
                .elements[iter_idx.element_idx]
                .iterations[iter_idx.iteration_idx]
            )
            objs.append(iter_i)
        return objs

    def get_elements_from_IDs(self, indices: List[ElementID]) -> List[Element]:
        """Return element objects from a list of two-tuples, representing the task insert
        ID, and element index, respectively."""
        return [
            self.tasks.get(insert_ID=idx.task_insert_ID).elements[idx.element_idx]
            for idx in indices
        ]

    def set_EAR_submission_indices(
        self,
        sub_idx: int,
        EAR_indices: Tuple[int, int, int, int],
    ) -> None:
        """Set the submission index on an EAR."""
        with self._store.cached_load():
            with self.batch_update():
                self._store.set_EAR_submission_indices(sub_idx, EAR_indices)

    def set_EAR_start(
        self,
        submission_idx: int,
        jobscript_idx: int,
        JS_element_idx: int,
        JS_action_idx: int,
    ) -> None:
        """Set the start time on an EAR."""
        with self._store.cached_load():
            with self.batch_update():
                jobscript = self.submissions[submission_idx].jobscripts[jobscript_idx]
                (t_iD, _, i_idx, a_idx, r_idx, _) = jobscript.get_EAR_ID_array()[
                    JS_action_idx, JS_element_idx
                ].item()
                self._store.set_EAR_start(t_iD, i_idx, a_idx, r_idx)

    def set_EAR_end(
        self,
        submission_idx: int,
        jobscript_idx: int,
        JS_element_idx: int,
        JS_action_idx: int,
    ) -> None:
        """Set the end time on an EAR."""
        with self._store.cached_load():
            with self.batch_update():
                jobscript = self.submissions[submission_idx].jobscripts[jobscript_idx]
                (t_iD, _, i_idx, a_idx, r_idx, _) = jobscript.get_EAR_ID_array()[
                    JS_action_idx, JS_element_idx
                ].item()
                self._store.set_EAR_end(t_iD, i_idx, a_idx, r_idx)

    def _from_internal_get_EAR(
        self,
        submission_idx: int,
        jobscript_idx: int,
        JS_element_idx: int,
        JS_action_idx: int,
    ):
        with self._store.cached_load():
            jobscript = self.submissions[submission_idx].jobscripts[jobscript_idx]
            id_args = jobscript.get_EAR_ID_array()[JS_action_idx, JS_element_idx].item()
            EAR_id = EAR_ID(*id_args)
            EAR = self.get_EARs_from_IDs([EAR_id])[0]

        return jobscript, EAR

    def write_commands(
        self,
        submission_idx: int,
        jobscript_idx: int,
        JS_element_idx: int,
        JS_action_idx: int,
    ) -> None:
        """Write run-time commands for a given EAR."""
        with self._store.cached_load():
            jobscript, EAR = self._from_internal_get_EAR(
                submission_idx, jobscript_idx, JS_element_idx, JS_action_idx
            )
            commands, shell_vars = EAR.compose_commands(jobscript)
            for param_name, shell_var_name in shell_vars:
                commands += jobscript.shell.format_save_parameter(
                    workflow_app_alias=jobscript.workflow_app_alias,
                    param_name=param_name,
                    shell_var_name=shell_var_name,
                )
            with Path(jobscript.commands_file_name).open("wt", newline="\n") as fp:
                # (assuming we have CD'd correctly to the element run directory)
                fp.write(commands)

    def save_parameter(
        self,
        name,
        value,
        submission_idx: int,
        jobscript_idx: int,
        JS_element_idx: int,
        JS_action_idx: int,
    ):
        with self._store.cached_load():
            with self.batch_update():
                _, EAR = self._from_internal_get_EAR(
                    submission_idx, jobscript_idx, JS_element_idx, JS_action_idx
                )
                data_idx = EAR.data_idx[name]
                self._store.set_parameter(data_idx, value)

    def resolve_jobscripts(self) -> List[Jobscript]:

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
                EAR_idx_arr = np.empty(EAR_idx_arr_shape, dtype=np.int32)
                EAR_idx_arr[:] = -1

                new_js_idx = len(submission_jobscripts)

                js_i = {
                    "task_insert_IDs": [task.insert_ID],
                    "task_loop_idx": [loop_idx_i],
                    "task_actions": task_actions,  # map jobscript actions to task actions
                    "task_elements": task_elements,  # map jobscript elements to task elements
                    "EARs": {},  # keys are (task insert ID, elem_idx, EAR_idx)
                    "EAR_idx": EAR_idx_arr,
                    "resources": res[js_dat["resources"]],
                    "resource_hash": res_hash[js_dat["resources"]],
                    "dependencies": {},
                }
                for elem_idx, act_indices in js_dat["elements"].items():

                    js_elem_idx = task_elements[task.insert_ID].index((elem_idx))
                    all_EAR_IDs = []
                    for act_idx in act_indices:
                        EAR_idx, run_idx, iter_idx = (
                            i.item() for i in EAR_map[act_idx, elem_idx]
                        )
                        # construct EAR_ID object so we can retrieve the EAR objects and
                        # so their dependencies:
                        EAR_id = EAR_ID(
                            task_insert_ID=task.insert_ID,
                            element_idx=elem_idx,
                            iteration_idx=iter_idx,
                            action_idx=act_idx,
                            run_idx=run_idx,
                            EAR_idx=EAR_idx,
                        )
                        all_EAR_IDs.append(EAR_id)
                        js_i["EARs"][(task.insert_ID, elem_idx, EAR_idx)] = (
                            iter_idx,
                            act_idx,
                            run_idx,
                        )

                        js_act_idx = task_actions.index([task.insert_ID, act_idx, 0])
                        js_i["EAR_idx"][js_act_idx][js_elem_idx] = EAR_idx

                    # get indices of EARs that this element depends on:
                    EAR_objs = self.get_EARs_from_IDs(all_EAR_IDs)
                    EAR_deps = [i.get_EAR_dependencies() for i in EAR_objs]
                    EAR_deps_flat = [j for i in EAR_deps for j in i]

                    # represent EAR dependencies of this jobscripts using the same key
                    # format as in the "EARs" dict, to allow for quick lookup when
                    # resolving dependencies between jobscripts; also, no need to include
                    # EAR dependencies that are in this jobscript:
                    EAR_deps_EAR_idx = [
                        (i.task_insert_ID, i.element_idx, i.EAR_idx)
                        for i in EAR_deps_flat
                        if (i.task_insert_ID, i.element_idx, i.EAR_idx)
                        not in js_i["EARs"]
                    ]
                    if EAR_deps_EAR_idx:
                        if new_js_idx not in all_element_deps:
                            all_element_deps[new_js_idx] = {}

                        all_element_deps[new_js_idx][js_elem_idx] = EAR_deps_EAR_idx

                submission_jobscripts[new_js_idx] = js_i

        return submission_jobscripts, all_element_deps

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

    def show_all_EAR_statuses(self):
        print(
            f"{'task':8s} {'element':8s} {'iteration':8s} {'action':8s} "
            f"{'run':8s} {'status':8s}"
        )
        for task in self.tasks:
            for element in task.elements:
                for iter_idx, iteration in enumerate(element.iterations):
                    for act_idx, action_runs in iteration.actions.items():
                        for run_idx, EAR in enumerate(action_runs.runs):
                            print(
                                f"{task.insert_ID:^8d} {element.index:^8d} "
                                f"{iter_idx:^8d} {act_idx:^8d} {run_idx:^8d} "
                                f"{EAR.submission_status.name.lower():^8s}"
                            )


@dataclass
class WorkflowBlueprint:
    """Pre-built workflow templates that are simpler to parametrise (e.g. fitting workflows)."""

    workflow_template: WorkflowTemplate
