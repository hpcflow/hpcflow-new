from __future__ import annotations
from contextlib import contextmanager
import copy
from dataclasses import dataclass, field
from pathlib import Path
import shutil
import time
from typing import List, Optional
from warnings import warn

import zarr
from reretry import retry

from .event_log import EventLog
from .json_like import ChildObjectSpec, JSONLike
from .zarr_io import zarr_encode
from .parameters import InputSource
from .task import ElementSet, Task
from .utils import get_md5_hash, group_by_dict_key_values, read_YAML, read_YAML_file
from .errors import (
    InvalidInputSourceTaskReference,
    WorkflowBatchUpdateFailedError,
    WorkflowNotFoundError,
    WorkflowParameterMissingError,
)

TS_FMT = r"%Y.%m.%d_%H:%M:%S.%f_%z"
TS_NAME_FMT = r"%Y-%m-%d_%H%M%S"


def dropbox_retry_fail(err):
    # TODO: this should log instead of printing!
    print("retrying...")


# TODO: maybe this is only an issue on Windows?
dropbox_permission_err_retry = retry(
    PermissionError,
    tries=10,
    delay=1,
    backoff=2,
    fail_callback=dropbox_retry_fail,
)


@dataclass
class WorkflowTemplate(JSONLike):
    """Class to represent initial parametrisation of a workflow, with limited validation
    logic."""

    _child_objects = (
        ChildObjectSpec(
            name="tasks",
            class_name="Task",
            is_multiple=True,
            parent_ref="workflow_template",
        ),
    )

    name: str
    tasks: Optional[List[Task]] = field(default_factory=lambda: [])
    workflow: Optional[Workflow] = None

    def __post_init__(self):
        self._set_parent_refs()

    @classmethod
    def _from_data(cls, data):
        cls.app._ensure_data_files()  # TODO: fix this at App

        # use element_sets if not already:
        for task_idx, task_dat in enumerate(data["tasks"]):
            if "element_sets" not in task_dat:
                # add a single element set:
                elem_set = {}
                for chd_obj in ElementSet._child_objects:
                    if chd_obj.name in task_dat:
                        elem_set[chd_obj.name] = task_dat.pop(chd_obj.name)
                data["tasks"][task_idx]["element_sets"] = [elem_set]

        return cls.from_json_like(data, shared_data=cls.app.app_data)

    @classmethod
    def from_YAML_string(cls, string):
        return cls._from_data(read_YAML(string))

    @classmethod
    def from_YAML_file(cls, path):
        return cls._from_data(read_YAML_file(path))


class Workflow:
    """Class to represent a persistent workflow."""

    _app_attr = "app"

    def __init__(self, path):
        """Load a persistent workflow from a path."""

        self.path = path

        self._set_original_metadata()
        self._metadata = copy.deepcopy(self._original_metadata)  # "working copy"

        self.event_log = EventLog(self)

        self._shared_data = None
        self._tasks = None
        self._elements = None
        self._template = None

        self._in_batch_mode = False  # flag to track when processing batch updates
        self._batch_mode_parameter_data = {}

    def _get_workflow_root_group(self, mode="r"):
        try:
            return zarr.open(self.path, mode=mode)
        except zarr.errors.PathNotFoundError:
            raise WorkflowNotFoundError(
                f"No workflow found at path: {self.path}"
            ) from None

    def _get_workflow_parameter_group(self, mode="r"):
        return self._get_workflow_root_group(mode=mode).parameter_data

    @property
    def metadata(self):
        return self._metadata

    @property
    def shared_data(self):
        if not self._shared_data:
            self._shared_data = self.app.shared_data_from_json_like(
                self.metadata["shared_data"]
            )
        return self._shared_data

    @property
    def template(self):
        if not self._template:
            self._template = self.app.WorkflowTemplate.from_json_like(
                self.metadata["template"],
                self.shared_data,
            )
            self._template.workflow = self

        return self._template

    @property
    def tasks(self):
        if self._tasks is None:
            self._tasks = self.app.WorkflowTaskList(
                [
                    self.app.WorkflowTask(
                        workflow=self, template=self.template.tasks[idx], index=idx, **i
                    )
                    for idx, i in enumerate(self.metadata["tasks"])
                ]
            )
        return self._tasks

    @property
    def num_tasks(self):
        return len(self.metadata["tasks"])

    @property
    def num_elements(self):
        return len(self.metadata["elements"])

    @property
    def elements(self):
        if not self._elements:
            self._elements = sorted(
                [
                    self.app.Element(
                        task=task,
                        data_index=self.metadata["elements"][i],
                        global_index=i,
                    )
                    for task in self.tasks
                    for i in task.element_indices
                ],
                key=lambda x: x.global_index,
            )
        return self._elements

    @property
    def task_name_repeat_idx(self):
        return self.metadata["task_name_repeat_idx"]

    @staticmethod
    @dropbox_permission_err_retry
    def _write_persistent_workflow(store, overwrite, root_attrs):
        """Write the empty workflow data to the zarr store on disk."""
        root = zarr.group(store=store, overwrite=overwrite)
        root.attrs.update(root_attrs)
        root.create_group("parameter_data")
        EventLog.generate_in(root)

    @classmethod
    def _make_empty_workflow(
        cls,
        template: WorkflowTemplate,
        path=None,
        name=None,
        overwrite=False,
    ):
        """Generate a task-less workflow from a WorkflowTemplate, in preparation for
        adding valid tasks."""

        # Write initial Zarr root group and attributes, then add tasks/elements
        # incrementally:

        cls.app._ensure_data_files()  # TODO: fix this at App

        timestamp = EventLog.get_timestamp()

        path = Path(path or "").resolve()
        name = name or f"{template.name}_{timestamp.strftime(TS_NAME_FMT)}"
        path = path.joinpath(name)

        template_js, template_sh = template.to_json_like()

        store = zarr.DirectoryStore(path)
        root_attrs = {
            "shared_data": template_sh,
            "template": template_js,
            "elements": [],
            "tasks": [],
            "task_name_repeat_idx": [],
        }
        cls._write_persistent_workflow(store, overwrite, root_attrs)

        obj = cls.load(path)
        obj.event_log.event_workflow_create(
            timestamp=timestamp,
            machine=obj.app.config.get("machine"),
        )

        return obj

    @classmethod
    def from_template(cls, template, path=None, name=None, overwrite=False):
        tasks = template.__dict__.pop("tasks") or []
        template.tasks = []
        obj = cls._make_empty_workflow(template, path, name, overwrite)
        with obj.batch_update(is_workflow_creation=True):
            for task in tasks:
                obj._add_task(task, parent_events=[0])
        return obj

    @classmethod
    def from_YAML_file(cls, YAML_path, path=None, name=None, overwrite=False):
        template = cls.app.WorkflowTemplate.from_YAML_file(YAML_path)
        return cls.from_template(template, path, name, overwrite)

    @classmethod
    def load(cls, path):
        """Alias for object initialisation."""
        return cls(path)

    def copy(self, path=None):
        """Copy the workflow to a new path and return the copied workflow."""
        path = self.path.with_suffix(".copy") if path is None else path
        if path.exists():
            raise ValueError(f"Path already exists: {path}.")
        shutil.copytree(self.path, path)
        return self.load(path)

    @dropbox_permission_err_retry
    def _delete_no_confirm(self):
        # Dropbox (on Windows, at least) seems to try to re-sync some of the workflow
        # files if it is deleted soon after creation, which is the case on a failed
        # workflow creation (e.g. missing inputs):
        while self.path.is_dir():
            shutil.rmtree(self.path)
            time.sleep(0.5)

    def delete(self):
        """Delete the persistent workflow."""
        # TODO: add confirmation
        self._delete_no_confirm()

    def _resolve_input_source_task_reference(
        self, input_source: InputSource, new_task_name: str
    ):
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
                    # TODO: add an InputSource source_type setter to reset task_ref/source_type
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

    def _clear_children(self):
        """Force re-initialisation of workflow attributes from metadata on next access."""

        self._tasks = None
        self._elements = None
        self._template = None
        self._shared_data = None

    def _discard_changed_metadata(self):
        """Discard the working-copy of the metadata and re-load the original persistent
        copy."""
        self._metadata = copy.deepcopy(self._original_metadata)

    def _set_original_metadata(self):
        self._original_metadata = self._get_workflow_root_group(mode="r").attrs.asdict()

    def _dump_metadata(self):
        self._get_workflow_root_group(mode="r+").attrs.put(self.metadata)
        self._set_original_metadata()
        self._clear_children()

    def _get_pending_add_parameter_keys(self):
        keys = []
        for grp_idx, val in self._get_parameter_items(mode="r"):
            if "is_pending_add" in val.attrs:
                keys.append(grp_idx)
        return keys

    def _accept_new_parameters(self):
        """Remove the pending flag from newly added parameters so they integrate with the
        workflow."""

        param_group = self._get_workflow_parameter_group(mode="r+")
        for key in self._get_pending_add_parameter_keys():
            # print(f"_accept_new_parameters: {grp_idx}")  # TODO: log this
            del param_group[key].attrs["is_pending_add"]

    def _save_metadata(self):
        """Make changes to the metadata attribute effective."""
        self._clear_children()
        if not self._in_batch_mode:
            self._dump_metadata()

    def _discard_new_parameters(self):
        """Remove newly added parameter data from the workflow, in the case where a batch
        update failed."""

        param_group = self._get_workflow_parameter_group(mode="r+")
        for grp_idx, val in param_group.items():
            if "is_pending_add" in val.attrs:
                # print(f"_discard_new_parameters: {grp_idx}")  # TODO: log this
                self._remove_parameter_group(param_group, grp_idx)

    def _check_is_modified_on_disk(self):
        """Check if the workflow metadata has changed on disk since we loaded it."""
        # TODO: check parameters and event log as well
        md_mem = self._original_metadata
        md_disk = self._get_workflow_root_group(mode="r").attrs.asdict()
        return get_md5_hash(md_disk) != get_md5_hash(md_mem)

    def _check_is_modified(self):
        """Check if the workflow structure as stored in memory has changed at all."""

        if self.event_log.has_unsaved_events:
            return True

        md_original = self._original_metadata
        md = self.metadata
        md_changed = get_md5_hash(md_original) != get_md5_hash(md)
        if md_changed:
            return True

        for val in self._get_parameter_values():
            if "is_pending_add" in val.attrs:
                return True

        return False

    @contextmanager
    def batch_update(self, is_workflow_creation=False):
        """A context manager that batches up structural changes to the workflow and
        executes them together when the context manager exits.

        This reduces the number of writes to disk and means we don't have to roll-back
        if an exception is raised halfway through a change.

        """

        if self._in_batch_mode:
            yield
        else:
            try:
                self._in_batch_mode = True
                yield

            except Exception as err:

                self._in_batch_mode = False
                self._discard_changed_metadata()
                self._discard_new_parameters()
                self.event_log.discard_new_events()
                if is_workflow_creation:
                    # creation failed, so no need to keep the newly generated workflow:
                    self._delete_no_confirm()
                raise err

            else:
                self._in_batch_mode = False

                if self._check_is_modified_on_disk():
                    raise WorkflowBatchUpdateFailedError(
                        "Workflow modified on disk since it was loaded!"
                    )

                elif self._check_is_modified():
                    self._dump_metadata()
                    self._accept_new_parameters()
                    self.event_log.dump_new_events()

    def get_zarr_parameter_group(self, group_idx):
        grp = self._get_workflow_parameter_group(mode="r").get(str(group_idx))
        if grp is None:
            raise WorkflowParameterMissingError(
                f"Workflow parameter with group index {group_idx} does not exist."
            )
        return grp

    @staticmethod
    def resolve_element_data_indices(multiplicities):
        """Find the index of the Zarr parameter group index list corresponding to each
        input data for all elements.

        Parameters
        ----------
        multiplicities : list of dict
            Each list item represents a sequence of values with keys:
                multiplicity: int
                nesting_order: int
                path : str

        Returns
        -------
        element_dat_idx : list of dict
            Each list item is a dict representing a single task element and whose keys are
            input data paths and whose values are indices that index the values of the
            dict returned by the `task.make_persistent` method.

        """

        # order by nesting order (so lower nesting orders will be fastest-varying):
        multi_srt = sorted(multiplicities, key=lambda x: x["nesting_order"])
        multi_srt_grp = group_by_dict_key_values(
            multi_srt, "nesting_order"
        )  # TODO: is tested?

        element_dat_idx = [{}]
        for para_sequences in multi_srt_grp:

            # check all equivalent nesting_orders have equivalent multiplicities
            all_multis = {i["multiplicity"] for i in para_sequences}
            if len(all_multis) > 1:
                raise ValueError(
                    f"All inputs with the same `nesting_order` must have the same "
                    f"multiplicity, but for paths "
                    f"{[i['path'] for i in para_sequences]} with `nesting_order` "
                    f"{para_sequences[0]['nesting_order']} found multiplicities "
                    f"{[i['multiplicity'] for i in para_sequences]}."
                )

            new_elements = []
            for val_idx in range(para_sequences[0]["multiplicity"]):
                for element in element_dat_idx:
                    new_elements.append(
                        {
                            **element,
                            **{i["path"]: val_idx for i in para_sequences},
                        }
                    )
            element_dat_idx = new_elements

        return element_dat_idx

    def _add_parameter_group(self, data, is_pending_add, is_set):

        param_dat_group = self._get_workflow_parameter_group(mode="r+")

        names = [int(i) for i in param_dat_group.keys()]
        new_idx = max(names) + 1 if names else 0
        new_name = str(new_idx)

        new_param_group = param_dat_group.create_group(name=new_name)
        zarr_encode(data, new_param_group, is_pending_add, is_set)

        return new_idx

    @dropbox_permission_err_retry
    def _remove_parameter_group(self, param_group, group_idx):
        """Since removal of a parameter group may happen soon after it is added (in the
        case a batch update goes wrong), we may run into dropbox sync issues, hence the
        decorator."""

        del param_group[str(group_idx)]

    def check_parameter_group_exists(self, group_idx):

        is_multi = True
        if not isinstance(group_idx, (list, tuple)):
            is_multi = False
            group_idx = [group_idx]

        param_group = self._get_workflow_parameter_group(mode="r")
        out = [str(i) in param_group for i in group_idx]
        if not is_multi:
            out = out[0]

        return out

    def generate_new_elements(
        self,
        input_data_indices,
        output_data_indices,
        element_data_indices,
        input_sources,
        sequence_indices,
    ):

        new_elements = []
        element_inp_sources = {}
        element_sequence_indices = {}
        for i_idx, i in enumerate(element_data_indices):
            elem_i = {k: input_data_indices[k][v] for k, v in i.items()}
            elem_i.update(
                {f"outputs.{k}": v[i_idx] for k, v in output_data_indices.items()}
            )

            # ensure sorted from smallest to largest path (so more-specific items
            # overwrite less-specific items):
            elem_i_split = {tuple(k.split(".")): v for k, v in elem_i.items()}
            elem_i_srt = dict(sorted(elem_i_split.items(), key=lambda x: len(x[0])))
            elem_i = {".".join(k): v for k, v in elem_i_srt.items()}

            new_elements.append(elem_i)

            # track input sources for each new element:
            for k, v in i.items():
                if k in input_sources:
                    if k not in element_inp_sources:
                        element_inp_sources[k] = []
                    element_inp_sources[k].append(input_sources[k][v])

            # track which sequence value indices (if any) are used for each new element:
            for k, v in i.items():
                if k in sequence_indices:
                    if k not in element_sequence_indices:
                        element_sequence_indices[k] = []
                    element_sequence_indices[k].append(sequence_indices[k][v])

        return new_elements, element_inp_sources, element_sequence_indices

    def _get_parameter_keys(self):
        return list(int(i) for i in self._get_workflow_parameter_group().keys())

    def _get_parameter_items(self, mode="r"):
        for k, v in self._get_workflow_parameter_group(mode=mode).items():
            yield (int(k), v)

    def _get_parameter_values(self):
        return self._get_workflow_parameter_group().values()

    def get_task_unique_names(self, map_to_insert_ID=False):
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

    def _get_new_task_unique_name(self, new_task, new_index):

        task_templates = list(self.template.tasks)
        task_templates.insert(new_index, new_task)
        uniq_names = Task.get_task_unique_names(task_templates)

        return uniq_names[new_index]

    def _add_empty_task(self, task: Task, parent_events, new_index=None):

        if new_index is None:
            new_index = self.num_tasks

        new_task_name = self._get_new_task_unique_name(task, new_index)

        task, new_param_groups = task.to_persistent(self)
        task_js, task_shared_data = task.to_json_like(exclude=["element_sets"])
        task_js["element_sets"] = []
        task_js["insert_ID"] = self.num_tasks
        task_js["dir_name"] = f"task_{self.num_tasks}_{new_task_name}"

        # add any missing shared data for this task template:
        added_shared_data = {}
        for shared_name, shared_data in task_shared_data.items():
            if shared_name not in self.metadata["shared_data"]:
                self.metadata["shared_data"][shared_name] = {}

            added_shared_data[shared_name] = []

            for k, v in shared_data.items():
                if k not in self.metadata["shared_data"][shared_name]:
                    self.metadata["shared_data"][shared_name][k] = v
                    added_shared_data[shared_name].append(k)

        self.event_log.event_add_empty_task(
            new_index,
            added_shared_data,
            parents=parent_events,
            new_parameter_groups=new_param_groups,
        )

        empty_task = {
            "element_indices": [],
            "element_input_sources": {},
            "element_set_indices": [],
            "element_sequence_indices": {},
        }
        self.metadata["template"]["tasks"].insert(new_index, task_js)
        self.metadata["tasks"].insert(new_index, empty_task)
        self._save_metadata()

        new_task = self.tasks[new_index]

        return new_task

    def _add_task(self, task: Task, parent_events, new_index=None):

        evt = self.event_log.event_add_task(new_index, parents=parent_events)
        parent_events = parent_events + [evt.index]
        new_wk_task = self._add_empty_task(
            task=task,
            new_index=new_index,
            parent_events=parent_events,
        )
        new_wk_task._add_elements(
            element_sets=task.element_sets,
            parent_events=parent_events,
        )

    def add_task(self, task: Task, new_index=None):
        with self.batch_update():
            self._add_task(task, new_index=new_index, parent_events=[])

    def add_task_after(self, task_ref):
        # TODO: find position of new task, then call add_task
        # TODO: add new downstream elements?
        pass

    def add_task_before(self, task_ref):
        # TODO: find position of new task, then call add_task
        # TODO: add new downstream elements?
        pass

    def submit(self):
        for task in self.tasks:
            task.write_element_dirs()
            for element in task.elements:
                for action in element.resolve_actions():
                    action.execute()

    def rename(self, new_name):
        pass

    def add_submission(self, filter):
        pass


@dataclass
class WorkflowBlueprint:
    """Pre-built workflow templates that are simpler to parametrise (e.g. fitting workflows)."""

    workflow_template: WorkflowTemplate
