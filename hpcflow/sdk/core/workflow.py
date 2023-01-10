from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
from pprint import pprint
from warnings import warn

import zarr

from .element import Element
from .json_like import ChildObjectSpec, JSONLike
from .zarr_io import zarr_encode
from .object_list import WorkflowTaskList
from .parameters import InputSource
from .loop import Loop
from .task import ElementSet, Task, WorkflowTask
from .task_schema import TaskSchema
from .utils import group_by_dict_key_values, read_YAML, read_YAML_file
from .errors import InvalidInputSourceTaskReference, MissingInputs, WorkflowNotFoundError

TS_FMT = r"%Y.%m.%d_%H:%M:%S.%f_%z"
TS_NAME_FMT = r"%Y-%m-%d_%H%M%S"


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

        root = self._get_workflow_root_group(mode="r")

        self._persistent_metadata = root.attrs.asdict()

        # TODO: load history on demand.
        self._history = root.get("history").attrs.asdict()

        self._shared_data = None
        self._tasks = None
        self._elements = None
        self._template = None

    def _get_workflow_root_group(self, mode):
        try:
            return zarr.open(self.path, mode=mode)
        except zarr.errors.PathNotFoundError:
            raise WorkflowNotFoundError(
                f"No workflow found at path: {self.path}"
            ) from None

    @property
    def shared_data(self):
        if not self._shared_data:
            self._shared_data = self.app.shared_data_from_json_like(
                self._persistent_metadata["shared_data"]
            )
        return self._shared_data

    @property
    def template(self):
        if not self._template:
            self._template = self.app.WorkflowTemplate.from_json_like(
                self._persistent_metadata["template"],
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
                    for idx, i in enumerate(self._persistent_metadata["tasks"])
                ]
            )
        return self._tasks

    @property
    def num_tasks(self):
        return len(self._persistent_metadata["tasks"])

    @property
    def num_elements(self):
        return len(self._persistent_metadata["elements"])

    @property
    def elements(self):
        if not self._elements:
            self._elements = [
                self.app.Element(
                    task=task,
                    data_index=self._persistent_metadata["elements"][i],
                    global_index=i,
                )
                for task in self.tasks
                for i in task.element_indices
            ]
        return self._elements

    @property
    def task_name_repeat_idx(self):
        return self._persistent_metadata["task_name_repeat_idx"]

    @classmethod
    def _get_new_history_event(cls, event_type, **kwargs):
        timestamp = datetime.now(timezone.utc).astimezone()
        event = {
            "type": event_type,
            "at": timestamp.strftime(TS_FMT),
            "machine": cls.app.config.get("machine"),
            "content": kwargs,
        }
        return timestamp, event

    def get_last_event_of_type(self, event_type):
        for evt in self._history["events"][::-1]:
            if evt["type"] == event_type:
                return evt

    def _append_history_event(self, evt):
        self._history["events"].append(evt)
        self._dump_history_metadata()

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

        timestamp, event = cls._get_new_history_event("create")
        history = {
            "timestamp_format": TS_FMT,
            "events": [event],
        }

        path = Path(path or "").resolve()
        name = name or f"{template.name}_{timestamp.strftime(TS_NAME_FMT)}"
        path = path.joinpath(name)

        template_js, template_sh = template.to_json_like()

        root_attrs = {
            "shared_data": template_sh,
            "template": template_js,
            "elements": [],
            "tasks": [],
            "task_name_repeat_idx": [],
        }

        # TODO: intermittent Dropbox permission error; use package `reretry` to retry?
        store = zarr.DirectoryStore(path)
        root = zarr.group(store=store, overwrite=overwrite)
        root.attrs.update(root_attrs)

        root.create_group("parameter_data")

        hist_group = root.create_group("history")
        hist_group.attrs.update(history)

        return cls.load(path)

    @classmethod
    def from_template(cls, template, path=None, name=None, overwrite=False):
        tasks = template.__dict__.pop("tasks") or []
        template.tasks = []
        obj = cls._make_empty_workflow(template, path, name, overwrite)
        for task in tasks:
            obj.add_task(task)
        return obj

    @classmethod
    def from_YAML_file(cls, YAML_path, path=None, name=None, overwrite=False):
        template = cls.app.WorkflowTemplate.from_YAML_file(YAML_path)
        return cls.from_template(template, path, name, overwrite)

    @classmethod
    def load(cls, path):
        """Alias for object initialisation."""
        return cls(path)

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

    def _dump_persistent_metadata(self):

        # invalidate workflow attributes to force re-init on access:
        self._tasks = None
        self._elements = None
        self._template = None
        self._shared_data = None

        root = self._get_workflow_root_group(mode="r+")
        root.attrs.put(self._persistent_metadata)

    def _dump_history_metadata(self):

        hist = self._get_workflow_root_group(mode="r+").get("history")
        hist.attrs.put(self._history)

    def get_zarr_parameter_group(self, group_idx):
        root = self._get_workflow_root_group(mode="r")
        return root.get(f"parameter_data/{group_idx}")

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
                    f"All sequences with the same `nesting_order` must have the same "
                    f"multiplicity, but found multiplicities {list(all_multis)!r} for "
                    f"`nesting_order` of {para_sequences[0]['nesting_order']}."
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

    def _add_parameter_group(self, data, is_set):

        root = self._get_workflow_root_group(mode="r+")
        param_dat_group = root.get("parameter_data")

        names = [int(i) for i in param_dat_group.keys()]
        new_idx = max(names) + 1 if names else 0
        new_name = str(new_idx)

        new_param_group = param_dat_group.create_group(name=new_name)
        zarr_encode(data, new_param_group)

        return new_idx

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

    def _append_history_add_empty_task(self, new_index, added_shared_data):
        _, evt = self._get_new_history_event(
            event_type="add_empty_task",
            new_index=new_index,
            added_shared_data=added_shared_data,
        )
        self._append_history_event(evt)

    def _append_history_add_element_set(self, task_index, element_indices):
        _, evt = self._get_new_history_event(
            event_type="add_element_set",
            task_index=task_index,
            element_indices=element_indices,
        )
        self._append_history_event(evt)

    def _append_history_remove_task(self, index, reason):
        _, evt = self._get_new_history_event(
            event_type="remove_task", index=index, reason=reason
        )
        self._append_history_event(evt)

    def _add_empty_task(self, task: Task, new_index=None):

        if new_index is None:
            new_index = self.num_tasks

        new_task_name = self._get_new_task_unique_name(task, new_index)

        task._insert_ID = self.num_tasks
        task._dir_name = f"task_{task.insert_ID}_{new_task_name}"

        # make any SchemaInput default values persistent:
        for schema in task.schemas:
            schema.make_persistent(self)

        task_js, task_shared_data = task.to_json_like(exclude=["element_sets"])
        task_js["element_sets"] = []

        # add any missing shared data for this task template:
        added_shared_data = {}
        for shared_name, shared_data in task_shared_data.items():
            if shared_name not in self._persistent_metadata["shared_data"]:
                self._persistent_metadata["shared_data"][shared_name] = {}

            added_shared_data[shared_name] = []

            for k, v in shared_data.items():
                if k not in self._persistent_metadata["shared_data"][shared_name]:
                    self._persistent_metadata["shared_data"][shared_name][k] = v
                    added_shared_data[shared_name].append(k)

        empty_task = {
            "element_indices": [],
            "element_input_sources": {},
            "element_set_indices": [],
            "element_sequence_indices": {},
        }
        self._persistent_metadata["template"]["tasks"].insert(new_index, task_js)
        self._persistent_metadata["tasks"].insert(new_index, empty_task)
        self._dump_persistent_metadata()

        self._append_history_add_empty_task(new_index, added_shared_data)

        new_task = self.tasks[new_index]

        return new_task

    def _remove_task(self, index, reason=None):

        # TODO: remove elements etc; and consider downstream data?

        self.app.logger.info(
            f"removing task {index}"
        )  # TODO: get logging working!!! why log twice?

        self._persistent_metadata["template"]["tasks"].pop(index)
        self._persistent_metadata["tasks"].pop(index)
        self._dump_persistent_metadata()

        add_task_event = self.get_last_event_of_type("add_empty_task")
        to_remove_sh = add_task_event["content"]["added_shared_data"]

        for sh_name, sh_hashes in to_remove_sh.items():
            for hash_i in sh_hashes:
                del self._persistent_metadata["shared_data"][sh_name][hash_i]

            if not self._persistent_metadata["shared_data"][sh_name]:
                del self._persistent_metadata["shared_data"][sh_name]

        self._dump_persistent_metadata()

        self._append_history_remove_task(index, reason)

    def add_task(self, task: Task, new_index=None):

        try:
            new_wk_task = self._add_empty_task(task, new_index)
            new_wk_task.add_elements(element_sets=task.element_sets)
        except MissingInputs as err:
            self._remove_task(new_wk_task.index, reason="Failed to add new task.")
            raise err

        # TODO: also save the original Task object, since it may be modified (e.g.
        # input_sources) before adding to the workflow

        # TODO: think about ability to change input sources? (in the context of adding a new task)
        # what happens when new insert a new task and it outputs a parameter that is used by a downstream task?
        # does it depend on what we originally specify as the downstream tasks's input sources?

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
