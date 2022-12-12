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
from .task import Task, WorkflowTask
from .task_schema import TaskSchema
from .utils import group_by_dict_key_values, read_YAML_file
from .errors import InvalidInputSourceTaskReference, MissingInputs, WorkflowNotFoundError

TS_FMT = r"%Y.%m.%d_%H:%M:%S_%z"
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
    def from_YAML_file(cls, path):
        dat = read_YAML_file(path)
        cls.app._ensure_data_files()  # TODO: fix this at App
        return cls.from_json_like(dat, shared_data=cls.app.app_data)


class Workflow:
    """Class to represent a persistent workflow."""

    _app_attr = "app"

    def __init__(self, path):
        """Load a persistent workflow from a path."""

        self.path = path

        root = self._get_workflow_root_group(mode="r")

        self._persistent_metadata = root.attrs.asdict()

        self._shared_data = None
        self._tasks = None
        self._elements = None
        self._template = None

        self.history = root.attrs["history"]

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
                    task=task, data_index=self._persistent_metadata["elements"][i]
                )
                for task in self.tasks
                for i in task.element_indices
            ]
        return self._elements

    @property
    def task_name_repeat_idx(self):
        return self._persistent_metadata["task_name_repeat_idx"]

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

        timestamp = datetime.now(timezone.utc).astimezone()
        history = {
            "timestamp_format": TS_FMT,
            "events": [
                {
                    "type": "create",
                    "at": timestamp.strftime(TS_FMT),
                    "machine": cls.app.config.get("machine"),
                }
            ],
        }

        path = Path(path or "").resolve()
        name = name or f"{template.name}_{timestamp.strftime(TS_NAME_FMT)}"
        path = path.joinpath(name)

        # TODO: extra out input values from template and save in zarr parameter data

        template_js, template_sh = template.to_json_like()
        # print(f"template_js: {template_js}")
        # print(f"template_sh: {template_sh}")

        root_attrs = {
            "history": history,
            "shared_data": template_sh,
            "template": template_js,
            "parameter_mapping": [],
            "elements": [],
            "tasks": [],
            "task_name_repeat_idx": [],
        }

        # TODO: intermittent Dropbox permission error; wrap in exception are retry?
        store = zarr.DirectoryStore(path)
        root = zarr.group(store=store, overwrite=overwrite)
        root.attrs.update(root_attrs)

        root.create_group("parameter_data")

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

    def ensure_input_sources(self, new_task: Task, new_index: int, new_name: str):
        """Check valid input sources are specified for a new task to be added to the
        workflow in a given position. If none are specified, set them according to the
        default behaviour."""

        # TODO: order sources by preference so can just take first in the case of input
        # source mode AUTO?
        available_sources = new_task.get_available_task_input_sources(
            self.template.tasks[:new_index]
        )

        # TODO: get available input sources from workflow imports

        # check any specified sources are valid:
        for schema_input in new_task.all_schema_inputs:
            for specified_source in new_task.input_sources.get(schema_input.typ, []):
                self._resolve_input_source_task_reference(specified_source, new_name)
                if specified_source not in available_sources[schema_input.typ]:
                    raise ValueError(
                        f"The input source {specified_source.to_string()!r} is not "
                        f"available for schema input {schema_input!r}. Available "
                        f"input sources are: "
                        f"{[i.to_string() for i in available_sources[schema_input.typ]]}"
                    )

        # TODO: if an input is not specified at all in the `inputs` dict (what about when list?),
        # then check if there is an input files entry for associated inputs,
        # if there is

        # set source for any unsourced inputs:
        missing = []
        for input_type in new_task.unsourced_inputs:
            inp_i_sources = available_sources[input_type]
            source = None
            try:
                source = inp_i_sources[0]
            except IndexError:
                missing.append(input_type)

            if source is not None:
                new_task.input_sources.update({input_type: [source]})

        if missing:
            missing_str = ", ".join(f"{i!r}" for i in missing)
            raise MissingInputs(
                message=f"The following inputs have no sources: {missing_str}.",
                missing_inputs=missing,
            )

    def _dump_persistent_metadata(self):

        # invalidate workflow attributes to force re-init on access:
        self._tasks = None
        self._elements = None
        self._template = None
        self._shared_data = None
        self._template = None

        root = self._get_workflow_root_group(mode="r+")
        root.attrs.put(self._persistent_metadata)

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
        self, input_data_indices, output_data_indices, element_data_indices
    ):

        # print(f"input_data_indices: {input_data_indices}")
        # print(f"output_data_indices: {output_data_indices}")
        # print(f"element_data_indices: {element_data_indices}")

        new_elements = []
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

        return new_elements

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

    def add_task(self, task: Task, new_index=None):

        if new_index is None:
            new_index = self.num_tasks

        new_task_name = self._get_new_task_unique_name(task, new_index)
        self.ensure_input_sources(task, new_index, new_task_name)

        # TODO: also save the original Task object, since it may be modified before
        # adding to the workflow

        # TODO: think about ability to change input sources? (in the context of adding a new task)
        # what happens when new insert a new task and it outputs a parameter that is used by a downstream task?
        # does it depend on what we originally specify as the downstream tasks's input sources?

        # add input source type "auto" (which is set by default)
        # then a "resolved source" property which is resolved to a specific source depending on the available sources?

        # TODO: perhaps refactor into an `add_elements` that can also be used to add
        # elements to a pre-existing task?

        input_data_idx = task.make_persistent(self)
        multiplicities = task.prepare_element_resolution(input_data_idx)
        element_data_idx = self.resolve_element_data_indices(multiplicities)

        output_data_idx = task._prepare_persistent_outputs(self, len(element_data_idx))
        elements = self.generate_new_elements(
            input_data_idx, output_data_idx, element_data_idx
        )

        element_indices = list(
            range(len(self.elements), len(self.elements) + len(elements))
        )

        task._insert_ID = self.num_tasks
        task._dir_name = f"task_{task.insert_ID}_{new_task_name}"

        task_js, task_shared_data = task.to_json_like()

        # add any missing shared data for this task template:
        for shared_name, shared_data in task_shared_data.items():
            if shared_name not in self._persistent_metadata["shared_data"]:
                self._persistent_metadata["shared_data"][shared_name] = {}
            for k, v in shared_data.items():
                if k not in self._persistent_metadata["shared_data"][shared_name]:
                    self._persistent_metadata["shared_data"][shared_name][k] = v

        self._persistent_metadata["template"]["tasks"].append(task_js)

        wk_task = {"element_indices": element_indices}

        self._persistent_metadata["tasks"].insert(new_index, wk_task)
        self._persistent_metadata["elements"].extend(elements)

        self._dump_persistent_metadata()

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
