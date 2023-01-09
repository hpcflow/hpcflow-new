from __future__ import annotations
from typing import Dict, List, Optional, Tuple, Union

from .json_like import ChildObjectSpec, JSONLike
from .command_files import FileSpec
from .element import ElementFilter, ElementGroup
from .errors import (
    MissingInputs,
    TaskTemplateInvalidNesting,
    TaskTemplateMultipleInputValues,
    TaskTemplateMultipleSchemaObjectives,
    TaskTemplateUnexpectedInput,
    TaskTemplateUnexpectedSequenceInput,
)
from .parameters import (
    InputSource,
    InputSourceMode,
    InputSourceType,
    InputValue,
    ParameterPath,
    SchemaInput,
    SchemaOutput,
    ValuePerturbation,
    ValueSequence,
)
from .utils import get_duplicate_items, get_item_repeat_index


class ElementSet(JSONLike):
    """Class to represent a parametrisation of a new set of elements."""

    _child_objects = (
        ChildObjectSpec(
            name="inputs",
            class_name="InputValue",
            is_multiple=True,
            dict_key_attr="parameter",
            dict_val_attr="value",
            parent_ref="_element_set",
        ),
        ChildObjectSpec(
            name="resources",
            class_name="ResourceList",
            parent_ref="_element_set",
        ),
        ChildObjectSpec(
            name="sequences",
            class_name="ValueSequence",
            is_multiple=True,
            parent_ref="_element_set",
        ),
        ChildObjectSpec(
            name="input_sources",
            class_name="InputSource",
            is_multiple=True,
            is_dict_values=True,
            is_dict_values_ensure_list=True,
        ),
        ChildObjectSpec(
            name="input_source_mode",
            class_name="InputSourceMode",
            is_enum=True,
        ),
    )

    def __init__(
        self,
        inputs: Optional[List[InputValue]] = None,
        sequences: Optional[List[ValueSequence]] = None,
        resources: Optional[Dict[str, Dict]] = None,
        repeats: Optional[Union[int, List[int]]] = 1,
        input_sources: Optional[Dict[str, InputSource]] = None,
        input_source_mode: Optional[Union[str, InputSourceType]] = None,
        nesting_order: Optional[List] = None,
    ):

        if isinstance(resources, dict):
            resources = self.app.ResourceList.from_json_like(resources)
        elif not resources:
            resources = self.app.ResourceList([self.app.ResourceSpec()])

        self.inputs = inputs or []
        self.repeats = repeats
        self.resources = resources
        self.sequences = sequences or []
        self.input_sources = input_sources or {}
        self.input_source_mode = input_source_mode or (
            InputSourceMode.MANUAL if input_sources else InputSourceMode.AUTO
        )
        self.nesting_order = nesting_order or {}

        self._validate()
        self._set_parent_refs()

        self._task_template = None  # assigned by parent Task
        self._defined_input_types = None  # assigned on _task_template assignment

    def to_dict(self):
        dct = super().to_dict()
        del dct["_defined_input_types"]
        del dct["_task_template"]
        return dct

    @property
    def task_template(self):
        return self._task_template

    @task_template.setter
    def task_template(self, value):
        self._task_template = value
        self._validate_against_template()

    @property
    def input_types(self):
        return [i.parameter.typ for i in self.inputs]

    def _validate(self):
        dup_params = get_duplicate_items(self.input_types)
        if dup_params:
            raise TaskTemplateMultipleInputValues(
                f"The following parameters are associated with multiple input value "
                f"definitions: {dup_params!r}."
            )

    def _validate_against_template(self):

        unexpected_types = (
            set(self.input_types) - self.task_template.all_schema_input_types
        )
        if unexpected_types:
            raise TaskTemplateUnexpectedInput(
                f"The following input parameters are unexpected: {list(unexpected_types)!r}"
            )

        seq_inp_types = []
        for seq_i in self.sequences:
            inp_type = seq_i.input_type
            if inp_type:
                bad_inp = {inp_type} - self.task_template.all_schema_input_types
                allowed_str = ", ".join(
                    f'"{i}"' for i in self.task_template.all_schema_input_types
                )
                if bad_inp:
                    raise TaskTemplateUnexpectedSequenceInput(
                        f"The input type {inp_type!r} specified in the following sequence"
                        f" path is unexpected: {seq_i.path!r}. Available input types are: "
                        f"{allowed_str}."
                    )
                seq_inp_types.append(inp_type)
            self.nesting_order.update({seq_i.path: seq_i.nesting_order})

        for k, v in self.nesting_order.items():
            if v < 0:
                raise TaskTemplateInvalidNesting(
                    f"`nesting_order` must be >=0 for all keys, but for key {k!r}, value "
                    f"of {v!r} was specified."
                )

        self._defined_input_types = set(self.input_types + seq_inp_types)

    @classmethod
    def ensure_element_sets(
        cls,
        inputs=None,
        sequences=None,
        resources=None,
        repeats=None,
        input_sources=None,
        input_source_mode=None,
        nesting_order=None,
        element_sets=None,
    ):
        args = (
            inputs,
            sequences,
            resources,
            repeats,
            input_sources,
            input_source_mode,
            nesting_order,
        )
        args_not_none = [i is not None for i in args]

        if any(args_not_none):
            if element_sets is not None:
                raise ValueError(
                    "If providing an `element_set`, no other arguments are allowed."
                )
            else:
                element_sets = [cls(*args)]
        else:
            if element_sets is None:
                element_sets = [cls(*args)]

        return element_sets

    @property
    def defined_input_types(self):
        return self._defined_input_types

    @property
    def undefined_input_types(self):
        return self.task_template.all_schema_input_types - self.defined_input_types


class Task(JSONLike):
    """Parametrisation of an isolated task for which a subset of input values are given
    "locally". The remaining input values are expected to be satisfied by other
    tasks/imports in the workflow."""

    _child_objects = (
        ChildObjectSpec(
            name="schemas",
            class_name="TaskSchema",
            is_multiple=True,
            shared_data_name="task_schemas",
            shared_data_primary_key="name",
            parent_ref="_task_template",
        ),
        ChildObjectSpec(
            name="element_sets",
            class_name="ElementSet",
            is_multiple=True,
            parent_ref="task_template",
        ),
    )

    def __init__(
        self,
        schemas: Union[TaskSchema, str, List[TaskSchema], List[str]],
        repeats: Optional[Union[int, List[int]]] = None,
        resources: Optional[Dict[str, Dict]] = None,
        inputs: Optional[List[InputValue]] = None,
        sequences: Optional[List[ValueSequence]] = None,
        input_sources: Optional[Dict[str, InputSource]] = None,
        input_source_mode: Optional[Union[str, InputSourceType]] = None,
        nesting_order: Optional[List] = None,
        element_sets: Optional[List[ElementSet]] = None,  # TODO
    ):

        """
        Parameters
        ----------
        schema
            A (list of) `TaskSchema` object(s) and/or a (list of) strings that are task
            schema names that uniquely identify a task schema. If strings are provided,
            the `TaskSchema` object will be fetched from the known task schemas loaded by
            the app configuration.

        """

        # TODO: allow init via specifying objective and/or method and/or implementation
        # (lists of) strs e.g.: Task(
        #   objective='simulate_VE_loading',
        #   method=['CP_FFT', 'taylor'],
        #   implementation=['damask', 'damask']
        # )
        # where method and impl must be single strings of lists of the same length
        # and method/impl are optional/required only if necessary to disambiguate
        #
        # this would be like Task(schemas=[
        #   'simulate_VE_loading_CP_FFT_damask',
        #   'simulate_VE_loading_taylor_damask'
        # ])

        if not isinstance(schemas, list):
            schemas = [schemas]

        _schemas = []
        for i in schemas:
            if isinstance(i, str):
                try:
                    i = self.app.TaskSchema.get_by_key(i)
                except KeyError:
                    raise KeyError(f"TaskSchema {i!r} not found.")
            elif not isinstance(i, self.app.TaskSchema):
                raise TypeError(f"Not a TaskSchema object: {i!r}")
            _schemas.append(i)

        self._schemas = _schemas

        self._element_sets = self.app.ElementSet.ensure_element_sets(
            inputs=inputs,
            sequences=sequences,
            resources=resources,
            repeats=repeats,
            input_sources=input_sources,
            input_source_mode=input_source_mode,
            nesting_order=nesting_order,
            element_sets=element_sets,
        )

        self._validate()
        self._name = self._get_name()

        self.workflow_template = None  # assigned by parent WorkflowTemplate
        self._insert_ID = None
        self._dir_name = None

        self._set_parent_refs()

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        insert_ID = json_like.pop("insert_ID", None)
        dir_name = json_like.pop("dir_name", None)
        obj = cls(**json_like)
        obj._insert_ID = insert_ID
        obj._dir_name = dir_name
        return obj

    def __repr__(self):
        return f"{self.__class__.__name__}(" f"name={self.name!r}" f")"

    def to_dict(self):
        out = super().to_dict()
        return {k.lstrip("_"): v for k, v in out.items() if k != "_name"}

    def _validate(self):

        # TODO: check a nesting order specified for each sequence?

        names = set(i.objective.name for i in self.schemas)
        if len(names) > 1:
            raise TaskTemplateMultipleSchemaObjectives(
                f"All task schemas used within a task must have the same "
                f"objective, but found multiple objectives: {list(names)!r}"
            )

    def _get_name(self):
        out = f"{self.objective.name}"
        for idx, schema_i in enumerate(self.schemas, start=1):
            need_and = idx < len(self.schemas) and (
                self.schemas[idx].method or self.schemas[idx].implementation
            )
            out += (
                f"{f'_{schema_i.method}' if schema_i.method else ''}"
                f"{f'_{schema_i.implementation}' if schema_i.implementation else ''}"
                f"{f'_and' if need_and else ''}"
            )
        return out

    @staticmethod
    def get_task_unique_names(tasks: List[Task]):
        """Get the unique name of each in a list of tasks.

        Returns
        -------
        list of str

        """

        task_name_rep_idx = get_item_repeat_index(
            tasks,
            item_callable=lambda x: x.name,
            distinguish_singular=True,
        )

        names = []
        for idx, task in enumerate(tasks):
            add_rep = f"_{task_name_rep_idx[idx]}" if task_name_rep_idx[idx] > 0 else ""
            names.append(f"{task.name}{add_rep}")

        return names

    def _get_nesting_order(self, seq):
        """Find the nesting order for a task sequence."""
        return self.nesting_order[seq._get_param_path()] if len(seq.values) > 1 else -1

    def make_persistent(self, workflow):
        """Add all task input data to a persistent workflow and return a record of the
        Zarr parameter group indices for each bit of task data."""

        input_data_indices = {}

        for res_i in self.resources:
            input_data_indices.update(res_i.make_persistent(workflow))

        for inp_i in self.inputs:
            input_data_indices.update(inp_i.make_persistent(workflow))

        for seq_i in self.sequences:
            input_data_indices.update(seq_i.make_persistent(workflow))

        for inp_typ in self.all_schema_input_types:
            sources = self.input_sources[inp_typ]
            for inp_src in sources:
                if inp_src.source_type is InputSourceType.TASK:
                    src_task = inp_src.get_task(workflow)
                    grp_idx = [
                        elem.data_index[f"outputs.{inp_typ}"]
                        for elem in src_task.elements
                    ]
                    key = f"inputs.{inp_typ}"
                    if self.app.InputSource.local() in sources:
                        # add task source to local source
                        input_data_indices[key] += grp_idx
                    else:
                        input_data_indices.update({key: grp_idx})

        return input_data_indices

    def _prepare_persistent_outputs(self, workflow, num_elements):
        # TODO: check that schema is present when adding task? (should this be here?)
        output_data_indices = {}
        for schema in self.schemas:
            for output in schema.outputs:
                output_data_indices[output.typ] = []
                for i in range(num_elements):
                    group_idx = workflow._add_parameter_group(data=None, is_set=False)
                    output_data_indices[output.typ].append(group_idx)

        return output_data_indices

    def prepare_element_resolution(self, element_set, input_data_indices):

        multiplicities = []
        for path_i, inp_idx_i in input_data_indices.items():
            multiplicities.append(
                {
                    "multiplicity": len(inp_idx_i),
                    "nesting_order": element_set.nesting_order.get(path_i, -1),
                    "path": path_i,
                }
            )

        return multiplicities

    @property
    def index(self):
        if self.workflow_template:
            return self.workflow_template.tasks.index(self)
        else:
            return None

    def get_available_task_input_sources(
        self,
        element_set: ElementSet,
        source_tasks: Optional[List[Task]] = None,
    ) -> List[InputSource]:
        """For each input parameter of this task, generate a list of possible input sources
        that derive from inputs or outputs of this and other provided tasks.

        Note this only produces a subset of available input sources for each input
        parameter; other available input sources may exist from workflow imports."""

        # TODO: also search sub-parameters in the source tasks!

        available = {}
        for schema_input in self.all_schema_inputs:
            available[schema_input.typ] = []

            for src_task_i in source_tasks or []:

                for param_i in src_task_i.provides_parameters:

                    if param_i.typ == schema_input.typ:

                        available[schema_input.typ].append(
                            self.app.InputSource(
                                source_type=self.app.InputSourceType.TASK,
                                task_ref=src_task_i.insert_ID,
                                task_source_type={
                                    "SchemaInput": self.app.TaskSourceType.INPUT,
                                    "SchemaOutput": self.app.TaskSourceType.OUTPUT,
                                }[
                                    param_i.__class__.__name__
                                ],  # TODO: make nicer
                            )
                        )

            if schema_input.typ in element_set.defined_input_types:
                available[schema_input.typ].append(self.app.InputSource.local())

            if schema_input.default_value is not None:
                available[schema_input.typ].append(self.app.InputSource.default())

        return available

    @property
    def schemas(self):
        return self._schemas

    @property
    def element_sets(self):
        return self._element_sets

    @property
    def insert_ID(self):
        return self._insert_ID

    @property
    def dir_name(self):
        "Artefact directory name."
        return self._dir_name

    @property
    def name(self):
        return self._name

    @property
    def objective(self):
        return self.schemas[0].objective

    @property
    def all_schema_inputs(self) -> Tuple[SchemaInput]:
        return tuple(inp_j for schema_i in self.schemas for inp_j in schema_i.inputs)

    @property
    def all_schema_outputs(self) -> Tuple[SchemaOutput]:
        return tuple(inp_j for schema_i in self.schemas for inp_j in schema_i.outputs)

    @property
    def all_schema_input_types(self):
        """Get the set of all schema input types (over all specified schemas)."""
        return {inp_j for schema_i in self.schemas for inp_j in schema_i.input_types}

    @property
    def all_schema_output_types(self):
        """Get the set of all schema output types (over all specified schemas)."""
        return {out_j for schema_i in self.schemas for out_j in schema_i.output_types}

    @property
    def universal_input_types(self):
        """Get input types that are associated with all schemas"""

    @property
    def non_universal_input_types(self):
        """Get input types for each schema that are non-universal."""

    @property
    def defined_input_types(self):
        return self._defined_input_types

    @property
    def undefined_input_types(self):
        return self.all_schema_input_types - self.defined_input_types

    @property
    def undefined_inputs(self):
        return [
            inp_j
            for schema_i in self.schemas
            for inp_j in schema_i.inputs
            if inp_j.typ in self.undefined_input_types
        ]

    @property
    def unsourced_inputs(self):
        """Get schema input types for which no input sources are currently specified."""
        return self.all_schema_input_types - set(self.input_sources.keys())

    @property
    def provides_parameters(self):
        return tuple(j for schema in self.schemas for j in schema.provides_parameters)

    def get_sub_parameter_input_values(self):
        return [i for i in self.inputs if i.is_sub_value]

    def get_non_sub_parameter_input_values(self):
        return [i for i in self.inputs if not i.is_sub_value]

    def add_group(
        self, name: str, where: ElementFilter, group_by_distinct: ParameterPath
    ):
        group = ElementGroup(name=name, where=where, group_by_distinct=group_by_distinct)
        self.groups.add_object(group)


class WorkflowTask:
    """Class to represent a Task that is bound to a Workflow."""

    _app_attr = "app"

    def __init__(
        self,
        template: Task,
        element_indices: List,
        element_input_sources: Dict,
        index: int,
        workflow: Workflow,
    ):

        self._template = template
        self._element_indices = element_indices
        self._element_input_sources = element_input_sources
        self._workflow = workflow
        self._index = index

    @property
    def template(self):
        return self._template

    @property
    def element_indices(self):
        return self._element_indices

    @property
    def elements(self):
        return [self.workflow.elements[i] for i in self.element_indices]

    @property
    def workflow(self):
        return self._workflow

    @property
    def num_elements(self):
        return len(self.element_indices)

    @property
    def index(self):
        """Zero-based position within the workflow. Uses initial index if appending to the
        workflow is not complete."""
        return self._index

    @property
    def name(self):
        return self.template.name

    @property
    def insert_ID(self):
        return self.template.insert_ID

    @property
    def dir_name(self):
        return self.template.dir_name

    @property
    def dir_path(self):
        return self.workflow.path / "tasks" / self.dir_name

    @property
    def unique_name(self):
        return self.workflow.get_task_unique_names()[self.index]

    @property
    def element_dir_list_file_path(self):
        return self.dir_path / "element_dirs.txt"

    @property
    def run_script_file_path(self):
        return self.dir_path / "run_script.ps1"

    def write_element_dirs(self):
        self.dir_path.mkdir(exist_ok=True, parents=True)
        elem_paths = [self.dir_path / elem.dir_name for elem in self.elements]
        for path_i in elem_paths:
            path_i.mkdir(exist_ok=True)

        # write a text file whose lines correspond to element paths
        with self.element_dir_list_file_path.open("wt") as fp:
            for elem in elem_paths:
                fp.write(f"{elem}\n")

    def _make_new_elements_persistent(self, element_set):

        input_data_indices = {}
        input_sources = {}

        for res_i in element_set.resources:
            key, group = res_i.make_persistent(self.workflow)
            input_data_indices[key] = group

        for inp_i in element_set.inputs:
            key, group = inp_i.make_persistent(self.workflow)
            input_data_indices[key] = group
            input_sources[key] = ["local" for _ in group]

        for seq_i in element_set.sequences:
            key, group = seq_i.make_persistent(self.workflow)
            input_data_indices[key] = group
            input_sources[key] = ["local" for _ in group]

        for schema_input in self.template.all_schema_inputs:
            key = f"inputs.{schema_input.typ}"
            sources = element_set.input_sources[schema_input.typ]
            for inp_src in sources:

                if inp_src.source_type is InputSourceType.TASK:

                    src_task = inp_src.get_task(self.workflow)
                    grp_idx = [
                        elem.data_index[f"outputs.{schema_input.typ}"]
                        for elem in src_task.elements
                    ]
                    inp_src_i = [
                        f"element.{i}.{inp_src.task_source_type.name}"
                        for i in src_task.element_indices
                    ]

                    if self.app.InputSource.local() in sources:
                        # add task source to existing local source:
                        input_data_indices[key] += grp_idx
                        input_sources[key] += inp_src_i

                    else:
                        # overwrite existing local source (if it exists):
                        input_data_indices[key] = grp_idx
                        input_sources[key] = inp_src_i

                if inp_src.source_type is InputSourceType.DEFAULT:

                    grp_idx = [schema_input.default_value._value_group_idx]
                    if self.app.InputSource.local() in sources:
                        input_data_indices[key] += grp_idx
                        input_sources[key] += ["default"]

                    else:
                        input_data_indices[key] = grp_idx
                        input_sources[key] = ["default"]

        return input_data_indices, input_sources

    def ensure_input_sources(self, element_set):
        """Check valid input sources are specified for a new task to be added to the
        workflow in a given position. If none are specified, set them according to the
        default behaviour."""

        # TODO: order sources by preference so can just take first in the case of input
        # source mode AUTO?

        # this just depends on this schema and other schemas:
        available_sources = self.template.get_available_task_input_sources(
            element_set=element_set,
            source_tasks=self.workflow.template.tasks[: self.index],
        )  # TODO: test all parameters have a key here?

        # TODO: get available input sources from workflow imports

        # check any specified sources are valid:
        for schema_input in self.template.all_schema_inputs:
            for specified_source in element_set.input_sources.get(schema_input.typ, []):
                self.workflow._resolve_input_source_task_reference(
                    specified_source, self.unique_name
                )
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

        unsourced_inputs = self.template.all_schema_input_types - set(
            element_set.input_sources.keys()
        )

        # set source for any unsourced inputs:
        missing = []
        for input_type in unsourced_inputs:
            inp_i_sources = available_sources[input_type]
            source = None
            try:
                source = inp_i_sources[0]
            except IndexError:
                missing.append(input_type)

            if source is not None:
                element_set.input_sources.update({input_type: [source]})

        if missing:
            missing_str = ", ".join(f"{i!r}" for i in missing)
            raise MissingInputs(
                message=f"The following inputs have no sources: {missing_str}.",
                missing_inputs=missing,
            )

    def add_elements(
        self,
        # base_element=None, # TODO
        inputs=None,
        sequences=None,
        resources=None,
        repeats=None,
        input_sources=None,
        input_source_mode=None,
        nesting_order=None,
        element_sets=None,
    ):

        element_sets = self.app.ElementSet.ensure_element_sets(
            inputs=inputs,
            sequences=sequences,
            resources=resources,
            repeats=repeats,
            input_sources=input_sources,
            input_source_mode=input_source_mode,
            nesting_order=nesting_order,
            element_sets=element_sets,
        )

        for elem_set_i in element_sets:
            elem_set_i.task_template = self.template
            self.ensure_input_sources(
                elem_set_i
            )  # currently modifies element_set.input_sources

            input_data_idx, input_sources = self._make_new_elements_persistent(elem_set_i)
            multiplicities = self.template.prepare_element_resolution(
                elem_set_i, input_data_idx
            )
            element_data_idx = self.workflow.resolve_element_data_indices(multiplicities)
            output_data_idx = self.template._prepare_persistent_outputs(
                self.workflow, len(element_data_idx)
            )

            new_elements, element_input_sources = self.workflow.generate_new_elements(
                input_data_idx,
                output_data_idx,
                element_data_idx,
                input_sources,
            )

            element_indices = list(
                range(
                    len(self.workflow.elements),
                    len(self.workflow.elements) + len(new_elements),
                )
            )

            elem_set_i_js, _ = elem_set_i.to_json_like()
            # (shared data should already have been updated as part of the schema)

            self.workflow._persistent_metadata["elements"].extend(new_elements)
            self.workflow._persistent_metadata["tasks"][self.index][
                "element_indices"
            ].extend(element_indices)

            for k, v in element_input_sources.items():
                self.workflow._persistent_metadata["tasks"][self.index][
                    "element_input_sources"
                ][k].extend(v)

            self.workflow._persistent_metadata["template"]["tasks"][self.index][
                "element_sets"
            ].append(elem_set_i_js)
            self.workflow._append_history_add_element_set(self.index, element_indices)

        self.workflow._dump_persistent_metadata()
