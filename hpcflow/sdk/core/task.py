from __future__ import annotations
import copy
from dataclasses import dataclass
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
        sourceable_elements: Optional[List[int]] = None,
    ):

        if isinstance(resources, dict):
            resources = self.app.ResourceList.from_json_like(resources)
        elif isinstance(resources, list):
            resources = self.app.ResourceList(resources)
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
        self.sourceable_elements = sourceable_elements

        self._validate()
        self._set_parent_refs()

        self._task_template = None  # assigned by parent Task
        self._defined_input_types = None  # assigned on _task_template assignment

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        orig_inp = json_like.pop("original_input_sources", None)
        orig_nest = json_like.pop("original_nesting_order", None)
        obj = cls(**json_like)
        obj.original_input_sources = orig_inp
        obj.original_nesting_order = orig_nest
        return obj

    def prepare_persistent_copy(self):
        """Return a copy of self, which will then be made persistent, and save copies of
        attributes that may be changed during integration with the workflow."""
        obj = copy.deepcopy(self)
        obj.original_nesting_order = self.nesting_order
        obj.original_input_sources = self.input_sources
        return obj

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
            if seq_i.path not in self.nesting_order:
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
        sourceable_elements=None,
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
                element_sets = [cls(*args, sourceable_elements=sourceable_elements)]
        else:
            if element_sets is None:
                element_sets = [cls(*args, sourceable_elements=sourceable_elements)]

        return element_sets

    @property
    def defined_input_types(self):
        return self._defined_input_types

    @property
    def undefined_input_types(self):
        return self.task_template.all_schema_input_types - self.defined_input_types

    def get_sequence_from_path(self, sequence_path):
        for i in self.sequences:
            if i.path == sequence_path:
                return i

    def get_defined_parameter_types(self):
        out = []
        for inp in self.inputs:
            if not inp.is_sub_value:
                out.append(inp.normalised_inputs_path)
        for seq in self.sequences:
            if seq.parameter and not seq.is_sub_value:  # ignore resource sequences
                out.append(seq.normalised_inputs_path)
        return out

    def get_defined_sub_parameter_types(self):
        out = []
        for inp in self.inputs:
            if inp.is_sub_value:
                out.append(inp.normalised_inputs_path)
        for seq in self.sequences:
            if seq.parameter and seq.is_sub_value:  # ignore resource sequences
                out.append(seq.normalised_inputs_path)
        return out

    def get_locally_defined_inputs(self):
        return self.get_defined_parameter_types() + self.get_defined_sub_parameter_types()

    def get_sequence_by_path(self, path):
        for seq in self.sequences:
            if seq.path == path:
                return seq

    @property
    def index(self):
        for idx, element_set in enumerate(self.task_template.element_sets):
            if element_set is self:
                return idx

    @property
    def elements(self):
        task = self.task_template.workflow_template.workflow.tasks[
            self.task_template.index
        ]
        elements = task.get_elements_of_element_set(self.index)
        return elements

    @property
    def task_dependencies(self):
        """Get tasks that this element set depends on."""

        dependencies = []
        for element in self.elements:
            for task_dep_i in element.task_dependencies:
                if task_dep_i not in dependencies:
                    dependencies.append(task_dep_i)

        return dependencies

    @property
    def dependent_tasks(self):
        """Get tasks that depend on this element set."""

        dependents = []
        for element in self.elements:
            for task_dep_i in element.dependent_tasks:
                if task_dep_i not in dependents:
                    dependents.append(task_dep_i)
        return dependents

    @property
    def element_dependencies(self):
        """Get indices of elements that this element set depends on."""

        dependencies = []
        for element in self.elements:
            for elem_dep_i in element.element_dependencies:
                if elem_dep_i not in dependencies:
                    dependencies.append(elem_dep_i)

        return dependencies

    @property
    def dependent_elements(self):
        """Get indices of elements that depend on this element set."""

        dependents = []
        for element in self.elements:
            for elem_dep_i in element.dependent_elements:
                if elem_dep_i not in dependents:
                    dependents.append(elem_dep_i)
        return dependents


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
        element_sets: Optional[List[ElementSet]] = None,
        sourceable_elements: Optional[List[int]] = None,
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
            sourceable_elements=sourceable_elements,
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

    def to_persistent(self, workflow):
        """Make a copy where any schema input defaults are saved to a persistent
        workflow. ElementSet data is not made persistent."""

        obj = copy.deepcopy(self)
        new_param_groups = []
        for schema in obj.schemas:
            new_param_groups.extend(schema.make_persistent(workflow))

        return obj, new_param_groups

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
        return self.nesting_order[seq.normalised_path] if len(seq.values) > 1 else -1

    def _prepare_persistent_outputs(self, workflow, num_elements):
        # TODO: check that schema is present when adding task? (should this be here?)
        output_data_indices = {}
        for schema in self.schemas:
            for output in schema.outputs:
                output_data_indices[output.typ] = []
                for _ in range(num_elements):
                    group_idx = workflow._add_parameter_group(
                        data=None,
                        is_pending_add=workflow._in_batch_mode,
                        is_set=False,
                    )
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

    @property
    def _element_indices(self):
        return self.workflow_template.workflow.tasks[self.index].element_indices

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
        for inputs_path, def_val in self.get_all_inputs_info(element_set).items():

            available[inputs_path] = []

            # local specification takes precedence:
            if inputs_path in element_set.get_locally_defined_inputs():
                available[inputs_path].append(self.app.InputSource.local())

            # search for task sources:
            for src_task_i in source_tasks or []:

                for param_i in src_task_i.provides_parameters:

                    if param_i.typ == inputs_path:

                        src_elems = None
                        if element_set.sourceable_elements is not None:
                            # can only use a subset of elements:
                            src_elems = list(
                                set(element_set.sourceable_elements)
                                & set(src_task_i._element_indices)
                            )
                            if not src_elems:
                                continue

                        task_source = self.app.InputSource.task(
                            task_ref=src_task_i.insert_ID,
                            task_source_type=param_i.input_or_output,
                            elements=src_elems,
                        )
                        available[inputs_path].append(task_source)

            if def_val:
                available[inputs_path].append(self.app.InputSource.default())

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
    def all_schema_input_normalised_paths(self):
        return {f"inputs.{i}" for i in self.all_schema_input_types}

    @property
    def all_schema_output_types(self):
        """Get the set of all schema output types (over all specified schemas)."""
        return {out_j for schema_i in self.schemas for out_j in schema_i.output_types}

    @property
    def all_sourced_normalised_paths(self):
        sourced_input_types = []
        for elem_set in self.element_sets:
            for inp in elem_set.inputs:
                if inp.is_sub_value:
                    sourced_input_types.append(inp.normalised_path)
            for seq in elem_set.sequences:
                if seq.is_sub_value:
                    sourced_input_types.append(seq.normalised_path)
        return set(sourced_input_types) | self.all_schema_input_normalised_paths

    def get_all_inputs_info(self, element_set):
        """Get a dict whose keys are the normalised paths (without the "inputs" prefix),
        and whose values are the associated default value InputValue object, in the case
        the input is a SchemaInput, and a default is defined.

        Parameters
        ----------
        element_set : ElementSet
            Find inputs and sequences in this element set that have sub-parameter paths.

        """

        info = {}
        for schema_input in self.all_schema_inputs:
            info[schema_input.parameter.typ] = schema_input.default_value

        for inp_path in element_set.get_defined_sub_parameter_types():
            info[inp_path] = None

        return info

    @property
    def all_sequences_normalised_paths(self):
        return [j.normalised_path for i in self.element_sets for j in i.sequences]

    @property
    def all_used_sequences_normalised_paths(self):
        return [
            j.normalised_path
            for i in self.element_sets
            for j in i.sequences
            if not j.is_unused
        ]

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

    @property
    def _metadata(self):
        return self.workflow_template.workflow.metadata["template"]["tasks"][self.index]

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
        element_set_indices: List,
        element_sequence_indices: Dict,
        index: int,
        workflow: Workflow,
    ):

        self._template = template
        self._element_indices = element_indices
        self._element_input_sources = element_input_sources
        self._element_set_indices = element_set_indices
        self._element_sequence_indices = element_sequence_indices
        self._workflow = workflow
        self._index = index

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.unique_name!r})"

    @property
    def template(self):
        # via the workflow so that we see any metadata updates:
        return self.workflow.tasks[self.index]._template

    @property
    def element_indices(self):
        return self._element_indices

    @property
    def element_input_sources(self):
        return self._element_input_sources

    @property
    def element_set_indices(self):
        return self._element_set_indices

    @property
    def element_sequence_indices(self):
        return self._element_sequence_indices

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

    @property
    def num_element_sets(self):
        return len(
            self.workflow.metadata["template"]["tasks"][self.index]["element_sets"]
        )

    @property
    def _metadata(self):
        return self.workflow.metadata["tasks"][self.index]

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
        sequence_idx = {}
        new_param_groups = []

        # Assign first assuming all locally defined values are to be used:
        for res_i in element_set.resources:
            key, group, is_new = res_i.make_persistent(self.workflow)
            input_data_indices[key] = group
            new_param_groups.extend(group) if is_new else None

        for inp_i in element_set.inputs:
            key, group, is_new = inp_i.make_persistent(self.workflow)
            input_data_indices[key] = group
            input_sources[key] = ["local" for _ in group]
            new_param_groups.extend(group) if is_new else None

        for seq_i in element_set.sequences:
            key, group, is_new = seq_i.make_persistent(self.workflow)
            input_data_indices[key] = group
            input_sources[key] = ["local" for _ in group]
            sequence_idx[key] = list(range(len(group)))
            new_param_groups.extend(group) if is_new else None

        # Now check for task- and default-sources and overwrite or append to local sources:
        for schema_input in self.template.all_schema_inputs:

            key = f"inputs.{schema_input.typ}"
            sources = element_set.input_sources[schema_input.typ]

            for inp_src in sources:

                if inp_src.source_type is InputSourceType.TASK:

                    src_task = inp_src.get_task(self.workflow)

                    src_elements = [i for i in src_task.elements]
                    if inp_src.elements:
                        src_elements = [
                            i for i in src_elements if i.global_index in inp_src.elements
                        ]

                    if not src_elements:
                        continue

                    grp_idx = [
                        elem.data_index[f"outputs.{schema_input.typ}"]
                        for elem in src_elements
                    ]
                    inp_src_i = [
                        f"element.{i.global_index}.{inp_src.task_source_type.name}"
                        for i in src_elements
                    ]

                    if self.app.InputSource.local() in sources:
                        # add task source to existing local source:
                        input_data_indices[key] += grp_idx
                        input_sources[key] += inp_src_i

                    else:
                        # overwrite existing local source (if it exists):
                        input_data_indices[key] = grp_idx
                        input_sources[key] = inp_src_i
                        if key in sequence_idx:
                            sequence_idx.pop(key)
                            seq = element_set.get_sequence_by_path(key)
                            seq.is_unused = True

                if inp_src.source_type is InputSourceType.DEFAULT:

                    grp_idx = [schema_input.default_value._value_group_idx]
                    if self.app.InputSource.local() in sources:
                        input_data_indices[key] += grp_idx
                        input_sources[key] += ["default"]

                    else:
                        input_data_indices[key] = grp_idx
                        input_sources[key] = ["default"]

        return input_data_indices, input_sources, sequence_idx, new_param_groups

    def ensure_input_sources(self, element_set):
        """Check valid input sources are specified for a new task to be added to the
        workflow in a given position. If none are specified, set them according to the
        default behaviour."""

        # this just depends on this schema and other schemas:
        available_sources = self.template.get_available_task_input_sources(
            element_set=element_set,
            source_tasks=self.workflow.template.tasks[: self.index],
        )  # TODO: test all parameters have a key here?

        # TODO: get available input sources from workflow imports

        all_inputs_info = self.template.get_all_inputs_info(element_set)

        # check any specified sources are valid:
        for inputs_path in all_inputs_info:
            for specified_source in element_set.input_sources.get(inputs_path, []):
                self.workflow._resolve_input_source_task_reference(
                    specified_source, self.unique_name
                )
                if not specified_source.is_in(available_sources[inputs_path]):
                    raise ValueError(
                        f"The input source {specified_source.to_string()!r} is not "
                        f"available for input path {inputs_path!r}. Available "
                        f"input sources are: "
                        f"{[i.to_string() for i in available_sources[inputs_path]]}"
                    )

        # TODO: if an input is not specified at all in the `inputs` dict (what about when list?),
        # then check if there is an input files entry for associated inputs,
        # if there is

        unsourced_inputs = set(all_inputs_info.keys()) - set(
            element_set.input_sources.keys()
        )

        # set source for any unsourced inputs:
        missing = []
        for input_type in unsourced_inputs:
            inp_i_sources = available_sources[input_type]
            source = None
            try:
                # first element is defined by default to take precedence in
                # `get_available_task_input_sources`:
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

    def _add_element_set(self, element_set, parent_events):
        """
        Returns
        -------
        element_indices : list of int
            Global indices of newly added elements.

        """

        element_set.task_template = self.template  # may modify element_set.nesting_order
        self.ensure_input_sources(element_set)  # may modify element_set.input_sources

        (
            input_data_idx,
            input_sources,
            seq_idx,
            new_param_groups,
        ) = self._make_new_elements_persistent(element_set)

        multiplicities = self.template.prepare_element_resolution(
            element_set, input_data_idx
        )
        element_data_idx = self.workflow.resolve_element_data_indices(multiplicities)
        output_data_idx = self.template._prepare_persistent_outputs(
            self.workflow, len(element_data_idx)
        )

        (
            new_elements,
            element_input_sources,
            element_seq_idx,
        ) = self.workflow.generate_new_elements(
            input_data_idx,
            output_data_idx,
            element_data_idx,
            input_sources,
            seq_idx,
        )

        element_indices = list(
            range(
                len(self.workflow.elements),
                len(self.workflow.elements) + len(new_elements),
            )
        )

        evt = self.workflow.event_log.event_add_element_set(
            task_index=self.index,
            new_element_indices=element_indices,
            new_parameter_groups=new_param_groups,
            parents=parent_events,
        )

        element_set_js, _ = element_set.to_json_like()
        # (shared data should already have been updated as part of the schema)

        # need to add the element_set first so we can update `element_input_sources` and
        # `element_sequence_indices` keys to include potentially new sequence/input paths
        # from this element set:

        self.template._metadata["element_sets"].append(element_set_js)
        self.workflow._save_metadata()

        self._metadata["element_input_sources"] = {
            **{k: [] for k in self.template.all_sourced_normalised_paths},
            **self._metadata["element_input_sources"],
        }

        self._metadata["element_sequence_indices"] = {
            **{
                k: [None for _ in range(self.num_elements)]
                for k in self.template.all_used_sequences_normalised_paths
            },
            **self._metadata["element_sequence_indices"],
        }

        # Now update the remaining metadata:
        self.workflow.metadata["elements"].extend(new_elements)
        self._metadata["element_indices"].extend(element_indices)

        for k, v in self._metadata["element_input_sources"].items():
            v.extend(element_input_sources.get(k, [None] * len(new_elements)))

        for k, v in self._metadata["element_sequence_indices"].items():
            v.extend(element_seq_idx.get(k, [None] * len(new_elements)))

        self._metadata["element_set_indices"].extend(
            [self.num_element_sets - 1] * len(new_elements)
        )

        self.workflow._save_metadata()

        return element_indices

    @property
    def upstream_tasks(self):
        return [task for task in self.workflow.tasks[: self.index]]

    @property
    def downstream_tasks(self):
        return [task for task in self.workflow.tasks[self.index + 1 :]]

    def get_elements_of_element_set(self, set_index):
        idx = [idx for idx, i in enumerate(self.element_set_indices) if i == set_index]
        elements = [self.workflow.elements[self.element_indices[i]] for i in idx]
        return elements

    def add_elements(
        self,
        base_element=None,
        inputs=None,
        sequences=None,
        resources=None,
        repeats=None,
        input_sources=None,
        input_source_mode=None,
        nesting_order=None,
        element_sets=None,
        sourceable_elements=None,
        propagate_to=None,
        return_indices=False,
    ):
        with self.workflow.batch_update():
            return self._add_elements(
                base_element=base_element,
                inputs=inputs,
                sequences=sequences,
                resources=resources,
                repeats=repeats,
                input_sources=input_sources,
                input_source_mode=input_source_mode,
                nesting_order=nesting_order,
                element_sets=element_sets,
                sourceable_elements=sourceable_elements,
                propagate_to=propagate_to,
                return_indices=return_indices,
                parent_events=[],
            )

    def _add_elements(
        self,
        base_element=None,
        inputs=None,
        sequences=None,
        resources=None,
        repeats=None,
        input_sources=None,
        input_source_mode=None,
        nesting_order=None,
        element_sets=None,
        sourceable_elements=None,
        propagate_to=None,
        return_indices=False,
        parent_events=None,
    ):
        """Add more elements to this task.

        Parameters
        ----------
        sourceable_elements : list of int, optional
            If specified, a list of global element indices from which inputs
            may be sourced. If not specified, all workflow elements are considered
            sourceable.
        propagate_to : list of ElementPropagation, optional
            If specified as an empty or non-empty list, propagate the new elements
            downstream. If an `ElementPropagation` object is not specified for a given
            task, propagation will be attempted using default behaviour.
        return_indices : bool, optional
            If True, return the list of indices of the newly added elements. False by
            default.

        """

        evt = self.workflow.event_log.event_add_elements(
            task_index=self.index,
            parents=parent_events,
        )

        if base_element is not None:
            if base_element.task is not self:
                raise ValueError("If specified, `base_element` must belong to this task.")
            b_inputs, b_resources = base_element.to_element_set_data()
            inputs = inputs or b_inputs
            resources = resources or b_resources

        element_sets = self.app.ElementSet.ensure_element_sets(
            inputs=inputs,
            sequences=sequences,
            resources=resources,
            repeats=repeats,
            input_sources=input_sources,
            input_source_mode=input_source_mode,
            nesting_order=nesting_order,
            element_sets=element_sets,
            sourceable_elements=sourceable_elements,
        )

        parent_events = (parent_events or []) + [evt.index]
        elem_idx = []
        for elem_set_i in element_sets:
            elem_set_i = elem_set_i.prepare_persistent_copy()
            elem_idx += self._add_element_set(elem_set_i, parent_events=parent_events)

        if propagate_to is not None:

            # TODO: also accept a dict as func arg:
            propagate_to = {i.task.unique_name: i for i in propagate_to}

            for task in self.downstream_tasks:

                elem_propagate = propagate_to.get(
                    task.unique_name, ElementPropagation(task=task)
                )
                if self.unique_name not in (
                    i.unique_name for i in elem_propagate.element_set.task_dependencies
                ):
                    # TODO: why can't we just do
                    #  `if self in not elem_propagate.element_set.task_dependencies:`?
                    continue

                # TODO: generate a new ElementSet for this task;
                #       Assume for now we use a single base element set.
                #       Later, allow combining multiple element sets.

                elem_set_i = self.app.ElementSet(
                    inputs=elem_propagate.element_set.inputs,
                    sequences=elem_propagate.element_set.sequences,
                    resources=elem_propagate.element_set.resources,
                    repeats=elem_propagate.element_set.repeats,
                    nesting_order=elem_propagate.nesting_order,
                    sourceable_elements=elem_idx,
                )
                prop_elem_idx = task._add_elements(
                    element_sets=[elem_set_i],
                    return_indices=True,
                    parent_events=parent_events,
                )
                elem_idx.extend(prop_elem_idx)

        if return_indices:
            return elem_idx

    @property
    def task_dependencies(self):
        """Get tasks that this task depends on."""

        dependencies = []
        for element in self.elements:
            for task_dep_i in element.task_dependencies:
                if task_dep_i not in dependencies:
                    dependencies.append(task_dep_i)

        return dependencies

    @property
    def dependent_tasks(self):
        """Get tasks that depend on this task."""
        dependents = []

        # get direct dependents:
        for task in self.downstream_tasks:
            for elem_set in task.template.element_sets:
                for src_lst in elem_set.input_sources.values():
                    for src in src_lst:
                        if src.source_type is self.app.InputSourceType.TASK:
                            if src.task_ref == self.index:
                                if task not in dependents:
                                    dependents.append(task)

        # get indirect dependents:
        for task in dependents:
            for task_i_dep in task.dependent_tasks:
                if task_i_dep not in dependents:
                    dependents.append(task_i_dep)

        return dependents

    @property
    def element_dependencies(self):
        """Get indices of elements that this task depends on."""
        dependencies = []
        for element in self.elements:
            for elem_dep_i in element.element_dependencies:
                if elem_dep_i not in dependencies:
                    dependencies.append(elem_dep_i)

        return dependencies

    @property
    def dependent_elements(self):
        """Get elements that depend on this task."""
        dependents = []
        for element in self.elements:
            for elem_dep_i in element.dependent_elements:
                if elem_dep_i not in dependents:
                    dependents.append(elem_dep_i)

        return dependents


@dataclass
class ElementPropagation:
    """Class to represent how a newly added element set should propagate to a given
    downstream task."""

    task: Task
    element_sets: Optional[Union[List[int], List[ElementSet]]] = None
    nesting_order: Optional[Dict] = None

    @property
    def element_set(self):
        # TEMP property; for now just use the first element set as the base:
        return self.task.template.element_sets[0]
