from __future__ import annotations
import copy
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from valida.datapath import DataPath
from valida.rules import Rule

from .json_like import ChildObjectSpec, JSONLike
from .command_files import FileSpec, InputFile
from .element import ElementFilter, ElementGroup
from .errors import (
    ExtraInputs,
    MissingInputs,
    TaskTemplateInvalidNesting,
    TaskTemplateMultipleInputValues,
    TaskTemplateMultipleSchemaObjectives,
    TaskTemplateUnexpectedInput,
    TaskTemplateUnexpectedSequenceInput,
)
from .parameters import (
    InputSource,
    InputSourceType,
    InputValue,
    ParameterPath,
    SchemaInput,
    SchemaOutput,
    ValueSequence,
)
from .utils import (
    get_duplicate_items,
    get_in_container,
    get_item_repeat_index,
    get_relative_path,
    group_by_dict_key_values,
    set_in_container,
)


INPUT_SOURCE_TYPES = ["local", "default", "task", "import"]


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
            name="input_files",
            class_name="InputFile",
            is_multiple=True,
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
    )

    def __init__(
        self,
        inputs: Optional[List[InputValue]] = None,
        input_files: Optional[List[InputFile]] = None,
        sequences: Optional[List[ValueSequence]] = None,
        resources: Optional[Dict[str, Dict]] = None,
        repeats: Optional[Union[int, List[int]]] = 1,
        input_sources: Optional[Dict[str, InputSource]] = None,
        nesting_order: Optional[List] = None,
        sourceable_elements: Optional[List[int]] = None,
        allow_non_coincident_task_sources: Optional[bool] = False,
    ):
        """
        Parameters
        ----------
        sourceable_elements
            If specified, a list of global element indices from which inputs for the new
            elements associated with this element set may be sourced. If not specified,
            all workflow elements are considered sourceable.
        allow_non_coincident_task_sources
            If True, if more than one parameter is sourced from the same task, then allow
            these sources to come from distinct element sub-sets. If False (default),
            only the intersection of element sub-sets for all parameters are included.

        """

        if isinstance(resources, dict):
            resources = self.app.ResourceList.from_json_like(resources)
        elif isinstance(resources, list):
            resources = self.app.ResourceList(resources)
        elif not resources:
            resources = self.app.ResourceList([self.app.ResourceSpec()])

        self.inputs = inputs or []
        self.input_files = input_files or []
        self.repeats = repeats
        self.resources = resources
        self.sequences = sequences or []
        self.input_sources = input_sources or {}
        self.nesting_order = nesting_order or {}
        self.sourceable_elements = sourceable_elements
        self.allow_non_coincident_task_sources = allow_non_coincident_task_sources

        self._validate()
        self._set_parent_refs()

        self._task_template = None  # assigned by parent Task
        self._defined_input_types = None  # assigned on _task_template assignment
        self._element_local_idx_range = None  # assigned by WorkflowTask._add_element_set
        self._element_global_idx_range = None  # assigned by WorkflowTask._add_element_set

    def __deepcopy__(self, memo):
        dct = self.to_dict()
        elem_local_idx_range = dct.pop("_element_local_idx_range", None)
        elem_global_idx_range = dct.pop("_element_global_idx_range", None)
        obj = self.__class__(**copy.deepcopy(dct, memo))
        obj._task_template = self._task_template
        obj._defined_input_types = self._defined_input_types
        obj._element_local_idx_range = elem_local_idx_range
        obj._element_global_idx_range = elem_global_idx_range
        return obj

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.to_dict() == other.to_dict():
            return True
        return False

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        orig_inp = json_like.pop("original_input_sources", None)
        orig_nest = json_like.pop("original_nesting_order", None)
        elem_local_idx_range = json_like.pop("_element_local_idx_range", None)
        elem_global_idx_range = json_like.pop("_element_global_idx_range", None)
        obj = cls(**json_like)
        obj.original_input_sources = orig_inp
        obj.original_nesting_order = orig_nest
        obj._element_local_idx_range = elem_local_idx_range
        obj._element_global_idx_range = elem_global_idx_range
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

    @property
    def element_local_idx_range(self):
        return tuple(self._element_local_idx_range)

    @property
    def element_global_idx_range(self):
        return tuple(self._element_global_idx_range)

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
        input_files=None,
        sequences=None,
        resources=None,
        repeats=None,
        input_sources=None,
        nesting_order=None,
        element_sets=None,
        sourceable_elements=None,
    ):
        args = (
            inputs,
            input_files,
            sequences,
            resources,
            repeats,
            input_sources,
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
    def task(self):
        return self.task_template.workflow_template.workflow.tasks[
            self.task_template.index
        ]

    @property
    def elements(self):
        elements = self.task.get_elements_of_element_set(self.index)
        return elements

    def get_task_dependencies(self, as_objects=False):
        """Get upstream tasks that this element set depends on."""
        deps = []
        for element in self.elements:
            for dep_i in element.get_task_dependencies(as_objects=False):
                if dep_i not in deps:
                    deps.append(dep_i)
        deps = sorted(deps)
        if as_objects:
            deps = [self.task.workflow.tasks.get(insert_ID=i) for i in deps]

        return deps


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
        input_files: Optional[List[InputFile]] = None,
        sequences: Optional[List[ValueSequence]] = None,
        input_sources: Optional[Dict[str, InputSource]] = None,
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
            input_files=input_files,
            sequences=sequences,
            resources=resources,
            repeats=repeats,
            input_sources=input_sources,
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

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.to_dict() == other.to_dict():
            return True
        return False

    def _add_element_set(self, element_set: ElementSet):
        """Invoked by WorkflowTask._add_element_set."""
        self._element_sets.append(element_set)
        self.workflow_template.workflow._store.add_element_set(
            self.index, element_set.to_json_like()[0]
        )

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

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        _insert_ID = kwargs.pop("insert_ID")
        _dir_name = kwargs.pop("dir_name")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._insert_ID = _insert_ID
        obj._dir_name = _dir_name
        obj._name = self._name
        obj.workflow_template = self.workflow_template
        return obj

    def to_persistent(self, workflow, insert_ID):
        """Return a copy where any schema input defaults are saved to a persistent
        workflow. Element set data is not made persistent."""

        obj = copy.deepcopy(self)
        new_refs = []
        source = {"type": "default_input", "task_insert_ID": insert_ID}
        for schema in obj.schemas:
            new_refs.extend(schema.make_persistent(workflow, source))

        return obj, new_refs

    def to_dict(self):
        out = super().to_dict()
        return {k.lstrip("_"): v for k, v in out.items() if k != "_name"}

    def set_sequence_parameters(self, element_set):
        # set ValueSequence Parameter objects:
        for seq in element_set.sequences:
            if seq.input_type:
                for schema_i in self.schemas:
                    for inp_j in schema_i.inputs:
                        if inp_j.typ == seq.input_type:
                            seq._parameter = inp_j.parameter

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

    def _prepare_persistent_outputs(self, workflow, local_element_idx_range):
        # TODO: check that schema is present when adding task? (should this be here?)
        output_data_indices = {}
        for schema in self.schemas:
            for output in schema.outputs:

                # TODO: consider multiple schemas in action index?

                # Find the last action where specified output type is an output:
                output_act_idx = None
                for act_idx, act in enumerate(schema.actions):
                    if output.typ in act.get_output_types():
                        output_act_idx = act_idx

                if output_act_idx is None:
                    raise RuntimeError(
                        f"Output {output} does not appear in any schema actions."
                    )

                path = f"outputs.{output.typ}"
                output_data_indices[path] = []
                for idx in range(*local_element_idx_range):
                    param_src = {
                        "type": "EAR_output",
                        "task_insert_ID": self.insert_ID,
                        "element_idx": idx,
                        "action_idx": output_act_idx,
                        "run_idx": 0,
                    }
                    data_ref = workflow._add_unset_parameter_data(param_src)
                    output_data_indices[path].append(data_ref)

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
        for inputs_path, inp_info in self.get_all_inputs_info(element_set).items():

            available[inputs_path] = []

            # local specification takes precedence:
            if inputs_path in element_set.get_locally_defined_inputs():
                available[inputs_path].append(self.app.InputSource.local())

            # search for task sources:
            for src_task_i in source_tasks or []:

                for param_i in src_task_i.provides_parameters:

                    if param_i.typ == inputs_path:

                        if param_i.input_or_output == "input":
                            # input parameter might not be provided e.g. if it only used
                            # to generate an input file, and that input file is passed
                            # directly, so consider only source task element sets that
                            # provide the input:
                            es_idx = src_task_i.get_param_provided_element_sets(
                                param_i.typ
                            )
                        else:
                            # outputs are always available, so consider all source task
                            # element sets:
                            es_idx = range(src_task_i.num_element_sets)

                        if not es_idx:
                            continue
                        else:
                            src_elems = []
                            for es_idx_i in es_idx:
                                es_i = src_task_i.element_sets[es_idx_i]
                                src_elems += list(range(*es_i.element_global_idx_range))

                        if element_set.sourceable_elements is not None:
                            # can only use a subset of elements (this is the case where
                            # this element set is generated from an upstream element set,
                            # in which case we only want to consider newly added upstream
                            # elements when adding elements from this element set):
                            src_elems = list(
                                set(element_set.sourceable_elements) & set(src_elems)
                            )
                            if not src_elems:
                                continue

                        task_source = self.app.InputSource.task(
                            task_ref=src_task_i.insert_ID,
                            task_source_type=param_i.input_or_output,
                            elements=src_elems,
                        )
                        available[inputs_path].append(task_source)

            if inp_info["has_default"]:
                available[inputs_path].append(self.app.InputSource.default())

        return available

    @property
    def schemas(self):
        return self._schemas

    @property
    def element_sets(self):
        return self._element_sets

    @property
    def num_element_sets(self):
        return len(self._element_sets)

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

    def get_schema_action(self, idx):
        _idx = 0
        for schema in self.schemas:
            for action in schema.actions:
                if _idx == idx:
                    return action
                _idx += 1
        raise ValueError(f"No action in task {self.name!r} with index {idx!r}.")

    def all_schema_actions(self) -> Iterator[Tuple[int, Action]]:
        idx = 0
        for schema in self.schemas:
            for action in schema.actions:
                yield (idx, action)
                idx += 1

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

    def is_input_type_required(self, typ, element_set):

        provided_files = [i.file for i in element_set.input_files]
        # required if is appears in any command:
        for schema in self.schemas:
            for act in schema.actions:
                if typ in act.get_command_input_types():
                    return True

                # required if used in any input file generators and input file is not
                # provided:
                for IFG in act.input_file_generators:
                    if typ in (i.typ for i in IFG.inputs):
                        if IFG.input_file not in provided_files:
                            return True

        return False

    def is_input_type_provided(self, typ: str, element_set: ElementSet) -> bool:
        """Check if an input is provided as an InputValue or a ValueSequence."""
        for inp in element_set.inputs:
            if typ == inp.parameter.typ:
                return True

        for seq in element_set.sequences:
            if not seq.is_sub_value and typ == seq.parameter.typ:
                return True

        return False

    def get_param_provided_element_sets(self, typ: str) -> List[int]:
        """Get the element set indices of this task for which a specified parameter type
        is provided."""
        es_idx = []
        for idx, src_es in enumerate(self.element_sets):
            if self.is_input_type_provided(typ, src_es):
                es_idx.append(idx)
        return es_idx

    def get_all_inputs_info(self, element_set):
        """Get a dict whose keys are the normalised paths (without the "inputs" prefix),
        and whose values are the associated default value InputValue object, in the case
        the input is a SchemaInput, and a default is defined.

        # TODO update docstring

        Parameters
        ----------
        element_set : ElementSet
            Find inputs and sequences in this element set that have sub-parameter paths.

        """

        info = {}
        for schema_input in self.all_schema_inputs:
            info[schema_input.parameter.typ] = {
                "has_default": schema_input.default_value is not None
            }

        for inp_path in element_set.get_defined_sub_parameter_types():
            info[inp_path] = {"has_default": False}

        for inp in info:
            info[inp]["is_required"] = self.is_input_type_required(inp, element_set)
            info[inp]["is_provided"] = self.is_input_type_provided(inp, element_set)

        return info

    def get_all_required_schema_inputs(self, element_set):
        info = self.get_all_inputs_info(element_set)
        return tuple(
            i for i in self.all_schema_inputs if info[i.parameter.typ]["is_required"]
        )

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
        workflow: Workflow,
        template: Task,
        index: int,
        num_elements: int,
    ):
        self._workflow = workflow
        self._template = template
        self._index = index
        self._num_elements = num_elements

        # assigned/incremented when new elements are added and reset on dump to disk:
        self._pending_num_elements = 0

        self._elements = None  # assigned on `elements` first access

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.unique_name!r})"

    def _reset_pending_elements(self):
        self._pending_num_elements = 0

    def _accept_pending_elements(self):
        self._num_elements = self.num_elements
        self._reset_pending_elements()

    @classmethod
    def new_empty_task(cls, workflow: Workflow, template: Task, index: int):
        obj = cls(
            workflow=workflow,
            template=template,
            index=index,
            num_elements=0,
        )
        return obj

    @property
    def workflow(self):
        return self._workflow

    @property
    def template(self):
        return self._template

    @property
    def index(self):
        return self._index

    @property
    def num_elements(self):
        return self._num_elements + self._pending_num_elements

    @property
    def name(self):
        return self.template.name

    @property
    def unique_name(self):
        return self.workflow.get_task_unique_names()[self.index]

    @property
    def insert_ID(self):
        return self.template.insert_ID

    @property
    def dir_name(self):
        return self.template.dir_name

    @property
    def num_element_sets(self):
        return self.template.num_element_sets

    @property
    def elements(self):
        if self._elements is None:
            self._elements = self.app.Elements(self)
        return self._elements

    @property
    def dir_path(self):
        return self.workflow.path / "tasks" / self.dir_name

    @property
    def element_dir_list_file_path(self):
        return self.dir_path / "element_dirs.txt"

    @property
    def run_script_file_path(self):
        return self.dir_path / "run_script.ps1"

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

    def _make_new_elements_persistent(self, element_set, element_set_idx):
        """Save parameter data to the persistent workflow."""

        input_data_idx = {}
        sequence_idx = {}

        # Assign first assuming all locally defined values are to be used:
        param_src = {
            "type": "local_input",
            "task_insert_ID": self.insert_ID,
            "element_set_idx": element_set_idx,
        }
        for res_i in element_set.resources:
            key, dat_ref, _ = res_i.make_persistent(self.workflow, param_src)
            input_data_idx[key] = dat_ref

        for inp_i in element_set.inputs:
            key, dat_ref, _ = inp_i.make_persistent(self.workflow, param_src)
            input_data_idx[key] = dat_ref

        for inp_file_i in element_set.input_files:
            key, dat_ref, _ = inp_file_i.make_persistent(self.workflow, param_src)
            input_data_idx[key] = dat_ref

        for seq_i in element_set.sequences:
            key, dat_ref, _ = seq_i.make_persistent(self.workflow, param_src)
            input_data_idx[key] = dat_ref
            sequence_idx[key] = list(range(len(dat_ref)))

        # Now check for task- and default-sources and overwrite or append to local sources:
        for schema_input in self.template.get_all_required_schema_inputs(element_set):

            key = f"inputs.{schema_input.typ}"
            sources = element_set.input_sources[schema_input.typ]

            for inp_src in sources:

                if inp_src.source_type is InputSourceType.TASK:

                    src_task = inp_src.get_task(self.workflow)

                    src_elements = [i for i in src_task.elements]  # TODO: ??
                    if inp_src.elements:
                        # only include "sourceable" elements:
                        src_elements = [
                            i for i in src_elements if i.global_idx in inp_src.elements
                        ]

                    if not src_elements:
                        continue

                    task_source_type = inp_src.task_source_type.name.lower()
                    src_key = f"{task_source_type}s.{schema_input.typ}"
                    grp_idx = [elem.get_data_idx()[src_key] for elem in src_elements]

                    if self.app.InputSource.local() in sources:
                        # add task source to existing local source:
                        input_data_idx[key] += grp_idx

                    else:
                        # overwrite existing local source (if it exists):
                        input_data_idx[key] = grp_idx
                        if key in sequence_idx:
                            sequence_idx.pop(key)
                            seq = element_set.get_sequence_by_path(key)
                            seq.is_unused = True

                if inp_src.source_type is InputSourceType.DEFAULT:

                    grp_idx = [schema_input.default_value._value_group_idx]
                    if self.app.InputSource.local() in sources:
                        input_data_idx[key] += grp_idx

                    else:
                        input_data_idx[key] = grp_idx

        # sort smallest to largest path, so more-specific items overwrite less-specific
        # items parameter retrieval:
        # TODO: is this still necessary?
        # data_idx_paths = sorted(input_data_idx.keys(), key=lambda x: len(x.split(".")))
        # input_data_idx = {data_idx_paths.index(k): v for k, v in input_data_idx.items()}
        # input_sources = {data_idx_paths.index(k): v for k, v in input_sources.items()}

        return (input_data_idx, sequence_idx)

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

        # an input is not required if it is only used to generate an input file that is
        # passed directly:
        req_types = set(k for k, v in all_inputs_info.items() if v["is_required"])
        unsourced_inputs = req_types - set(element_set.input_sources.keys())

        extra_types = set(
            k
            for k, v in all_inputs_info.items()
            if not v["is_required"] and v["is_provided"]
        )
        if extra_types:
            extra_str = ", ".join(f"{i!r}" for i in extra_types)
            raise ExtraInputs(
                message=(
                    f"The following inputs are not required, but have been passed: "
                    f"{extra_str}."
                ),
                extra_inputs=extra_types,
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

        # TODO: collate all input sources separately, then can fall back to a different
        # input source (if it was not specified manually) and if the "top" input source
        # results in no available elements due to `allow_non_coincident_task_sources`.

        if not element_set.allow_non_coincident_task_sources:

            sources_by_task = {}
            for inp_type, sources in element_set.input_sources.items():
                source = sources[0]
                if source.source_type is InputSourceType.TASK:
                    if source.task_ref not in sources_by_task:
                        sources_by_task[source.task_ref] = {}
                    sources_by_task[source.task_ref][inp_type] = source

            # if multiple parameters are sourced from the same upstream task, only use
            # elements for which all parameters are available (the set intersection):
            for sources in sources_by_task.values():
                first_src = next(iter(sources.values()))
                intersect_task_i = set(first_src.elements)
                for src_i in sources.values():
                    intersect_task_i.intersection_update(src_i.elements)

                # now change elements for the affected input sources:
                for inp_type in sources.keys():
                    element_set.input_sources[inp_type][0].elements = list(
                        intersect_task_i
                    )

        if missing:
            missing_str = ", ".join(f"{i!r}" for i in missing)
            raise MissingInputs(
                message=f"The following inputs have no sources: {missing_str}.",
                missing_inputs=missing,
            )

    def generate_new_elements(
        self,
        input_data_indices,
        output_data_indices,
        element_data_indices,
        sequence_indices,
    ):

        new_elements = []
        element_sequence_indices = {}
        for i_idx, i in enumerate(element_data_indices):
            elem_i = {k: input_data_indices[k][v] for k, v in i.items()}
            elem_i.update({k: v[i_idx] for k, v in output_data_indices.items()})
            new_elements.append(elem_i)

            # track which sequence value indices (if any) are used for each new element:
            for k, v in i.items():
                if k in sequence_indices:
                    if k not in element_sequence_indices:
                        element_sequence_indices[k] = []
                    element_sequence_indices[k].append(sequence_indices[k][v])

        return new_elements, element_sequence_indices

    @property
    def upstream_tasks(self):
        return [task for task in self.workflow.tasks[: self.index]]

    @property
    def downstream_tasks(self):
        return [task for task in self.workflow.tasks[self.index + 1 :]]

    def get_elements_of_element_set(self, element_set_index):
        elem_slice = slice(
            *self.template.element_sets[element_set_index].element_local_idx_range
        )
        return self.elements[elem_slice]

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
                    f"{[i['path'] for i in para_sequences]} with "
                    f"`nesting_order` {para_sequences[0]['nesting_order']} found "
                    f"multiplicities {[i['multiplicity'] for i in para_sequences]}."
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

    def _add_element_set(self, element_set):
        """
        Returns
        -------
        element_indices : list of int
            Global indices of newly added elements.

        """

        self.template.set_sequence_parameters(element_set)

        self.ensure_input_sources(element_set)  # may modify element_set.input_sources

        (input_data_idx, seq_idx) = self._make_new_elements_persistent(
            element_set=element_set,
            element_set_idx=self.num_element_sets,
        )

        element_set.task_template = self.template  # may modify element_set.nesting_order

        multiplicities = self.template.prepare_element_resolution(
            element_set, input_data_idx
        )

        element_inp_data_idx = self.resolve_element_data_indices(multiplicities)

        global_element_idx_range = [
            self.workflow.num_elements,
            self.workflow.num_elements + len(element_inp_data_idx),
        ]
        local_element_idx_range = [
            self.num_elements,
            self.num_elements + len(element_inp_data_idx),
        ]

        # associate global element indices (as a range) with the element set:
        element_set._element_global_idx_range = global_element_idx_range
        element_set._element_local_idx_range = local_element_idx_range
        self.template._add_element_set(element_set)

        output_data_idx = self.template._prepare_persistent_outputs(
            workflow=self.workflow,
            local_element_idx_range=local_element_idx_range,
        )

        (element_data_idx, element_seq_idx) = self.generate_new_elements(
            input_data_idx,
            output_data_idx,
            element_inp_data_idx,
            seq_idx,
        )

        elements = []

        for elem_idx, data_idx in enumerate(element_data_idx):
            schema_params = set(i for i in data_idx.keys() if len(i.split(".")) == 2)
            data_idx_i = data_idx
            action_runs = {}
            for act_idx, action in self.template.all_schema_actions():
                if all(self.test_action_rule(i, data_idx) for i in action.rules):

                    param_source = {
                        "type": "EAR_output",
                        "task_insert_ID": self.insert_ID,
                        "element_idx": self.num_elements + elem_idx,
                        "action_idx": act_idx,
                        "run_idx": 0,
                    }
                    data_idx_i = action.generate_data_index(
                        data_idx=data_idx,
                        workflow=self.workflow,
                        param_source=param_source,
                    )
                    run_0 = {"data_idx": data_idx_i}
                    action_runs[act_idx] = [run_0]
                    data_idx.update(data_idx_i)

            elements.append(
                {
                    "global_idx": self.workflow.num_elements + elem_idx,
                    "index": self.num_elements + elem_idx,
                    "actions": action_runs,
                    "schema_parameters": list(schema_params),
                    "es_idx": self.num_element_sets - 1,
                    "seq_idx": {k: v[elem_idx] for k, v in element_seq_idx.items()},
                    "loop_idx": {},  # TODO
                }
            )

        self.workflow._store.add_elements(self.index, self.insert_ID, elements)
        self._pending_num_elements += len(elements)

        return list(range(*global_element_idx_range))

    def add_elements(
        self,
        base_element=None,
        inputs=None,
        input_files=None,
        sequences=None,
        resources=None,
        repeats=None,
        input_sources=None,
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
                input_files=input_files,
                sequences=sequences,
                resources=resources,
                repeats=repeats,
                input_sources=input_sources,
                nesting_order=nesting_order,
                element_sets=element_sets,
                sourceable_elements=sourceable_elements,
                propagate_to=propagate_to,
                return_indices=return_indices,
            )

    def _add_elements(
        self,
        base_element=None,
        inputs=None,
        input_files=None,
        sequences=None,
        resources=None,
        repeats=None,
        input_sources=None,
        nesting_order=None,
        element_sets=None,
        sourceable_elements=None,
        propagate_to=None,
        return_indices=False,
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

        if base_element is not None:
            if base_element.task is not self:
                raise ValueError("If specified, `base_element` must belong to this task.")
            b_inputs, b_resources = base_element.to_element_set_data()
            inputs = inputs or b_inputs
            resources = resources or b_resources

        element_sets = self.app.ElementSet.ensure_element_sets(
            inputs=inputs,
            input_files=input_files,
            sequences=sequences,
            resources=resources,
            repeats=repeats,
            input_sources=input_sources,
            nesting_order=nesting_order,
            element_sets=element_sets,
            sourceable_elements=sourceable_elements,
        )

        elem_idx = []  # global element indices
        for elem_set_i in element_sets:
            elem_set_i = elem_set_i.prepare_persistent_copy()
            elem_idx += self._add_element_set(elem_set_i)

        if propagate_to is not None:

            # TODO: also accept a dict as func arg:
            propagate_to = {i.task.unique_name: i for i in propagate_to}

            for task in self.downstream_tasks:

                elem_propagate = propagate_to.get(
                    task.unique_name, ElementPropagation(task=task)
                )
                if self.unique_name not in (
                    i.unique_name
                    for i in elem_propagate.element_set.get_task_dependencies(
                        as_objects=True
                    )
                ):
                    # TODO: why can't we just do
                    #  `if self in not elem_propagate.element_set.task_dependencies:`?
                    continue

                # TODO: generate a new ElementSet for this task;
                #       Assume for now we use a single base element set.
                #       Later, allow combining multiple element sets.

                elem_set_i = self.app.ElementSet(
                    inputs=elem_propagate.element_set.inputs,
                    input_files=elem_propagate.element_set.input_files,
                    sequences=elem_propagate.element_set.sequences,
                    resources=elem_propagate.element_set.resources,
                    repeats=elem_propagate.element_set.repeats,
                    nesting_order=elem_propagate.nesting_order,
                    sourceable_elements=elem_idx,
                )
                prop_elem_idx = task._add_elements(
                    element_sets=[elem_set_i],
                    return_indices=True,
                )
                elem_idx.extend(prop_elem_idx)

        if return_indices:
            return elem_idx

    def get_task_dependencies(
        self,
        as_objects: bool = False,
    ) -> List[Union[int, WorkflowTask]]:
        """Get tasks (insert ID or WorkflowTask objects) that this task depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""

        deps = []
        for element_set in self.template.element_sets:
            for sources in element_set.input_sources.values():
                for src in sources:
                    if (
                        src.source_type is InputSourceType.TASK
                        and src.task_ref not in deps
                    ):
                        deps.append(src.task_ref)

        deps = sorted(deps)
        if as_objects:
            deps = [self.workflow.tasks.get(insert_ID=i) for i in deps]

        return deps

    def get_dependent_tasks(
        self,
        as_objects: bool = False,
    ) -> List[Union[int, WorkflowTask]]:
        """Get tasks (insert ID or WorkflowTask objects) that depends on this task."""
        deps = []
        for task in self.downstream_tasks:
            for element_set in task.template.element_sets:
                for sources in element_set.input_sources.values():
                    for src in sources:
                        if (
                            src.source_type is InputSourceType.TASK
                            and src.task_ref == self.insert_ID
                            and task.insert_ID not in deps
                        ):
                            deps.append(task.insert_ID)
        deps = sorted(deps)
        if as_objects:
            deps = [self.workflow.tasks.get(insert_ID=i) for i in deps]
        return deps

    def get_element_dependencies(
        self,
        as_objects: bool = False,
    ) -> List[Union[Tuple[int, int], Element]]:
        """Get elements from upstream tasks (tuples of (task_insert_ID, element idx) or
        Element objects) that this task depends on."""

        deps = []
        for element in self.elements:
            for (ti_ID, e_idx) in element.get_element_dependencies(as_objects=False):
                if (ti_ID, e_idx) not in deps:
                    deps.append((ti_ID, e_idx))

        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_elements_from_indices(deps)

        return deps

    def get_dependent_elements(
        self,
        as_objects: bool = False,
    ) -> List[Union[Tuple[int, int], Element]]:
        """Get elements from downstream tasks (tuples of (task_insert_ID, element idx) or
        Element objects) that depend on this task."""
        deps = []
        for task in self.downstream_tasks:
            for element in task.elements:
                for dep_i in element.get_task_dependencies(as_objects=False):
                    if dep_i == self.insert_ID:
                        key = (task.insert_ID, element.index)
                        if key not in deps:
                            deps.append(key)

        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_elements_from_indices(deps)

        return deps

    @property
    def inputs(self):
        return self.app.TaskInputParameters(self)

    @property
    def outputs(self):
        return self.app.TaskOutputParameters(self)

    def get(self, path, raise_on_missing=False, default=None):
        return self.app.Parameters(
            self,
            path=path,
            return_element_parameters=False,
            raise_on_missing=raise_on_missing,
            default=default,
        )

    def _path_to_parameter(self, path):
        if len(path) != 2 or path[0] == "resources":
            return

        if path[0] == "inputs":
            for i in self.template.schemas:
                for j in i.inputs:
                    if j.parameter.typ == path[1]:
                        return j.parameter

        elif path[0] == "outputs":
            for i in self.template.schemas:
                for j in i.outputs:
                    if j.parameter.typ == path[1]:
                        return j.parameter

    def _get_merged_parameter_data(
        self,
        data_index,
        path=None,
        raise_on_missing=False,
        default: Any = None,
    ):
        """Get element data from the persistent store."""

        path = [] if not path else path.split(".")
        parameter = self._path_to_parameter(path)
        current_value = None
        is_cur_val_set = False
        for path_i, data_idx_i in data_index.items():

            path_i = path_i.split(".")
            is_parent = False
            is_update = False
            try:
                rel_path = get_relative_path(path, path_i)
                is_parent = True
            except ValueError:
                try:
                    update_path = get_relative_path(path_i, path)
                    is_update = True

                except ValueError:
                    # no intersection between paths
                    continue

            # TODO: test unset outputs are returned correctly
            data = self.workflow._get_parameter_data(data_idx_i)

            if is_parent:
                # replace current value:
                try:
                    current_value = get_in_container(data, rel_path, cast_indices=True)
                    is_cur_val_set = True
                except (KeyError, IndexError, ValueError):
                    continue

            elif is_update:
                # update sub-part of current value
                current_value = current_value or {}
                set_in_container(current_value, update_path, data, ensure_path=True)
                is_cur_val_set = True

        if not is_cur_val_set:
            if raise_on_missing:
                raise ValueError(f"Path {path} does not exist in the element data.")
            else:
                current_value = default

        if parameter and parameter._value_class:
            current_value = parameter._value_class(**current_value)

        return current_value

    def test_action_rule(self, rule, data_idx):
        check = rule.check_exists or rule.check_missing
        if check:
            param_s = check.split(".")
            if len(param_s) > 2:
                # sub-parameter, so need to retrieve parameter data
                try:
                    self._get_merged_parameter_data(data_idx, raise_on_missing=True)
                    return True if rule.check_exists else False
                except ValueError:
                    return False if rule.check_exists else True
            else:
                if rule.check_exists:
                    return rule.check_exists in data_idx
                elif rule.check_missing:
                    return rule.check_missing not in data_idx

        else:
            # retrieve parameter data
            param_path = ".".join(
                i.condition.callable.kwargs["value"] for i in rule.path.parts[:2]
            )
            element_dat = rule.get(param_path)

            data_path = DataPath(*rule.path.parts[2:])
            rule = Rule(path=data_path, condition=rule.condition, cast=rule.cast)

            return rule.test(element_dat).is_valid


class Elements:

    __slots__ = ("_task",)

    def __init__(self, task: WorkflowTask):

        self._task = task

        # TODO: cache Element objects

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(task={self.task.unique_name!r}, "
            f"num_elements={self.task.num_elements})"
        )

    @property
    def task(self):
        return self._task

    def _get_selection(self, selection):

        if isinstance(selection, int):
            start, stop, step = selection, selection + 1, 1

        elif isinstance(selection, slice):
            start, stop, step = selection.start, selection.stop, selection.step
            stop = self.task.num_elements if stop is None else stop
            start = start or 0
            step = 1 if step is None else step

        selection = slice(start, stop, step)
        length = len(range(*selection.indices(self.task.num_elements)))

        return selection, length

    def islice(
        self,
        start: int = None,
        end: int = None,
    ) -> Iterator[Element]:

        selection, _ = self._get_selection(slice(start, end))
        for i in self.task.workflow.get_task_elements_islice(self.task, selection):
            yield i

    def __len__(self):
        return self.task.num_elements

    def __iter__(self):
        return self.islice()

    def __getitem__(
        self,
        selection: Union[int, slice],
    ) -> Union[Element, List[Element]]:

        selection, length = self._get_selection(selection)
        elements = self.task.workflow.get_task_elements(self.task, selection)

        if length == 1:
            return elements[0]
        else:
            return elements


@dataclass
class Parameters:

    _app_attr = "_app"

    task: WorkflowTask
    path: str
    return_element_parameters: bool
    raise_on_missing: Optional[bool] = False
    default: Optional[Any] = None

    def islice(self, start=None, end=None):
        for i in self.task.workflow.pIO.task_parameter_islice(
            self.task, self.path, start, end
        ):
            yield i

    def _get_selection(self, selection):

        if isinstance(selection, int):
            start, stop, step = selection, selection + 1, 1

        elif isinstance(selection, slice):
            start, stop, step = selection.start, selection.stop, selection.step
            stop = self.task.num_elements if stop is None else stop
            start = start or 0
            step = 1 if step is None else step

        selection = slice(start, stop, step)
        length = len(range(*selection.indices(self.task.num_elements)))

        return selection, length

    def __iter__(self):
        return self.islice()

    def __getitem__(self, selection: Union[int, slice]) -> Union[Any, List[Any]]:

        selection, length = self._get_selection(selection)
        elements = self.task.workflow.get_task_elements(self.task, selection)
        if self.return_element_parameters:
            params = [
                self._app.ElementParameter(
                    task=self.task,
                    path=self.path,
                    parent=self,
                    element=i,
                    data_idx=i.get_data_idx(self.path),
                )
                for i in elements
            ]
        else:
            params = [
                i.get(
                    path=self.path,
                    raise_on_missing=self.raise_on_missing,
                    default=self.default,
                )
                for i in elements
            ]

        if length == 1:
            return params[0]
        else:
            return params


@dataclass
class TaskInputParameters:
    """For retrieving schema input parameters across all elements."""

    _app_attr = "_app"

    task: WorkflowTask

    def __getattr__(self, name):
        if name not in self._get_input_names():
            raise ValueError(f"No input named {name!r}.")
        return self._app.Parameters(self.task, f"inputs.{name}", True)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"{', '.join(f'{i!r}' for i in self._get_input_names())})"
        )

    def __dir__(self):
        return super().__dir__() + self._get_input_names()

    def _get_input_names(self):
        return sorted(self.task.template.all_schema_input_types)


@dataclass
class TaskOutputParameters:
    """For retrieving schema output parameters across all elements."""

    _app_attr = "_app"

    task: WorkflowTask

    def __getattr__(self, name):
        if name not in self._get_output_names():
            raise ValueError(f"No output named {name!r}.")
        return self._app.Parameters(self.task, f"outputs.{name}", True)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"{', '.join(f'{i!r}' for i in self._get_output_names())})"
        )

    def __dir__(self):
        return super().__dir__() + self._get_output_names()

    def _get_output_names(self):
        return sorted(self.task.template.all_schema_output_types)


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
