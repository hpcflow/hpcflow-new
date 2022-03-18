from dataclasses import dataclass
from optparse import Option
from typing import Dict, List, Optional, Tuple, Union
from hpcflow.command_files import FileSpec

from hpcflow.element import Element, ElementFilter, ElementGroup
from hpcflow.errors import (
    MissingInputs,
    TaskTemplateInvalidNesting,
    TaskTemplateMultipleInputValues,
    TaskTemplateMultipleSchemaObjectives,
    TaskTemplateUnexpectedInput,
)
from hpcflow.object_list import GroupList, index
from hpcflow.parameters import (
    InputSource,
    InputValue,
    ParameterPath,
    SchemaInput,
    SchemaOutput,
    ValuePerturbation,
    ValueSequence,
)
from hpcflow.task_schema import TaskSchema
from hpcflow.typing_stubs import Workflow, WorkflowTemplate
from hpcflow.utils import get_duplicate_items


class TaskTemplate:
    """Parametrisation of an isolated task for which a subset of input values are given
    "locally". The remaining input values are expected to be satisfied by other
    tasks/imports in the workflow."""

    __slots__ = (
        "_schemas",
        "_repeats",
        "_resources",
        "_inputs",
        "_input_files",
        "_input_file_generator_sources",
        "_output_file_parser_sources",
        "_perturbations",
        "_sequences",
        "_input_sources",
        "_nesting_order",
        "_groups",
        "_name",
        "_defined_input_types",  # assigned in _validate()
    )

    def __init__(
        self,
        schemas: Union[List[TaskSchema], TaskSchema],
        repeats: Optional[Union[int, List[int]]] = 1,
        resources: Optional[Dict[str, Dict]] = None,
        inputs: Optional[List[InputValue]] = None,
        input_files: Optional[List[FileSpec]] = None,
        input_file_generator_sources: Optional[List] = None,
        output_file_parser_sources: Optional[List] = None,
        perturbations: Optional[List[ValuePerturbation]] = None,
        sequences: Optional[List[ValueSequence]] = None,
        input_sources: Optional[Dict[str, InputSource]] = None,
        nesting_order: Optional[Dict] = None,
        groups: Optional[List[ElementGroup]] = None,
    ):
        self._schemas = schemas if isinstance(schemas, list) else [schemas]
        self._repeats = repeats
        self._resources = resources or {
            "main": {}
        }  # TODO: use action names from schemas
        self._inputs = inputs or []
        self._input_files = input_files or []
        self._input_file_generator_sources = input_file_generator_sources or []
        self._output_file_parser_sources = output_file_parser_sources or []
        self._perturbations = perturbations or []
        self._sequences = sequences or []
        self._input_sources = input_sources or {}
        self._nesting_order = nesting_order or {}
        self._groups = GroupList(*(groups or ()))

        print(f"tasktemplate init nesting_order: {nesting_order}")

        self._validate()
        self._name = self._get_name()

    def _validate(self):

        # TODO: check a nesting order specified for each sequence?

        names = set(i.objective.name for i in self.schemas)
        if len(names) > 1:
            raise TaskTemplateMultipleSchemaObjectives(
                f"All task schemas used within a task must have the same "
                f"objective, but found multiple objectives: {list(names)!r}"
            )

        input_types = [
            i.parameter.typ for i in self.get_non_sub_parameter_input_values()
        ]
        dup_params = get_duplicate_items(input_types)
        if dup_params:
            raise TaskTemplateMultipleInputValues(
                f"The following parameters are associated with multiple input value "
                f"definitions: {dup_params!r}."
            )

        unexpected_types = set(input_types) - self.all_schema_input_types
        if unexpected_types:
            raise TaskTemplateUnexpectedInput(
                f"The following input parameters are unexpected: {list(unexpected_types)!r}"
            )

        for k, v in self.nesting_order.items():
            if v < 0:
                raise TaskTemplateInvalidNesting(
                    f"`nesting_order` must be >=0 for all keys, but for key {k!r}, value "
                    f"of {v!r} was specified."
                )

        self._defined_input_types = set(input_types)

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

    @classmethod
    def from_spec(cls, spec, all_schemas, all_parameters):
        key = (
            spec.pop("objective"),
            spec.pop("method", None),
            spec.pop("implementation", None),
        )  # TODO: maybe don't mutate spec?
        spec["schemas"] = all_schemas[
            key
        ]  # TODO parse multiple methods/impl not multiple schemas

        sequences = spec.pop("sequences", [])
        inputs_spec = spec.pop("inputs", [])
        input_sources_spec = spec.pop("input_sources", {})
        perturbs_spec = spec.pop("perturbations", {})
        nesting_order = spec.pop("nesting_order", {})

        for k in list(nesting_order.keys()):
            new_k = tuple(k.split("."))
            nesting_order[new_k] = nesting_order.pop(k)

        print(f"nesting_order: {nesting_order}")

        inputs = []
        if isinstance(inputs_spec, dict):
            # transform to a list:
            for input_path, input_val in inputs_spec.items():
                is_sequence = input_path.endswith("[]")
                if is_sequence:
                    input_path = input_path.split("[]")[0]
                input_path = input_path.split(".")
                inputs.append(
                    {
                        "parameter": input_path[0],
                        "path": input_path[1:],
                        "value": input_val[0] if is_sequence else input_val,
                    }
                )
                if is_sequence:
                    seq_path = ["inputs"] + input_path
                    sequences.append(
                        {
                            "path": seq_path,
                            "values": input_val,
                            "nesting_order": nesting_order.get(tuple(seq_path)),
                        }
                    )
        else:
            inputs = inputs_spec

        # add any nesting orders that are defined in the sequences:
        for i in sequences:
            if "nesting_order" in i:
                nesting_order.update({tuple(i["path"]): i["nesting_order"]})

        perturbs = [{"name": p_name, **p} for p_name, p in perturbs_spec.items()]

        spec.update(
            {
                "inputs": [InputValue.from_spec(i, all_parameters) for i in inputs],
                "sequences": [ValueSequence.from_spec(i) for i in sequences],
                "perturbations": [ValuePerturbation.from_spec(i) for i in perturbs],
                "nesting_order": nesting_order,
                "input_sources": {
                    i: [InputSource.from_spec(j) for j in sources]
                    for i, sources in input_sources_spec.items()
                },
            }
        )

        return cls(**spec)

    @property
    def schemas(self):
        return self._schemas

    @property
    def repeats(self):
        return self._repeats

    @property
    def resources(self):
        return self._resources

    @property
    def inputs(self):
        return self._inputs

    @property
    def input_files(self):
        return self._input_files

    @property
    def input_file_generator_sources(self):
        return self._input_file_generator_sources

    @property
    def output_file_parser_sources(self):
        return self._output_file_parser_sources

    @property
    def perturbations(self):
        return self._perturbations

    @property
    def sequences(self):
        return self._sequences

    @property
    def input_sources(self):
        return self._input_sources

    @property
    def nesting_order(self):
        return self._nesting_order

    @property
    def groups(self):
        return self._groups

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
        group = ElementGroup(
            name=name, where=where, group_by_distinct=group_by_distinct
        )
        self.groups.add_object(group)

    def get_input_multiplicities(self, missing_multiplicities=None):
        """Get multiplicities for all inputs."""

        if self.undefined_input_types:
            missing_inputs = self.undefined_input_types - set(
                (missing_multiplicities or {}).keys()
            )
            if missing_inputs:
                raise MissingInputs(
                    f"The following inputs are not assigned values, so task input "
                    f"multiplicities cannot be resolved: {list(missing_inputs)!r}."
                )

        input_multiplicities = []
        for i in self.input_values:
            if i.sequences:
                for seq in i.sequences:
                    address = tuple([i.parameter.typ] + seq.address)
                    address_str = ".".join(address)
                    input_multiplicities.append(
                        {
                            "address": address,
                            "multiplicity": len(seq.values),
                            "nesting_order": self.nesting_order[address_str],
                        }
                    )
            else:
                input_multiplicities.append(
                    {
                        "address": (i.parameter.typ,),
                        "multiplicity": 1,
                        "nesting_order": -1,
                    }
                )

        for inp_type, multi in (missing_multiplicities or {}).items():
            input_multiplicities.append(
                {
                    "address": (inp_type,),
                    "multiplicity": multi,
                    "nesting_order": self.nesting_order[inp_type],
                }
            )

        return input_multiplicities


class Task:
    """Class to represent a Task as positioned within a Workflow or WorkflowTemplate."""

    def __init__(
        self,
        template: TaskTemplate,
        workflow: Union[Workflow, WorkflowTemplate],
        initial_index: int,
    ):

        self._template = template
        self._workflow = workflow
        self._initial_index = initial_index

    @property
    def template(self):
        return self._template

    @property
    def workflow(self):
        return self._workflow

    @property
    def element_indices(self):
        return self.workflow.element_indices[self.index]

    @property
    def num_elements(self):
        return len(self.element_indices)

    @property
    def index(self):
        """Zero-based position within the workflow. Uses initial index if appending to the
        workflow is not complete."""
        try:
            return index(self.workflow.tasks, self)
        except ValueError:
            return self._initial_index

    @property
    def unique_name(self):
        name_repeat_index = self.workflow.name_repeat_indices[self.index]
        add_rep = f"{'_' + name_repeat_index if name_repeat_index > 1 else ''}"
        return f"{self.template.name}{add_rep}"
