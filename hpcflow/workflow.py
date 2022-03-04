from dataclasses import dataclass
from operator import itemgetter
from typing import List, Optional
from hpcflow.loop import Loop

from hpcflow.object_list import TaskList
from hpcflow.parameters import (
    InputSource,
    ParameterPropagationMode,
    SchemaInput,
    ValueSequence,
)
from hpcflow.task import Task, TaskTemplate
from hpcflow.utils import (
    get_in_container,
    get_relative_path,
    group_by_dict_key_values,
    set_in_container,
)


class WorkflowTemplate:
    def __init__(
        self,
        task_templates: Optional[List[TaskTemplate]] = None,
        loops: Optional[List[Loop]] = None,
    ):

        self.parameter_data = []
        self.parameter_mapping = []
        self.elements = []
        self.tasks = TaskList()
        self.element_indices = []
        self.name_repeat_indices = []

        for task_template in task_templates or []:
            self.add_task(task_template)

    def get_possible_input_sources(
        self, schema_input: SchemaInput, new_task: TaskTemplate, new_index: int
    ):
        """Enumerate the possible sources for an input of a new task, given a proposed
        placement of that task."""

        # Get parameters provided by tasks up to `new_index`:
        task_sources = {}
        for task in self.tasks[:new_index]:
            provided = tuple(
                i
                for i in task.template.provides_parameters
                if i.typ == schema_input.typ
            )
            if provided:
                task_sources.update({(task.index, task.unique_name): provided})

        out = {
            "imports": {},
            "tasks": task_sources,
            "has_local": schema_input.typ in new_task.defined_input_types,
            # TODO: there *might* be local definition of a parameter in the form of the input files/writers specified?
            "has_default": schema_input.default_value is not None,
        }
        return out

    def ensure_input_sources(self, new_task: TaskTemplate, new_index: int):
        """Check valid input sources are specified for a new task to be added to the
        workflow in a given position. If none are specified, set them according to the
        default behaviour."""

        all_sources = {}
        for schema_input in new_task.all_schema_inputs:

            all_sources.update(
                {
                    schema_input.typ: self.get_possible_input_sources(
                        schema_input, new_task, new_index
                    )
                }
            )

            # if sources are specified, check they are resolvable:
            for specified_source in new_task.input_sources.get(schema_input.typ, []):
                specified_source.validate(schema_input, new_task, self)

        print(f"\nall_sources: {all_sources}")
        print(f"new_task.unsourced_inputs: {new_task.unsourced_inputs}")

        # if an input is not specified at all in the `inputs` dict (what about when list?),
        # then check if there is an input files entry for associated inputs,
        # if there is

        # set source for any unsourced inputs:
        for input_type in new_task.unsourced_inputs:
            inp_i_sources = all_sources[input_type]

            print(f"sources: {inp_i_sources}")

            # input may not be required

            # set source for this input:
            if inp_i_sources["has_local"]:
                new_sources = [InputSource("local")]

            elif inp_i_sources["tasks"]:
                # we can only take parameters with implicit propagation mode:
                params_info = []
                for (task_idx, task_name), params in inp_i_sources["tasks"].items():
                    for i in params:
                        if i.propagation_mode == ParameterPropagationMode.IMPLICIT:
                            params_info.append((i.input_or_output, task_idx, task_name))

                # sort by output/input (output first), then by task index (highest first):
                params_info = sorted(
                    params_info, key=itemgetter(0, 1), reverse=True
                )  # TODO: TEST THIS!!!
                new_sources = [
                    InputSource(f"tasks.{params_info[0][2]}.{params_info[0][0]}s")
                ]

            else:
                # input may not need defining (if all associated input files are passed
                # and the input does not appear in any action commands)
                new_sources = None

            new_task.input_sources.update({input_type: new_sources})

    def add_task(self, task_template: TaskTemplate):

        # TODO: can't do this check yet because required inputs of different elements may be different
        # e.g. if an input file is passed for some elements but not others.

        self.ensure_input_sources(
            task_template, len(self.tasks)
        )  # modifies task_template.input_sources
        # at this point the source for each input should be decided and well-defined.

        element_indices = []

        add_sequences = [  # treat the base inputs and resources as single-item sequences:
            ValueSequence(
                path=["inputs"],
                values=[
                    {
                        param.parameter.typ: param.value
                        for param in task_template.get_non_sub_parameter_input_values()
                    }
                ],
                nesting_order=-1,
            ),
            ValueSequence(
                path=["resources"], values=[task_template.resources], nesting_order=-1
            ),
        ]  # treat sub-parameter input values as single-item sequences
        for i in task_template.get_sub_parameter_input_values():
            add_sequences.append(
                ValueSequence(
                    path=["inputs", i.parameter.typ] + i.path,
                    values=[i.value],
                    nesting_order=-1,
                )
            )

        multi = []
        input_map_indices = {}

        sequences = add_sequences + task_template.sequences
        for i in sequences:
            # add each sequence data:
            next_param_idx = len(self.parameter_data)
            num_values = len(i.values)
            self.parameter_data.extend([{"is_set": True, "data": j} for j in i.values])
            self.parameter_mapping.append(
                list(range(next_param_idx, next_param_idx + num_values))
            )
            param_map_idx = len(self.parameter_mapping) - 1
            input_map_indices[tuple(i.path)] = param_map_idx
            nesting_order_i = (
                task_template.nesting_order[tuple(i.path)] if num_values > 1 else -1
            )
            multi.append(
                {
                    "multiplicity": num_values,
                    "nesting_order": nesting_order_i,
                    "address": i.path,
                }
            )

        init_multi = WorkflowTemplate.resolve_initial_elements(multi)
        output_map_indices = {}
        num_elems = len(init_multi)
        for schema in task_template.schemas:
            for output in schema.outputs:
                next_dat_idx = len(self.parameter_data)
                next_map_idx = len(self.parameter_mapping)
                out_data = [{"is_set": False, "data": None} for _ in range(num_elems)]
                out_param_map = list(range(next_dat_idx, next_dat_idx + num_elems))
                self.parameter_data.extend(out_data)
                self.parameter_mapping.append(out_param_map)
                output_map_indices[output.typ] = next_map_idx

        for i_idx, i in enumerate(init_multi):
            element_indices.append(len(self.elements))
            self.elements.append(
                {
                    "inputs": [
                        {
                            "path": k,
                            "parameter_mapping_index": input_map_indices[tuple(k)],
                            "data_index": v,
                        }
                        for k, v in i["value_index"].items()
                    ],
                    "outputs": [
                        {
                            "path": ("outputs", k),
                            "parameter_mapping_index": v,
                            "data_index": i_idx,
                        }
                        for k, v in output_map_indices.items()
                    ],
                }
            )

        self.element_indices.append(element_indices)
        self.name_repeat_indices.append(
            sum(i.template.name == task_template.name for i in self.tasks) + 1
        )
        task = Task(task_template, self, len(self.tasks))
        self.tasks.add_object(task)

    @staticmethod
    def resolve_initial_elements(multi):
        """
        Parameters
        ----------
        multi : list of dict
            Each list item represents a sequence of values with keys:
                multiplicity: int
                nesting_order: int
                address : str
        """

        # order by nesting order (so lower nesting orders will be fastest-varying):
        multi_srt = sorted(multi, key=lambda x: x["nesting_order"])
        multi_srt_grp = group_by_dict_key_values(multi_srt, "nesting_order")

        elements = [{"value_index": {}}]
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
                for element in elements:
                    new_elements.append(
                        {
                            "value_index": {
                                **element["value_index"],
                                **{i["address"]: val_idx for i in para_sequences},
                            }
                        }
                    )
            elements = new_elements

        return elements

    def add_task_after(self, task):
        pass

    def add_task_before(self, task):
        pass

    def remove_task(self, task):
        pass

    def get_input_values(self, task_index, parameter_path):
        """Get the value of an input for each element in a task."""
        return [
            self.get_input_value(
                task_index=task_index, element_index=i, parameter_path=parameter_path
            )
            for i in range(self.tasks[task_index].num_elements)
        ]

    def get_input_value(self, task_index, element_index, parameter_path):

        element = self.elements[self.tasks[task_index].element_indices[element_index]]
        current_value = None
        for input_i in element["inputs"]:

            param_data_idx = self.parameter_mapping[input_i["parameter_mapping_index"]][
                input_i["data_index"]
            ]

            is_parent = False
            is_update = False
            try:
                rel_path_parts = get_relative_path(parameter_path, input_i["path"])
                is_parent = True
            except ValueError:
                try:
                    update_path = get_relative_path(input_i["path"], parameter_path)
                    is_update = True
                except ValueError:
                    pass

            if is_parent:
                # replace current value:
                final_data_path = (param_data_idx, "data", *rel_path_parts)
                try:
                    current_value = get_in_container(
                        self.parameter_data, final_data_path
                    )  # or use Zarr to get from persistent
                except TypeError:
                    # import traceback

                    # traceback.print_exc()
                    pass

            elif is_update:
                # update sub-part of current value
                update_data = self.parameter_data[param_data_idx][
                    "data"
                ]  # or use Zarr to get from persistent
                set_in_container(current_value, update_path, update_data)

        return current_value

    def make_workflow(self, path):
        # TODO: make the workflow and save to path
        wk = Workflow(path)
        return wk

    @classmethod
    def from_spec(cls, spec, all_schemas, all_parameters):

        # initialise task templates:
        tasks = []
        for i in spec.pop("tasks"):
            tasks.append(TaskTemplate.from_spec(i, all_schemas, all_parameters))
        spec["task_templates"] = tasks

        return cls(**spec)


@dataclass
class Workflow:
    tasks: List[Task]

    def rename(self, new_name):
        pass

    def add_submission(self, filter):
        pass


@dataclass
class WorkflowBlueprint:
    """For pre-built workflows that are simpler to parametrise (e.g. fitting workflows)."""

    workflow_template: WorkflowTemplate
