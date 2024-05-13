from __future__ import annotations

import copy
from typing import Dict, List, Optional, Tuple, Union

from hpcflow.sdk import app
from hpcflow.sdk.core.errors import LoopTaskSubsetError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.parameters import InputSourceType
from hpcflow.sdk.core.task import WorkflowTask
from hpcflow.sdk.core.utils import check_valid_py_identifier

# from .parameters import Parameter

# from valida.conditions import ConditionLike


# @dataclass
# class StoppingCriterion:
#     parameter: Parameter
#     condition: ConditionLike


# @dataclass
# class Loop:
#     parameter: Parameter
#     stopping_criteria: StoppingCriterion  # TODO: should be a logical combination of these (maybe provide a superclass in valida to re-use some logic there?)
#     maximum_iterations: int


class Loop(JSONLike):
    _app_attr = "app"
    _child_objects = (ChildObjectSpec(name="termination", class_name="Rule"),)

    def __init__(
        self,
        tasks: List[Union[int, app.WorkflowTask]],
        num_iterations: int,
        name: Optional[str] = None,
        non_iterable_parameters: Optional[List[str]] = None,
        termination: Optional[app.Rule] = None,
    ) -> None:
        """

        Parameters
        ----------
        name
            Loop name, optional
        tasks
            List of task insert IDs or WorkflowTask objects
        non_iterable_parameters
            Specify input parameters that should not iterate.
        termination
            Stopping criterion, expressed as a rule.

        """

        _task_insert_IDs = []
        for task in tasks:
            if isinstance(task, WorkflowTask):
                _task_insert_IDs.append(task.insert_ID)
            elif isinstance(task, int):
                _task_insert_IDs.append(task)
            else:
                raise TypeError(
                    f"`tasks` must be a list whose elements are either task insert IDs "
                    f"or WorkflowTask objects, but received the following: {tasks!r}."
                )

        self._task_insert_IDs = _task_insert_IDs
        self._num_iterations = num_iterations
        self._name = check_valid_py_identifier(name) if name else name
        self._non_iterable_parameters = non_iterable_parameters or []
        self._termination = termination

        self._workflow_template = None  # assigned by parent WorkflowTemplate

    def to_dict(self):
        out = super().to_dict()
        return {k.lstrip("_"): v for k, v in out.items()}

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""
        if "task_insert_IDs" in json_like:
            insert_IDs = json_like.pop("task_insert_IDs")
        else:
            insert_IDs = json_like.pop("tasks")
        obj = cls(tasks=insert_IDs, **json_like)
        return obj

    @property
    def task_insert_IDs(self) -> Tuple[int]:
        """Get the list of task insert_IDs that define the extent of the loop."""
        return tuple(self._task_insert_IDs)

    @property
    def name(self):
        return self._name

    @property
    def num_iterations(self):
        return self._num_iterations

    @property
    def non_iterable_parameters(self):
        return self._non_iterable_parameters

    @property
    def termination(self):
        return self._termination

    @property
    def workflow_template(self):
        return self._workflow_template

    @workflow_template.setter
    def workflow_template(self, template: app.WorkflowTemplate):
        self._workflow_template = template
        self._validate_against_template()

    @property
    def task_objects(self) -> Tuple[app.WorkflowTask]:
        if not self.workflow_template:
            raise RuntimeError(
                "Workflow template must be assigned to retrieve task objects of the loop."
            )
        return tuple(
            self.workflow_template.workflow.tasks.get(insert_ID=i)
            for i in self.task_insert_IDs
        )

    def _validate_against_template(self):
        """Validate the loop parameters against the associated workflow."""

        # insert IDs must exist:
        for insert_ID in self.task_insert_IDs:
            try:
                self.workflow_template.workflow.tasks.get(insert_ID=insert_ID)
            except ValueError:
                raise ValueError(
                    f"Loop {self.name!r} has an invalid task insert ID {insert_ID!r}. "
                    f"Such as task does not exist in the associated workflow."
                )

    def __repr__(self):
        num_iterations_str = ""
        if self.num_iterations is not None:
            num_iterations_str = f", num_iterations={self.num_iterations!r}"

        name_str = ""
        if self.name:
            name_str = f", name={self.name!r}"

        return (
            f"{self.__class__.__name__}("
            f"task_insert_IDs={self.task_insert_IDs!r}{num_iterations_str}{name_str}"
            f")"
        )

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        kwargs["tasks"] = kwargs.pop("task_insert_IDs")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._workflow_template = self._workflow_template
        return obj


class WorkflowLoop:
    """Class to represent a Loop that is bound to a Workflow."""

    _app_attr = "app"

    def __init__(
        self,
        index: int,
        workflow: app.Workflow,
        template: app.Loop,
        num_added_iterations: Dict[Tuple[int], int],
        iterable_parameters: Dict[int : List[int, List[int]]],
        parents: List[str],
    ):
        self._index = index
        self._workflow = workflow
        self._template = template
        self._num_added_iterations = num_added_iterations
        self._iterable_parameters = iterable_parameters
        self._parents = parents

        # appended to on adding a empty loop to the workflow that's a parent of this loop,
        # reset and added to `self._parents` on dump to disk:
        self._pending_parents = []

        # used for `num_added_iterations` when a new loop iteration is added, or when
        # parents are append to; reset to None on dump to disk. Each key is a tuple of
        # parent loop indices and each value is the number of pending new iterations:
        self._pending_num_added_iterations = None

        self._validate()

    def _validate(self):
        # task subset must be a contiguous range of task indices:
        task_indices = self.task_indices
        task_min, task_max = task_indices[0], task_indices[-1]
        if task_indices != tuple(range(task_min, task_max + 1)):
            raise LoopTaskSubsetError(
                f"Loop {self.name!r}: task subset must be an ascending contiguous range, "
                f"but specified task indices were: {self.task_indices!r}."
            )

        for task in self.downstream_tasks:
            for param in self.iterable_parameters:
                if param in task.template.all_schema_input_types:
                    raise NotImplementedError(
                        f"Downstream task {task.unique_name!r} of loop {self.name!r} "
                        f"has as one of its input parameters this loop's iterable "
                        f"parameter {param!r}. This parameter cannot be sourced "
                        f"correctly."
                    )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(template={self.template!r}, "
            f"num_added_iterations={self.num_added_iterations!r})"
        )

    @property
    def num_added_iterations(self):

        if self._pending_num_added_iterations:
            return self._pending_num_added_iterations
        else:
            return self._num_added_iterations

    def _initialise_pending_added_iters(self, added_iters_key):
        if not self._pending_num_added_iterations:
            self._pending_num_added_iterations = copy.deepcopy(self._num_added_iterations)

        if added_iters_key not in self._pending_num_added_iterations:
            self._pending_num_added_iterations[added_iters_key] = 1

    def _increment_pending_added_iters(self, added_iters_key):
        if not self._pending_num_added_iterations:
            self._pending_num_added_iterations = copy.deepcopy(self._num_added_iterations)

        self._pending_num_added_iterations[added_iters_key] += 1

    def _update_parents(self, parent: app.WorkflowLoop):
        self._pending_parents.append(parent.name)

        if not self._pending_num_added_iterations:
            self._pending_num_added_iterations = copy.deepcopy(self._num_added_iterations)

        self._pending_num_added_iterations = {
            tuple(list(k) + [0]): v for k, v in self._pending_num_added_iterations.items()
        }

        self.workflow._store.update_loop_parents(
            index=self.index,
            num_added_iters=self.num_added_iterations,
            parents=self.parents,
        )

    def _reset_pending_num_added_iters(self):
        self._pending_num_added_iterations = None

    def _accept_pending_num_added_iters(self):
        if self._pending_num_added_iterations:
            self._num_added_iterations = copy.deepcopy(self._pending_num_added_iterations)
            self._reset_pending_num_added_iters()

    def _reset_pending_parents(self):
        self._pending_parents = []

    def _accept_pending_parents(self):
        self._parents += self._pending_parents
        self._reset_pending_parents()

    @property
    def index(self):
        return self._index

    @property
    def task_insert_IDs(self):
        return self.template.task_insert_IDs

    @property
    def task_objects(self):
        return self.template.task_objects

    @property
    def task_indices(self) -> Tuple[int]:
        """Get the list of task indices that define the extent of the loop."""
        return tuple(i.index for i in self.task_objects)

    @property
    def workflow(self):
        return self._workflow

    @property
    def template(self):
        return self._template

    @property
    def parents(self) -> List[str]:
        return self._parents + self._pending_parents

    @property
    def name(self):
        return self.template.name

    @property
    def iterable_parameters(self):
        return self._iterable_parameters

    @property
    def num_iterations(self):
        return self.template.num_iterations

    @property
    def downstream_tasks(self) -> List[app.WorkflowLoop]:
        """Return tasks that are not part of the loop, and downstream from this loop."""
        return self.workflow.tasks[self.task_objects[-1].index + 1 :]

    @property
    def upstream_tasks(self) -> List[app.WorkflowLoop]:
        """Return tasks that are not part of the loop, and upstream from this loop."""
        return self.workflow.tasks[: self.task_objects[0].index]

    @staticmethod
    def _find_iterable_parameters(loop_template: app.Loop):
        all_inputs_first_idx = {}
        all_outputs_idx = {}
        for task in loop_template.task_objects:
            for typ in task.template.all_schema_input_types:
                if typ not in all_inputs_first_idx:
                    all_inputs_first_idx[typ] = task.insert_ID
            for typ in task.template.all_schema_output_types:
                if typ not in all_outputs_idx:
                    all_outputs_idx[typ] = []
                all_outputs_idx[typ].append(task.insert_ID)

        iterable_params = {}
        for typ, first_idx in all_inputs_first_idx.items():
            if typ in all_outputs_idx and first_idx <= all_outputs_idx[typ][0]:
                iterable_params[typ] = {
                    "input_task": first_idx,
                    "output_tasks": all_outputs_idx[typ],
                }

        for non_iter in loop_template.non_iterable_parameters:
            if non_iter in iterable_params:
                del iterable_params[non_iter]

        return iterable_params

    @classmethod
    def new_empty_loop(
        cls,
        index: int,
        workflow: app.Workflow,
        template: app.Loop,
        iterations: List[app.ElementIteration],
    ) -> Tuple[app.WorkflowLoop, List[Dict[str, int]]]:
        parent_loops = cls._get_parent_loops(index, workflow, template)
        parent_names = [i.name for i in parent_loops]
        num_added_iters = {}
        for iter_i in iterations:
            num_added_iters[tuple([iter_i.loop_idx[j] for j in parent_names])] = 1

        obj = cls(
            index=index,
            workflow=workflow,
            template=template,
            num_added_iterations=num_added_iters,
            iterable_parameters=cls._find_iterable_parameters(template),
            parents=parent_names,
        )
        return obj

    @classmethod
    def _get_parent_loops(
        cls,
        index: int,
        workflow: app.Workflow,
        template: app.Loop,
    ) -> List[app.WorkflowLoop]:
        parents = []
        passed_self = False
        self_tasks = set(template.task_insert_IDs)
        for loop_i in workflow.loops:
            if loop_i.index == index:
                passed_self = True
                continue
            other_tasks = set(loop_i.task_insert_IDs)
            if self_tasks.issubset(other_tasks):
                if (self_tasks == other_tasks) and not passed_self:
                    continue
                parents.append(loop_i)
        return parents

    def get_parent_loops(self) -> List[app.WorkflowLoop]:
        """Get loops whose task subset is a superset of this loop's task subset. If two
        loops have identical task subsets, the first loop in the workflow loop list is
        considered the child."""
        return self._get_parent_loops(self.index, self.workflow, self.template)

    def get_child_loops(self) -> List[app.WorkflowLoop]:
        """Get loops whose task subset is a subset of this loop's task subset. If two
        loops have identical task subsets, the first loop in the workflow loop list is
        considered the child."""
        children = []
        passed_self = False
        self_tasks = set(self.task_insert_IDs)
        for loop_i in self.workflow.loops:
            if loop_i.index == self.index:
                passed_self = True
                continue
            other_tasks = set(loop_i.task_insert_IDs)
            if self_tasks.issuperset(other_tasks):
                if (self_tasks == other_tasks) and passed_self:
                    continue
                children.append(loop_i)

        # order by depth, so direct child is first:
        children = sorted(children, key=lambda x: len(next(iter(x.num_added_iterations))))
        return children

    def add_iteration(self, parent_loop_indices=None):
        parent_loops = self.get_parent_loops()
        child_loops = self.get_child_loops()
        child_loop_names = [i.name for i in child_loops]
        parent_loop_indices = parent_loop_indices or {}
        if parent_loops and not parent_loop_indices:
            parent_loop_indices = {i.name: 0 for i in parent_loops}

        iters_key = tuple([parent_loop_indices[k] for k in self.parents])
        cur_loop_idx = self.num_added_iterations[iters_key] - 1
        all_new_data_idx = {}  # keys are (task.insert_ID and element.index)

        for task in self.task_objects:
            for elem_idx in range(task.num_elements):
                # element needs to take into account changes made in this code
                element = task.elements[elem_idx]
                inp_statuses = task.template.get_input_statuses(element.element_set)
                new_data_idx = {}
                existing_inners = []
                for iter_i in element.iterations:
                    if iter_i.loop_idx[self.name] == cur_loop_idx:
                        existing_inner_i = {
                            k: v
                            for k, v in iter_i.loop_idx.items()
                            if k in child_loop_names
                        }
                        if existing_inner_i:
                            existing_inners.append(existing_inner_i)

                # copy resources from zeroth iteration:
                for key, val in element.iterations[0].get_data_idx().items():
                    if key.startswith("resources."):
                        new_data_idx[key] = val

                for inp in task.template.all_schema_inputs:
                    is_inp_task = False
                    iter_dat = self.iterable_parameters.get(inp.typ)
                    if iter_dat:
                        is_inp_task = task.insert_ID == iter_dat["input_task"]

                    if is_inp_task:
                        # source from final output task of previous iteration, with all parent
                        # loop indices the same as previous iteration, and all child loop indices
                        # maximised:

                        # identify element(s) from which this iterable input should be
                        # parametrised:
                        if task.insert_ID == iter_dat["output_tasks"][-1]:
                            src_elem = element
                            grouped_elems = None
                        else:
                            src_elems = element.get_dependent_elements_recursively(
                                task_insert_ID=iter_dat["output_tasks"][-1]
                            )
                            # consider groups
                            inp_group_name = inp.single_labelled_data.get("group")
                            grouped_elems = []
                            for i in src_elems:
                                i_in_group = any(
                                    j.name == inp_group_name for j in i.element_set.groups
                                )
                                if i_in_group:
                                    grouped_elems.append(i)

                            if not grouped_elems and len(src_elems) > 1:
                                raise NotImplementedError(
                                    f"Multiple elements found in the iterable parameter {inp!r}'s"
                                    f" latest output task (insert ID: "
                                    f"{iter_dat['output_tasks'][-1]}) that can be used to "
                                    f"parametrise the next iteration: {src_elems!r}."
                                )

                            elif not src_elems:
                                # TODO: maybe OK?
                                raise NotImplementedError(
                                    f"No elements found in the iterable parameter {inp!r}'s"
                                    f" latest output task (insert ID: "
                                    f"{iter_dat['output_tasks'][-1]}) that can be used to "
                                    f"parametrise the next iteration."
                                )

                            else:
                                src_elem = src_elems[0]

                        child_loop_max_iters = {}
                        parent_loop_same_iters = {
                            i.name: parent_loop_indices[i.name] for i in parent_loops
                        }
                        child_iter_parents = {
                            **parent_loop_same_iters,
                            self.name: cur_loop_idx,
                        }
                        for i in child_loops:
                            i_num_iters = i.num_added_iterations[
                                tuple(child_iter_parents[j] for j in i.parents)
                            ]
                            i_max = i_num_iters - 1
                            child_iter_parents[i.name] = i_max
                            child_loop_max_iters[i.name] = i_max

                        source_iter_loop_idx = {
                            **child_loop_max_iters,
                            **parent_loop_same_iters,
                            self.name: cur_loop_idx,
                        }

                        # identify the ElementIteration from which this input should be
                        # parametrised:
                        source_iter = None
                        if grouped_elems:
                            source_iter = []
                            for src_elem in grouped_elems:
                                for iter_i in src_elem.iterations:
                                    if iter_i.loop_idx == source_iter_loop_idx:
                                        source_iter.append(iter_i)
                                        break
                        else:
                            for iter_i in src_elem.iterations:
                                if iter_i.loop_idx == source_iter_loop_idx:
                                    source_iter = iter_i
                                    break

                        if not source_iter:
                            raise RuntimeError(
                                f"Could not find a source iteration with loop_idx: "
                                f"{source_iter_loop_idx!r}."
                            )

                        if grouped_elems:
                            inp_dat_idx = [
                                i.get_data_idx()[f"outputs.{inp.typ}"]
                                for i in source_iter
                            ]
                        else:
                            inp_dat_idx = source_iter.get_data_idx()[f"outputs.{inp.typ}"]
                        new_data_idx[f"inputs.{inp.typ}"] = inp_dat_idx

                    else:
                        inp_key = f"inputs.{inp.typ}"

                        orig_inp_src = element.input_sources[inp_key]
                        inp_dat_idx = None

                        if orig_inp_src.source_type is InputSourceType.LOCAL:
                            # keep locally defined inputs from original element
                            inp_dat_idx = element.iterations[0].get_data_idx()[inp_key]

                        elif orig_inp_src.source_type is InputSourceType.DEFAULT:
                            # keep default value from original element
                            inp_dat_idx_iter_0 = element.iterations[0].get_data_idx()
                            try:
                                inp_dat_idx = inp_dat_idx_iter_0[inp_key]
                            except KeyError:
                                # if this input is required by a conditional action, and
                                # that condition is not met, then this input will not
                                # exist in the action-run data index, so use the initial
                                # iteration data index:
                                inp_dat_idx = element.iterations[0].data_idx[inp_key]

                        elif orig_inp_src.source_type is InputSourceType.TASK:
                            if orig_inp_src.task_ref not in self.task_insert_IDs:
                                # source task not part of the loop; copy existing data idx:
                                inp_dat_idx = element.iterations[0].get_data_idx()[
                                    inp_key
                                ]
                            else:
                                is_group = False
                                if (
                                    not inp.multiple
                                    and "group" in inp.single_labelled_data
                                ):
                                    # this input is a group, assume for now all elements:
                                    is_group = True

                                # same task/element, but update iteration to the just-added
                                # iteration:
                                key_prefix = orig_inp_src.task_source_type.name.lower()
                                prev_dat_idx_key = f"{key_prefix}s.{inp.typ}"
                                new_sources = []
                                for (
                                    tiID,
                                    e_idx,
                                ), prev_dat_idx in all_new_data_idx.items():
                                    if tiID == orig_inp_src.task_ref:
                                        # find which element in that task `element`
                                        # depends on:
                                        task_i = self.workflow.tasks.get(insert_ID=tiID)
                                        elem_i = task_i.elements[e_idx]
                                        src_elems_i = (
                                            elem_i.get_dependent_elements_recursively(
                                                task_insert_ID=task.insert_ID
                                            )
                                        )
                                        # filter src_elems_i for matching element IDs:
                                        src_elems_i = [
                                            i for i in src_elems_i if i.id_ == element.id_
                                        ]
                                        if (
                                            len(src_elems_i) == 1
                                            and src_elems_i[0].id_ == element.id_
                                        ):
                                            new_sources.append((tiID, e_idx))

                                if is_group:
                                    inp_dat_idx = [
                                        all_new_data_idx[i][prev_dat_idx_key]
                                        for i in new_sources
                                    ]
                                else:
                                    assert len(new_sources) == 1
                                    prev_dat_idx = all_new_data_idx[new_sources[0]]
                                    inp_dat_idx = prev_dat_idx[prev_dat_idx_key]

                        if inp_dat_idx is None:
                            raise RuntimeError(
                                f"Could not find a source for parameter {inp.typ} "
                                f"when adding a new iteration for task {task!r}."
                            )

                        new_data_idx[inp_key] = inp_dat_idx

                # add any locally defined sub-parameters:
                inp_status_inps = set([f"inputs.{i}" for i in inp_statuses])
                sub_params = inp_status_inps - set(new_data_idx.keys())
                for sub_param_i in sub_params:
                    sub_param_data_idx_iter_0 = element.iterations[0].get_data_idx()
                    try:
                        sub_param_data_idx = sub_param_data_idx_iter_0[sub_param_i]
                    except KeyError:
                        # as before, if this input is required by a conditional action,
                        # and that condition is not met, then this input will not exist in
                        # the action-run data index, so use the initial iteration data
                        # index:
                        sub_param_data_idx = element.iterations[0].data_idx[sub_param_i]

                    new_data_idx[sub_param_i] = sub_param_data_idx

                for out in task.template.all_schema_outputs:
                    path_i = f"outputs.{out.typ}"
                    p_src = {"type": "EAR_output"}
                    new_data_idx[path_i] = self.workflow._add_unset_parameter_data(p_src)

                schema_params = set(
                    i for i in new_data_idx.keys() if len(i.split(".")) == 2
                )
                all_new_data_idx[(task.insert_ID, element.index)] = new_data_idx

                new_loop_idx = {
                    **parent_loop_indices,
                    self.name: cur_loop_idx + 1,
                    **{
                        child.name: 0
                        for child in child_loops
                        if task.insert_ID in child.task_insert_IDs
                    },
                }
                # increment num_added_iterations on child loop for this parent loop index:
                for i in child_loops:
                    added_iters_key_chd = tuple([new_loop_idx[j] for j in i.parents])
                    i._initialise_pending_added_iters(added_iters_key_chd)

                iter_ID_i = self.workflow._store.add_element_iteration(
                    element_ID=element.id_,
                    data_idx=new_data_idx,
                    schema_parameters=list(schema_params),
                    loop_idx=new_loop_idx,
                )

                task.initialise_EARs()

        added_iters_key = tuple(parent_loop_indices[k] for k in self.parents)
        self._increment_pending_added_iters(added_iters_key)
        self.workflow._store.update_loop_num_iters(
            index=self.index,
            num_added_iters=self.num_added_iterations,
        )

        # add iterations to fixed-number-iteration children only:
        for child in child_loops[::-1]:
            if child.num_iterations is not None:
                for _ in range(child.num_iterations - 1):
                    par_idx = {k: 0 for k in child.parents}
                    child.add_iteration(
                        parent_loop_indices={
                            **par_idx,
                            **parent_loop_indices,
                            self.name: cur_loop_idx + 1,
                        }
                    )

    def test_termination(self, element_iter):
        """Check if a loop should terminate, given the specified completed element
        iteration."""
        if self.template.termination:
            return self.template.termination.test(element_iter)
        return False
