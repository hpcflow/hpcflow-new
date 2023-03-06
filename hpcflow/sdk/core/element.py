from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from valida.conditions import ConditionLike

from hpcflow.sdk.core.utils import check_valid_py_identifier


class _ElementPrefixedParameter:

    _app_attr = "_app"

    def __init__(
        self,
        prefix: str,
        element: Optional[Element] = None,
        element_action: Optional[ElementAction] = None,
        element_action_run: Optional[ElementActionRun] = None,
    ) -> None:

        self._prefix = prefix
        self._element = element
        self._element_action = element_action
        self._element_action_run = element_action_run

    @property
    def _parent(self):
        return self._element or self._element_action or self._element_action_run

    @property
    def _element_obj(self):
        return (
            self._element
            or self._element_action.element
            or self._element_action_run.element
        )

    @property
    def _task(self):
        return self._parent.task

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"{', '.join(f'{i!r}' for i in self._get_prefixed_names())})"
        )

    def _get_prefixed_names(self):
        return sorted(self._parent.get_parameter_names(self._prefix))

    def __getattr__(self, name):
        if name not in self._get_prefixed_names():
            raise ValueError(f"No {self._prefix} named {name!r}.")

        data_idx = self._parent.get_data_idx(path=f"{self._prefix}.{name}")
        param = self._app.ElementParameter(
            path=f"{self._prefix}.{name}",
            task=self._task,
            data_idx=data_idx,
            parent=self._parent,
            element=self._element_obj,
        )
        return param

    def __dir__(self):
        return super().__dir__() + self._get_prefixed_names()


class ElementInputs(_ElementPrefixedParameter):
    def __init__(
        self,
        element: Optional[Element] = None,
        element_action: Optional[ElementAction] = None,
        element_action_run: Optional[ElementActionRun] = None,
    ) -> None:
        super().__init__("inputs", element, element_action, element_action_run)


class ElementOutputs(_ElementPrefixedParameter):
    def __init__(
        self,
        element: Optional[Element] = None,
        element_action: Optional[ElementAction] = None,
        element_action_run: Optional[ElementActionRun] = None,
    ) -> None:
        super().__init__("outputs", element, element_action, element_action_run)


class ElementInputFiles(_ElementPrefixedParameter):
    def __init__(
        self,
        element: Optional[Element] = None,
        element_action: Optional[ElementAction] = None,
        element_action_run: Optional[ElementActionRun] = None,
    ) -> None:
        super().__init__("input_files", element, element_action, element_action_run)


class ElementOutputFiles(_ElementPrefixedParameter):
    def __init__(
        self,
        element: Optional[Element] = None,
        element_action: Optional[ElementAction] = None,
        element_action_run: Optional[ElementActionRun] = None,
    ) -> None:
        super().__init__("output_files", element, element_action, element_action_run)


@dataclass
class ElementResources:
    scratch: str
    num_cores: int

    def __post_init__(self):
        if self.num_cores is None:
            self.num_cores = 1


class Element:

    _app_attr = "app"

    # TODO: use slots

    def __init__(
        self,
        task: WorkflowTask,
        index: int,
        actions: List[Dict],
        global_idx: int,
        es_idx: int,
        seq_idx: Dict,
        schema_parameters: List[str],
        loop_idx: Optional[Dict],
    ) -> None:

        self._task = task
        self._global_idx = global_idx
        self._index = index
        self._es_idx = es_idx
        self._seq_idx = seq_idx
        self._loop_idx = loop_idx
        self._schema_parameters = schema_parameters
        self._actions = actions

        # assigned on first access of corresponding properties:
        self._inputs = None
        self._outputs = None
        self._input_files = None
        self._output_files = None
        self._action_objs = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"task={self.task.unique_name!r}, index={self.index!r}"
            f")"
        )

    @property
    def task(self) -> WorkflowTask:
        return self._task

    @property
    def global_idx(self) -> int:
        return self._global_idx

    @property
    def index(self) -> int:
        """Get the index of the element within the task.

        Note: the `global_idx` attribute returns the index of the element within the
        workflow, across all tasks."""

        return self._index

    @property
    def element_set_idx(self) -> int:
        return self._es_idx

    @property
    def element_set(self):
        return self.task.template.element_sets[self.element_set_idx]

    @property
    def sequence_idx(self) -> Dict[str, int]:
        return self._seq_idx

    @property
    def loop_idx(self) -> Dict[str, int]:
        return self._loop_idx

    @property
    def schema_parameters(self) -> List[str]:
        return self._schema_parameters

    @property
    def actions(self) -> Dict[ElementAction]:
        if self._action_objs is None:
            self._action_objs = {
                act_idx: self.app.ElementAction(
                    element=self,
                    action_idx=act_idx,
                    runs=runs,
                )
                for act_idx, runs in self._actions.items()
            }
        return self._action_objs

    @property
    def action_runs(self) -> List[ElementActionRun]:
        """Get a list of element action runs, where only the final run is taken for each
        element action."""
        return [i.runs[-1] for i in self.actions.values()]

    @property
    def workflow(self) -> Workflow:
        return self.task.workflow

    @property
    def inputs(self) -> ElementInputs:
        if not self._inputs:
            self._inputs = self.app.ElementInputs(element=self)
        return self._inputs

    @property
    def outputs(self) -> ElementOutputs:
        if not self._outputs:
            self._outputs = self.app.ElementOutputs(element=self)
        return self._outputs

    @property
    def input_files(self) -> ElementInputFiles:
        if not self._input_files:
            self._input_files = self.app.ElementInputFiles(element=self)
        return self._input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        if not self._output_files:
            self._output_files = self.app.ElementOutputFiles(element=self)
        return self._output_files

    @property
    def dir_name(self):
        return str(self.index)

    @property
    def dir_path(self):
        return self.task.dir_path / self.dir_name

    def to_element_set_data(self):
        """Generate lists of workflow-bound InputValues and ResourceList."""
        inputs = []
        resources = []
        for k, v in self.get_data_idx().items():

            k_s = k.split(".")

            if k_s[0] == "inputs":
                inp_val = self.app.InputValue(
                    parameter=k_s[1],
                    path=k_s[2:] or None,
                    value=None,
                )
                inp_val._value_group_idx = v
                inp_val._workflow = self.workflow
                inputs.append(inp_val)

            elif k_s[0] == "resources":
                scope = self.app.ActionScope.from_json_like(k_s[1])
                res = self.app.ResourceSpec(scope=scope)
                res._value_group_idx = v
                res._workflow = self.workflow
                resources.append(res)

        return inputs, resources

    def get_sequence_value(self, sequence_path: str) -> Any:
        seq = self.element_set.get_sequence_from_path(sequence_path)
        if not seq:
            raise ValueError(
                f"No sequence with path {sequence_path!r} in this element's originating "
                f"element set."
            )
        return seq.values[self.sequence_idx[sequence_path]]

    def get_parameter_names(self, prefix: str) -> List[str]:
        return list(
            ".".join(i.split(".")[1:])
            for i in self.schema_parameters
            if i.startswith(prefix)
        )

    # def _path_to_parameter(self, path):
    #     TODO: remove
    #     if len(path) != 2 or path[0] == "resources":
    #         return

    #     if path[0] == "inputs":
    #         for i in self.task.template.schemas:
    #             for j in i.inputs:
    #                 if j.parameter.typ == path[1]:
    #                     return j.parameter

    #     elif path[0] == "outputs":
    #         for i in self.task.template.schemas:
    #             for j in i.outputs:
    #                 if j.parameter.typ == path[1]:
    #                     return j.parameter

    def get_data_idx(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
    ) -> Dict[str, int]:
        """
        Parameters
        ----------
        action_idx
            The index of the action within the schema.
        """

        if action_idx is None:
            # default behaviour if no action_idx is specified: inputs should be from first
            # action where that input is defined; outputs should include modifications
            # from all actions. TODO: what about resources?
            data_idx = {}
            for action in self.actions.values():
                data_idx.update(action.runs[run_idx].data_idx)
                if path and "inputs" in path and path in data_idx:
                    break
        else:
            elem_act = self.actions[action_idx]
            data_idx = elem_act.runs[run_idx].data_idx

        if path:
            data_idx = {k: v for k, v in data_idx.items() if k.startswith(path)}

        return data_idx

    def get_parameter_sources(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
        typ: str = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> Dict[str, Union[str, Dict[str, Any]]]:
        """
        Parameters
        ----------
        use_task_index
            If True, use the task index within the workflow, rather than the task insert
            ID.
        """
        data_idx = self.get_data_idx(path, action_idx, run_idx)
        out = {k: self.workflow._get_parameter_source(v) for k, v in data_idx.items()}
        task_key = "task_insert_ID"

        if use_task_index:
            task_key = "task_idx"
            out_task_idx = {}
            for k, v in out.items():
                insert_ID = v.pop("task_insert_ID", None)
                if insert_ID is not None:
                    v[task_key] = self.workflow.tasks.get(insert_ID=insert_ID).index
                out_task_idx[k] = v
            out = out_task_idx

        if typ:
            out = {k: v for k, v in out.items() if v["type"] == typ}

        if as_strings:
            # format as a dict with compact string values
            self_task_val = (
                self.task.index if task_key == "task_idx" else self.task.insert_ID
            )
            out_strs = {}
            for k, v in out.items():
                if v["type"] == "local_input":
                    if v[task_key] == self_task_val:
                        out_strs[k] = "local"
                    else:
                        out_strs[k] = f"task.{v[task_key]}.input"
                elif v["type"] == "default_input":
                    out_strs == "default"
                else:
                    out_strs[k] = (
                        f"task.{v[task_key]}.element.{v['element_idx']}."
                        f"action.{v['action_idx']}.run.{v['run_idx']}"
                    )
            out = out_strs

        return out

    def get(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
        default: Any = None,
        raise_on_missing: bool = False,
    ) -> Any:
        """Get element data from the persistent store."""
        # TODO include a "stats" parameter which when set we know the run has been
        # executed (or if start time is set but not end time, we know it's running or
        # failed.)
        return self.task._get_merged_parameter_data(
            data_index=self.get_data_idx(action_idx=action_idx, run_idx=run_idx),
            path=path,
            raise_on_missing=raise_on_missing,
            default=default,
        )

    def get_EAR_dependencies(
        self,
        as_objects: Optional[bool] = False,
    ) -> List[Union[Tuple[int, int, int, int], ElementActionRun]]:
        """Get EARs that this element depends on (excluding EARs of this element)."""
        out = sorted(
            set(
                j
                for i in self.action_runs
                for j in i.get_EAR_dependencies(as_objects=False)
                if not (j[0] == self.task.insert_ID and j[1] == self.index)
            )
        )
        if as_objects:
            out = self.workflow.get_EARs_from_indices(out)
        return out

    def get_element_dependencies(
        self,
        as_objects: Optional[bool] = False,
    ) -> List[Union[Tuple[int, int], Element]]:
        """Get elements that this element depends on."""
        # TODO: this will be used in viz.
        out = sorted(
            set((i[0], i[1]) for i in self.get_EAR_dependencies(as_objects=False))
        )
        if as_objects:
            out = self.workflow.get_elements_from_indices(out)
        return out

    def get_input_dependencies(self) -> Dict[str, Dict]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        element depends on."""
        out = {}
        for k, v in self.get_parameter_sources().items():
            if (
                v["type"] in ["local_input", "default_input"]
                and v["task_insert_ID"] != self.task.insert_ID
            ):
                out[k] = v

        return out

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, WorkflowTask]]:
        """Get tasks (insert ID or WorkflowTask objects) that this element depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""

        out = []
        for elem_dep in self.get_element_dependencies(as_objects=False):
            out.append(elem_dep[0])

        for i in self.get_input_dependencies().values():
            out.append(i["task_insert_ID"])

        out = sorted(set(out))

        if as_objects:
            out = [self.workflow.tasks.get(insert_ID=i) for i in out]

        return out

    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> List[Union[Tuple[int, int, int, int], ElementActionRun]]:
        """Get EARs of downstream tasks that depend on this element."""
        deps = []
        for task in self.task.downstream_tasks:
            for element in task.elements:
                for EAR_i in element.action_runs:
                    for dep_i in EAR_i.get_EAR_dependencies(as_objects=False):
                        if (
                            dep_i[0] == self.task.insert_ID
                            and dep_i[1] == self.index
                            and dep_i not in deps
                        ):
                            deps.append(dep_i)

        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_EARs_from_indices(deps)

        return deps

    def get_dependent_elements(
        self,
        as_objects: bool = False,
    ) -> List[Union[Tuple[int, int], Element]]:
        """Get elements of downstream tasks that depend on this element."""
        deps = []
        for task in self.task.downstream_tasks:
            for element in task.elements:
                for dep_i in element.get_element_dependencies(as_objects=False):
                    (ti_ID, e_idx) = dep_i
                    dep_elem = (task.insert_ID, element.index)
                    if (
                        ti_ID == self.task.insert_ID
                        and e_idx == self.index
                        and dep_elem not in deps
                    ):
                        deps.append(dep_elem)

        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_elements_from_indices(deps)

        return deps

    def get_dependent_tasks(
        self,
        as_objects: bool = False,
    ) -> List[Union[int, WorkflowTask]]:
        """Get downstream tasks that depend on this element."""
        deps = []
        for task in self.task.downstream_tasks:
            for dep_i in task.get_element_dependencies(as_objects=False):
                (ti_ID, e_idx) = dep_i
                if (
                    ti_ID == self.task.insert_ID
                    and e_idx == self.index
                    and task.insert_ID not in deps
                ):
                    deps.append(task.insert_ID)

        deps = sorted(deps)
        if as_objects:
            deps = [self.workflow.tasks.get(insert_ID=i) for i in deps]

        return deps


@dataclass
class ElementParameter:

    # TODO: do we need `parent` attribute?

    _app_attr = "app"

    task: WorkflowTask
    path: str
    parent: Union[Element, ElementAction, ElementActionRun, Parameters]
    element: Element
    data_idx: Dict[str, int]

    @property
    def value(self):
        return self.task._get_merged_parameter_data(self.data_idx, self.path)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(element={self.element!r}, path={self.path!r})"

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, self.__class__):
            return False
        if self.task == __o.task and self.path == __o.path:
            return True

    @property
    def data_idx_is_set(self):
        return {
            k: self.task.workflow.is_parameter_set(v) for k, v in self.data_idx.items()
        }

    @property
    def is_set(self):
        return all(self.data_idx_is_set.values())

    def get_size(self, **store_kwargs):
        raise NotImplementedError


@dataclass
class ElementFilter:

    parameter_path: ParameterPath
    condition: ConditionLike


@dataclass
class ElementGroup:

    name: str
    where: Optional[ElementFilter] = None
    group_by_distinct: Optional[ParameterPath] = None

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


@dataclass
class ElementRepeats:

    number: int
    where: Optional[ElementFilter] = None
