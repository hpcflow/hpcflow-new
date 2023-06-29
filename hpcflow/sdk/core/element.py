from __future__ import annotations
import copy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from valida.conditions import ConditionLike

from hpcflow.sdk import app
from hpcflow.sdk.core.json_like import JSONLike
from hpcflow.sdk.core.utils import check_valid_py_identifier


class _ElementPrefixedParameter:
    _app_attr = "_app"

    def __init__(
        self,
        prefix: str,
        element_iteration: Optional[app.Element] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        self._prefix = prefix
        self._element_iteration = element_iteration
        self._element_action = element_action
        self._element_action_run = element_action_run

    def __getattr__(self, name):
        if name not in self._get_prefixed_names():
            raise ValueError(
                f"No {self._prefix} named {name!r}. Available {self._prefix} are: "
                f"{self._get_prefixed_names_str()}."
            )

        data_idx = self._parent.get_data_idx(path=f"{self._prefix}.{name}")
        param = self._app.ElementParameter(
            path=f"{self._prefix}.{name}",
            task=self._task,
            data_idx=data_idx,
            parent=self._parent,
            element=self._element_iteration_obj,
        )
        return param

    def __dir__(self):
        return super().__dir__() + self._get_prefixed_names()

    @property
    def _parent(self):
        return self._element_iteration or self._element_action or self._element_action_run

    @property
    def _element_iteration_obj(self):
        if self._element_iteration:
            return self._element_iteration
        else:
            return self._parent.element_iteration

    @property
    def _task(self):
        return self._parent.task

    def __repr__(self):
        return f"{self.__class__.__name__}({self._get_prefixed_names_str()})"

    def _get_prefixed_names_str(self):
        return f"{', '.join(f'{i!r}' for i in self._get_prefixed_names())}"

    def _get_prefixed_names(self):
        return sorted(self._parent.get_parameter_names(self._prefix))

    def __iter__(self):
        for name in self._get_prefixed_names():
            yield getattr(self, name)


class ElementInputs(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: Optional[app.ElementIteration] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        super().__init__("inputs", element_iteration, element_action, element_action_run)


class ElementOutputs(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: Optional[app.ElementIteration] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        super().__init__("outputs", element_iteration, element_action, element_action_run)


class ElementInputFiles(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: Optional[app.ElementIteration] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        super().__init__(
            "input_files", element_iteration, element_action, element_action_run
        )


class ElementOutputFiles(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: Optional[app.ElementIteration] = None,
        element_action: Optional[app.ElementAction] = None,
        element_action_run: Optional[app.ElementActionRun] = None,
    ) -> None:
        super().__init__(
            "output_files", element_iteration, element_action, element_action_run
        )


@dataclass
class ElementResources(JSONLike):
    # TODO: how to specify e.g. high-memory requirement?

    scratch: str = None
    num_cores: int = None
    scheduler: str = None
    shell: str = None
    use_job_array: bool = None
    time_limit: str = None
    scheduler_options: Dict = None

    scheduler_args: Dict = None
    shell_args: Dict = None
    os_name: str = None

    def __post_init__(self):
        if self.num_cores is None:
            self.num_cores = 1

        self.scheduler_args = self.scheduler_args or {}
        self.shell_args = self.shell_args or {}
        self.scheduler_options = self.scheduler_options or {}

    def __eq__(self, other) -> bool:
        if type(self) != type(other):
            return False
        else:
            return self.__dict__ == other.__dict__

    def get_jobscript_hash(self):
        """Get hash from all arguments that distinguish jobscripts."""

        def _hash_dict(d):
            if not d:
                return -1
            keys, vals = zip(*d.items())
            return hash(tuple((keys, vals)))

        exclude = ("time_limit",)
        sub_dicts = ("scheduler_options", "scheduler_args", "shell_args")
        dct = {k: copy.deepcopy(v) for k, v in self.__dict__.items() if k not in exclude}
        if "options" in dct.get("scheduler_args", []):
            dct["scheduler_args"]["options"] = tuple(dct["scheduler_args"]["options"])

        for k in sub_dicts:
            if k in dct:
                dct[k] = _hash_dict(dct[k])

        return _hash_dict(dct)


class ElementIteration:
    _app_attr = "app"

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        index: int,
        element: app.Element,
        data_idx: Dict,
        EAR_IDs: Dict[int, int],
        EARs: Union[List[Dict], None],
        schema_parameters: List[str],
        loop_idx: Dict,
    ):
        self._id = id_
        self._is_pending = is_pending
        self._index = index
        self._element = element
        self._data_idx = data_idx
        self._loop_idx = loop_idx
        self._schema_parameters = schema_parameters
        self._EARs = EARs
        self._EAR_IDs = EAR_IDs

        # assigned on first access of corresponding properties:
        self._inputs = None
        self._outputs = None
        self._input_files = None
        self._output_files = None
        self._action_objs = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(id={self.id_!r}, "
            f"index={self.index!r}, element={self.element!r}, "
            f"EARs_initialised={self.EARs_initialised!r}"
            f")"
        )

    @property
    def data_idx(self):
        """The overall element iteration data index, before resolution of EARs."""
        return self._data_idx

    @property
    def EARs_initialised(self):
        """Whether or not the EARs have been initialised."""
        return self._EARs is not None

    @property
    def element(self):
        return self._element

    @property
    def index(self):
        return self._index

    @property
    def id_(self) -> int:
        return self._id

    @property
    def is_pending(self) -> bool:
        return self._is_pending

    @property
    def task(self):
        return self.element.task

    @property
    def workflow(self):
        return self.element.workflow

    @property
    def loop_idx(self) -> Dict[str, int]:
        return self._loop_idx

    @property
    def schema_parameters(self) -> List[str]:
        return self._schema_parameters

    @property
    def EAR_IDs(self) -> Dict[int, int]:
        return self._EAR_IDs

    @property
    def EAR_IDs_flat(self):
        return [j for i in self.EAR_IDs.values() for j in i]

    @property
    def actions(self) -> Dict[app.ElementAction]:
        if self._action_objs is None:
            self._action_objs = {
                act_idx: self.app.ElementAction(
                    element_iteration=self,
                    action_idx=act_idx,
                    runs=runs,
                )
                for act_idx, runs in self._EARs.items()
            }
        return self._action_objs

    @property
    def action_runs(self) -> List[app.ElementActionRun]:
        """Get a list of element action runs, where only the final run is taken for each
        element action."""
        return [i.runs[-1] for i in self.actions.values()]

    @property
    def inputs(self) -> app.ElementInputs:
        if not self._inputs:
            self._inputs = self.app.ElementInputs(element_iteration=self)
        return self._inputs

    @property
    def outputs(self) -> app.ElementOutputs:
        if not self._outputs:
            self._outputs = self.app.ElementOutputs(element_iteration=self)
        return self._outputs

    @property
    def input_files(self) -> app.ElementInputFiles:
        if not self._input_files:
            self._input_files = self.app.ElementInputFiles(element_iteration=self)
        return self._input_files

    @property
    def output_files(self) -> app.ElementOutputFiles:
        if not self._output_files:
            self._output_files = self.app.ElementOutputFiles(element_iteration=self)
        return self._output_files

    def get_parameter_names(self, prefix: str) -> List[str]:
        return list(
            ".".join(i.split(".")[1:])
            for i in self.schema_parameters
            if i.startswith(prefix)
        )

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
            # inputs should be from first action where that input is defined, and outputs
            # should include modifications from all actions; we can't just take
            # `self.data_idx`, because 1) this is used for initial runs, and subsequent
            # runs might have different parametrisations, and 2) we want to include
            # intermediate input/output_files:
            data_idx = {}
            for action in self.actions.values():
                for k, v in action.runs[run_idx].data_idx.items():
                    is_input = k.startswith("inputs")
                    if (is_input and k not in data_idx) or not is_input:
                        data_idx[k] = v

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
        out = {k: self.workflow.get_parameter_source(v) for k, v in data_idx.items()}
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
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get EARs that this element iteration depends on (excluding EARs of this element
        iteration)."""
        # TODO: test this includes EARs of upstream iterations of this iteration's element
        out = sorted(
            set(
                EAR_ID
                for i in self.action_runs
                for EAR_ID in i.get_EAR_dependencies(as_objects=False)
                if not EAR_ID in self.EAR_IDs_flat
            )
        )
        if as_objects:
            out = self.workflow.get_EARs_from_IDs(out)
        return out

    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementIteration]]:
        """Get element iterations that this element iteration depends on."""
        # TODO: test this includes previous iterations of this iteration's element
        EAR_IDs = self.get_EAR_dependencies(as_objects=False)
        out = sorted(set(self.workflow.get_element_iteration_IDs_from_EAR_IDs(EAR_IDs)))
        if as_objects:
            out = self.workflow.get_element_iterations_from_IDs(out)
        return out

    def get_element_dependencies(
        self,
        as_objects: Optional[bool] = False,
    ) -> List[Union[int, app.Element]]:
        """Get elements that this element iteration depends on."""
        # TODO: this will be used in viz.
        EAR_IDs = self.get_EAR_dependencies(as_objects=False)
        out = sorted(set(self.workflow.get_element_IDs_from_EAR_IDs(EAR_IDs)))
        if as_objects:
            out = self.workflow.get_elements_from_IDs(out)
        return out

    def get_input_dependencies(self) -> Dict[str, Dict]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        element iteration depends on."""
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
    ) -> List[Union[int, app.WorkflowTask]]:
        """Get tasks (insert ID or WorkflowTask objects) that this element iteration
        depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""

        out = self.workflow.get_task_IDs_from_element_IDs(
            self.get_element_dependencies(as_objects=False)
        )
        for i in self.get_input_dependencies().values():
            out.append(i["task_insert_ID"])

        out = sorted(set(out))

        if as_objects:
            out = [self.workflow.tasks.get(insert_ID=i) for i in out]

        return out

    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get EARs of downstream iterations and tasks that depend on this element
        iteration."""
        # TODO: test this includes EARs of downstream iterations of this iteration's element
        deps = []
        for task in self.workflow.tasks[self.task.index :]:
            for elem in task.elements[:]:
                for iter_ in elem.iterations:
                    if iter_.id_ == self.id_:
                        # don't include EARs of this iteration
                        continue
                    for run in iter_.action_runs:
                        for dep_EAR_i in run.get_EAR_dependencies(as_objects=True):
                            # does dep_EAR_i belong to self?
                            if dep_EAR_i.id_ in self.EAR_IDs_flat and run.id_ not in deps:
                                deps.append(run.id_)
        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_EARs_from_IDs(deps)

        return deps

    def get_dependent_element_iterations(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementIteration]]:
        """Get elements iterations of downstream iterations and tasks that depend on this
        element iteration."""
        # TODO: test this includes downstream iterations of this iteration's element?
        deps = []
        for task in self.workflow.tasks[self.task.index :]:
            for elem in task.elements[:]:
                for iter_i in elem.iterations:
                    if iter_i.id_ == self.id_:
                        continue
                    for dep_iter_i in iter_i.get_element_iteration_dependencies(
                        as_objects=True
                    ):
                        if dep_iter_i.id_ == self.id_ and iter_i.id_ not in deps:
                            deps.append(iter_i.id_)
        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_element_iterations_from_IDs(deps)

        return deps

    def get_dependent_elements(
        self,
        as_objects: bool = False,
    ) -> List[Union[int, app.Element]]:
        """Get elements of downstream tasks that depend on this element iteration."""
        deps = []
        for task in self.task.downstream_tasks:
            for element in task.elements[:]:
                for iter_i in element.iterations:
                    for dep_iter_i in iter_i.get_element_iteration_dependencies(
                        as_objects=True
                    ):
                        if dep_iter_i.id_ == self.id_ and element.id_ not in deps:
                            deps.append(element.id_)

        deps = sorted(deps)
        if as_objects:
            deps = self.workflow.get_elements_from_IDs(deps)

        return deps

    def get_dependent_tasks(
        self,
        as_objects: bool = False,
    ) -> List[Union[int, app.WorkflowTask]]:
        """Get downstream tasks that depend on this element iteration."""
        deps = []
        for task in self.task.downstream_tasks:
            for element in task.elements[:]:
                for iter_i in element.iterations:
                    for dep_iter_i in iter_i.get_element_iteration_dependencies(
                        as_objects=True
                    ):
                        if dep_iter_i.id_ == self.id_ and task.insert_ID not in deps:
                            deps.append(task.insert_ID)
        deps = sorted(deps)
        if as_objects:
            deps = [self.workflow.tasks.get(insert_ID=i) for i in deps]

        return deps


class Element:
    _app_attr = "app"

    # TODO: use slots
    # TODO:
    #   - add `iterations` property which returns `ElementIteration`
    #   - also map iteration properties of the most recent iteration to this object

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        task: app.WorkflowTask,
        index: int,
        es_idx: int,
        seq_idx: Dict[str, int],
        src_idx: Dict[str, int],
        iteration_IDs: List[int],
        iterations: List[Dict],
    ) -> None:
        self._id = id_
        self._is_pending = is_pending
        self._task = task
        self._index = index
        self._es_idx = es_idx
        self._seq_idx = seq_idx
        self._src_idx = src_idx

        self._iteration_IDs = iteration_IDs
        self._iterations = iterations

        # assigned on first access:
        self._iteration_objs = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(id={self.id_!r}, "
            f"index={self.index!r}, task={self.task.unique_name!r}"
            f")"
        )

    @property
    def id_(self) -> int:
        return self._id

    @property
    def is_pending(self) -> bool:
        return self._is_pending

    @property
    def task(self) -> app.WorkflowTask:
        return self._task

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
    def input_source_idx(self) -> Dict[str, int]:
        return self._src_idx

    @property
    def input_sources(self) -> Dict[str, app.InputSource]:
        return {
            k: self.element_set.input_sources[k.split("inputs.")[1]][v]
            for k, v in self.input_source_idx.items()
        }

    @property
    def workflow(self) -> app.Workflow:
        return self.task.workflow

    @property
    def iteration_IDs(self) -> List[int]:
        return self._iteration_IDs

    @property
    def iterations(self) -> Dict[app.ElementAction]:
        # TODO: fix this
        if self._iteration_objs is None:
            self._iteration_objs = [
                self.app.ElementIteration(
                    element=self,
                    index=idx,
                    **{k: v for k, v in iter_i.items() if k != "element_ID"},
                )
                for idx, iter_i in enumerate(self._iterations)
            ]
        return self._iteration_objs

    @property
    def dir_name(self):
        return f"e_{self.index}"

    @property
    def latest_iteration(self):
        return self.iterations[-1]

    @property
    def inputs(self) -> app.ElementInputs:
        return self.latest_iteration.inputs

    @property
    def outputs(self) -> app.ElementOutputs:
        return self.latest_iteration.outputs

    @property
    def input_files(self) -> app.ElementInputFiles:
        return self.latest_iteration.input_files

    @property
    def output_files(self) -> app.ElementOutputFiles:
        return self.latest_iteration.output_files

    @property
    def schema_parameters(self) -> List[str]:
        return self.latest_iteration.schema_parameters

    @property
    def actions(self) -> Dict[app.ElementAction]:
        return self.latest_iteration.actions

    @property
    def action_runs(self) -> List[app.ElementActionRun]:
        """Get a list of element action runs from the latest iteration, where only the
        final run is taken for each element action."""
        return self.latest_iteration.action_runs

    def init_loop_index(self, loop_name: str):
        pass

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

    def get_data_idx(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
    ) -> Dict[str, int]:
        """Get the data index of the most recent element iteration.

        Parameters
        ----------
        action_idx
            The index of the action within the schema.
        """
        return self.latest_iteration.get_data_idx(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
        )

    def get_parameter_sources(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
        typ: str = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> Dict[str, Union[str, Dict[str, Any]]]:
        """ "Get the parameter sources of the most recent element iteration.

        Parameters
        ----------
        use_task_index
            If True, use the task index within the workflow, rather than the task insert
            ID.
        """
        return self.latest_iteration.get_parameter_sources(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
            typ=typ,
            as_strings=as_strings,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str = None,
        action_idx: int = None,
        run_idx: int = -1,
        default: Any = None,
        raise_on_missing: bool = False,
    ) -> Any:
        """Get element data of the most recent iteration from the persistent store."""
        return self.latest_iteration.get(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
            default=default,
            raise_on_missing=raise_on_missing,
        )

    def get_EAR_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get EARs that the most recent iteration of this element depends on."""
        return self.latest_iteration.get_EAR_dependencies(as_objects=as_objects)

    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementIteration]]:
        """Get element iterations that the most recent iteration of this element depends
        on."""
        return self.latest_iteration.get_element_iteration_dependencies(
            as_objects=as_objects
        )

    def get_element_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.Element]]:
        """Get elements that the most recent iteration of this element depends on."""
        return self.latest_iteration.get_element_dependencies(as_objects=as_objects)

    def get_input_dependencies(self) -> Dict[str, Dict]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        the most recent iteration of this element depends on."""
        return self.latest_iteration.get_input_dependencies()

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> List[Union[int, app.WorkflowTask]]:
        """Get tasks (insert ID or WorkflowTask objects) that the most recent iteration of
        this element depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""
        return self.latest_iteration.get_task_dependencies(as_objects=as_objects)

    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementActionRun]]:
        """Get EARs that depend on the most recent iteration of this element."""
        return self.latest_iteration.get_dependent_EARs(as_objects=as_objects)

    def get_dependent_element_iterations(
        self, as_objects: bool = False
    ) -> List[Union[int, app.ElementIteration]]:
        """Get element iterations that depend on the most recent iteration of this
        element."""
        return self.latest_iteration.get_dependent_element_iterations(
            as_objects=as_objects
        )

    def get_dependent_elements(
        self, as_objects: bool = False
    ) -> List[Union[int, app.Element]]:
        """Get elements that depend on the most recent iteration of this element."""
        return self.latest_iteration.get_dependent_elements(as_objects=as_objects)

    def get_dependent_tasks(
        self, as_objects: bool = False
    ) -> List[Union[int, app.WorkflowTask]]:
        """Get tasks that depend on the most recent iteration of this element."""
        return self.latest_iteration.get_dependent_tasks(as_objects=as_objects)

    def get_dependent_elements_recursively(self, task_insert_ID=None):
        """Get downstream elements that depend on this element, including recursive
        dependencies.

        Dependencies are resolved using the initial iteration only. This method is used to
        identify from which element in the previous iteration a new iteration should be
        parametrised.

        Parameters
        ----------
        task_insert_ID
            If specified, only return elements from this task.

        """

        def get_deps(element):
            deps = element.iterations[0].get_dependent_elements(as_objects=False)
            deps_objs = self.workflow.get_elements_from_IDs(deps)
            return set(deps).union(
                [dep_j for deps_i in deps_objs for dep_j in get_deps(deps_i)]
            )

        all_deps = get_deps(self)

        if task_insert_ID is not None:
            elem_ID_subset = self.workflow.tasks.get(insert_ID=task_insert_ID).element_IDs
            all_deps = [i for i in all_deps if i in elem_ID_subset]

        return self.workflow.get_elements_from_IDs(sorted(all_deps))


@dataclass
class ElementParameter:
    # TODO: do we need `parent` attribute?

    _app_attr = "app"

    task: app.WorkflowTask
    path: str
    parent: Union[Element, app.ElementAction, app.ElementActionRun, app.Parameters]
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
    parameter_path: app.ParameterPath
    condition: ConditionLike


@dataclass
class ElementGroup:
    name: str
    where: Optional[ElementFilter] = None
    group_by_distinct: Optional[app.ParameterPath] = None

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


@dataclass
class ElementRepeats:
    number: int
    where: Optional[ElementFilter] = None
