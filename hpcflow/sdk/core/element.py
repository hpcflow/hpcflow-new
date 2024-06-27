from __future__ import annotations
import copy
from dataclasses import dataclass, field
import os
from typing import cast, overload, TYPE_CHECKING

from valida.rules import Rule  # type: ignore

from hpcflow.sdk.core.actions import ElementAction, ElementActionRun
from hpcflow.sdk.core.errors import UnsupportedOSError, UnsupportedSchedulerError
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.parallel import ParallelMode
from hpcflow.sdk.core.task import ElementSet, WorkflowTask
from hpcflow.sdk.core.utils import (
    check_valid_py_identifier,
    dict_values_process_flat,
    get_enum_by_name_or_val,
    split_param_label,
)
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.shells import get_shell
if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from typing import Any, ClassVar, Dict, Literal
    from ..app import BaseApp
    from ..typing import ParamSource
    from .actions import Action
    from .task import WorkflowTask
    from .parameters import InputSource, ParameterPath, InputValue, ResourceSpec
    from .workflow import Workflow
    from .actions import ElementAction, ElementActionRun
    from .task import Parameters


class _ElementPrefixedParameter:
    _app: ClassVar[BaseApp]
    _app_attr: ClassVar[str] = "_app"

    def __init__(
        self,
        prefix: str,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        self._prefix = prefix
        self._element_iteration = element_iteration
        self._element_action = element_action
        self._element_action_run = element_action_run

        self._prefixed_names_unlabelled: dict[str, list[str]] | None = None  # assigned on first access

    def __getattr__(self, name) -> ElementParameter | dict[str, ElementParameter]:
        if name not in self.prefixed_names_unlabelled:
            raise ValueError(
                f"No {self._prefix} named {name!r}. Available {self._prefix} are: "
                f"{self.prefixed_names_unlabelled_str}."
            )

        labels = self.prefixed_names_unlabelled.get(name)
        if labels:
            # is multiple; return a dict of `ElementParameter`s
            return {
                label_i: self._app.ElementParameter(
                    path=f"{self._prefix}.{name}[{label_i}]",
                    task=self._task,
                    parent=self._parent,
                    element=self._element_iteration_obj,
                )
                for label_i in labels
            }
        else:
            # could be labelled still, but with `multiple=False`
            return self._app.ElementParameter(
                path=f"{self._prefix}.{name}",
                task=self._task,
                parent=self._parent,
                element=self._element_iteration_obj,
            )

    def __dir__(self):
        return [*super().__dir__(), *self.prefixed_names_unlabelled]

    @property
    def _parent(self) -> ElementIteration | ElementActionRun | None | ElementAction:
        return self._element_iteration or self._element_action or self._element_action_run

    @property
    def _element_iteration_obj(self) -> ElementIteration:
        if self._element_iteration:
            return self._element_iteration
        else:
            return self._parent.element_iteration

    @property
    def _task(self) -> WorkflowTask:
        return self._parent.task

    @property
    def prefixed_names_unlabelled(self) -> dict[str, list[str]]:
        """Get a mapping between inputs types and associated labels.

        If the schema input for a given input type has `multiple=False` (even if a label
        is defined), the values for that input type will be an empty list.

        """
        if self._prefixed_names_unlabelled is None:
            self._prefixed_names_unlabelled = self._get_prefixed_names_unlabelled()
        return self._prefixed_names_unlabelled

    @property
    def prefixed_names_unlabelled_str(self) -> str:
        return ", ".join(i for i in self.prefixed_names_unlabelled)

    def __repr__(self) -> str:
        # If there are one or more labels present, then replace with a single name
        # indicating there could be multiple (using a `*` prefix):
        names: list[str] = []
        for unlabelled, labels in self.prefixed_names_unlabelled.items():
            name_i = unlabelled
            if labels:
                name_i = "*" + name_i
            names.append(name_i)
        names_str = ", ".join(i for i in names)
        return f"{self.__class__.__name__}({names_str})"

    def _get_prefixed_names(self) -> list[str]:
        return sorted(self._parent.get_parameter_names(self._prefix))

    def _get_prefixed_names_unlabelled(self) -> dict[str, list[str]]:
        names = self._get_prefixed_names()
        all_names: dict[str, list[str]] = {}
        for i in list(names):
            if "[" in i:
                unlab_i, label_i = split_param_label(i)
                if unlab_i is not None and label_i is not None:
                    all_names.setdefault(unlab_i, []).append(label_i)
            else:
                all_names[i] = []
        return all_names

    def __iter__(self) -> Iterator[ElementParameter | dict[str, ElementParameter]]:
        for name in self.prefixed_names_unlabelled:
            yield getattr(self, name)


class ElementInputs(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        super().__init__("inputs", element_iteration, element_action, element_action_run)


class ElementOutputs(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        super().__init__("outputs", element_iteration, element_action, element_action_run)


class ElementInputFiles(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        super().__init__(
            "input_files", element_iteration, element_action, element_action_run
        )


class ElementOutputFiles(_ElementPrefixedParameter):
    def __init__(
        self,
        element_iteration: ElementIteration | None = None,
        element_action: ElementAction | None = None,
        element_action_run: ElementActionRun | None = None,
    ) -> None:
        super().__init__(
            "output_files", element_iteration, element_action, element_action_run
        )


@dataclass
class ElementResources(JSONLike):
    # TODO: how to specify e.g. high-memory requirement?

    app: ClassVar[BaseApp]

    scratch: str | None = None
    parallel_mode: ParallelMode | None = None
    num_cores: int | None = None
    num_cores_per_node: int | None = None
    num_threads: int | None = None
    num_nodes: int | None = None
    scheduler: str | None = None
    shell: str | None = None
    use_job_array: bool | None = None
    max_array_items: int | None = None
    time_limit: str | None = None

    scheduler_args: Dict | None = None
    shell_args: Dict | None = None
    os_name: str | None = None
    environments: Dict | None = None

    # SGE scheduler specific:
    SGE_parallel_env: str | None = None

    # SLURM scheduler specific:
    SLURM_partition: str | None = None
    SLURM_num_tasks: int | None = None
    SLURM_num_tasks_per_node: int | None = None
    SLURM_num_nodes: int | None = None
    SLURM_num_cpus_per_task: int | None = None

    def __post_init__(self):
        if (
            self.num_cores is None
            and self.num_cores_per_node is None
            and self.num_threads is None
            and self.num_nodes is None
        ):
            self.num_cores = 1

        if self.parallel_mode:
            self.parallel_mode = get_enum_by_name_or_val(ParallelMode, self.parallel_mode)

        self.scheduler_args = self.scheduler_args or {}
        self.shell_args = self.shell_args or {}

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
        dct = {k: copy.deepcopy(v) for k, v in self.__dict__.items() if k not in exclude}

        scheduler_args = dct["scheduler_args"]
        shell_args = dct["shell_args"]
        envs = dct["environments"]

        if isinstance(scheduler_args, dict):
            if "options" in scheduler_args:
                dct["scheduler_args"]["options"] = _hash_dict(scheduler_args["options"])
            dct["scheduler_args"] = _hash_dict(dct["scheduler_args"])

        if isinstance(shell_args, dict):
            dct["shell_args"] = _hash_dict(shell_args)

        if isinstance(envs, dict):
            for k, v in envs.items():
                dct["environments"][k] = _hash_dict(v)
            dct["environments"] = _hash_dict(dct["environments"])

        return _hash_dict(dct)

    @property
    def is_parallel(self) -> bool:
        """Returns True if any scheduler-agnostic arguments indicate a parallel job."""
        return bool(
            (self.num_cores and self.num_cores != 1)
            or (self.num_cores_per_node and self.num_cores_per_node != 1)
            or (self.num_nodes and self.num_nodes != 1)
            or (self.num_threads and self.num_threads != 1)
        )

    @property
    def SLURM_is_parallel(self) -> bool:
        """Returns True if any SLURM-specific arguments indicate a parallel job."""
        return bool(
            (self.SLURM_num_tasks and self.SLURM_num_tasks != 1)
            or (self.SLURM_num_tasks_per_node and self.SLURM_num_tasks_per_node != 1)
            or (self.SLURM_num_nodes and self.SLURM_num_nodes != 1)
            or (self.SLURM_num_cpus_per_task and self.SLURM_num_cpus_per_task != 1)
        )

    @staticmethod
    def get_env_instance_filterable_attributes() -> tuple[str, ...]:
        """Get a tuple of resource attributes that are used to filter environment
        executable instances at submit- and run-time."""
        return ("num_cores",)  # TODO: filter on `parallel_mode` later

    @staticmethod
    def get_default_os_name() -> str:
        return os.name

    @classmethod
    def get_default_shell(cls):
        return cls.app.config.default_shell

    @classmethod
    def get_default_scheduler(cls, os_name, shell_name):
        if os_name == "nt" and "wsl" in shell_name:
            # provide a "*_posix" default scheduler on windows if shell is WSL:
            return "direct_posix"
        return cls.app.config.default_scheduler

    def set_defaults(self):
        if self.os_name is None:
            self.os_name = self.get_default_os_name()
        if self.shell is None:
            self.shell = self.get_default_shell()
        if self.scheduler is None:
            self.scheduler = self.get_default_scheduler(self.os_name, self.shell)

        # merge defaults shell args from config:
        self.shell_args = {
            **self.app.config.shells.get(self.shell, {}).get("defaults", {}),
            **self.shell_args,
        }

        # "direct_posix" scheduler is valid on Windows if using WSL:
        cfg_lookup = f"{self.scheduler}_posix" if "wsl" in self.shell else self.scheduler
        cfg_sched = copy.deepcopy(self.app.config.schedulers.get(cfg_lookup, {}))

        # merge defaults scheduler args from config:
        cfg_defs = cfg_sched.get("defaults", {})
        cfg_opts = cfg_defs.pop("options", {})
        opts = {**cfg_opts, **self.scheduler_args.get("options", {})}
        self.scheduler_args["options"] = opts
        self.scheduler_args = {**cfg_defs, **self.scheduler_args}

    def validate_against_machine(self):
        """Validate the values for `os_name`, `shell` and `scheduler` against those
        supported on this machine (as specified by the app configuration)."""
        if self.os_name != os.name:
            raise UnsupportedOSError(os_name=self.os_name)
        if self.scheduler not in self.app.config.schedulers:
            raise UnsupportedSchedulerError(
                scheduler=self.scheduler,
                supported=self.app.config.schedulers,
            )
        # might raise `UnsupportedShellError`:
        get_shell(shell_name=self.shell, os_name=self.os_name)

        # Validate num_cores/num_nodes against options in config and set scheduler-
        # specific resources (e.g. SGE parallel environmentPE, and SLURM partition)
        if "_" in self.scheduler:  # e.g. WSL on windows uses *_posix
            key = tuple(self.scheduler.split("_"))
        else:
            key = (self.scheduler.lower(), self.os_name.lower())
        scheduler_cls = self.app.scheduler_lookup[key]
        scheduler_cls.process_resources(self, self.app.config.schedulers[self.scheduler])


class ElementIteration:
    app: ClassVar[BaseApp]
    _app_attr: ClassVar[str] = "app"

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        index: int,
        element: Element,
        data_idx: dict[str, int],
        EARs_initialised: bool,
        EAR_IDs: dict[int, int],  # FIXME: wrong type
        EARs: dict[int, dict[Mapping[str, Any], Any]] | None,
        schema_parameters: list[str],
        loop_idx: Dict,
    ):
        self._id = id_
        self._is_pending = is_pending
        self._index = index
        self._element = element
        self._data_idx = data_idx
        self._loop_idx = loop_idx
        self._schema_parameters = schema_parameters
        self._EARs_initialised = EARs_initialised
        self._EARs = EARs
        self._EAR_IDs = EAR_IDs

        # assigned on first access of corresponding properties:
        self._inputs: ElementInputs | None = None
        self._outputs: ElementOutputs | None = None
        self._input_files: ElementInputFiles | None = None
        self._output_files: ElementOutputFiles | None = None
        self._action_objs: dict[int, ElementAction] | None = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(id={self.id_!r}, "
            f"index={self.index!r}, element={self.element!r}, "
            f"EARs_initialised={self.EARs_initialised!r}"
            f")"
        )

    @property
    def data_idx(self) -> dict[str, int]:
        """The overall element iteration data index, before resolution of EARs."""
        return self._data_idx

    @property
    def EARs_initialised(self) -> bool:
        """Whether or not the EARs have been initialised."""
        return self._EARs_initialised

    @property
    def element(self) -> Element:
        return self._element

    @property
    def index(self) -> int:
        return self._index

    @property
    def id_(self) -> int:
        return self._id

    @property
    def is_pending(self) -> bool:
        return self._is_pending

    @property
    def task(self) -> WorkflowTask:
        return self.element.task

    @property
    def workflow(self) -> Workflow:
        return self.element.workflow

    @property
    def loop_idx(self) -> dict[str, int]:
        return self._loop_idx

    @property
    def schema_parameters(self) -> list[str]:
        return self._schema_parameters

    @property
    def EAR_IDs(self) -> dict[int, int]:
        return self._EAR_IDs

    @property
    def EAR_IDs_flat(self):
        return [j for i in self.EAR_IDs.values() for j in i]

    @property
    def actions(self) -> dict[int, ElementAction]:
        if self._action_objs is None:
            self._action_objs = ao = {
                act_idx: self.app.ElementAction(self, act_idx, runs)
                for act_idx, runs in (self._EARs or {}).items()
            }
            return ao
        return self._action_objs

    @property
    def action_runs(self) -> list[ElementActionRun]:
        """Get a list of element action runs, where only the final run is taken for each
        element action."""
        return [i.runs[-1] for i in self.actions.values()]

    @property
    def inputs(self) -> ElementInputs:
        if not self._inputs:
            self._inputs = ins = self.app.ElementInputs(element_iteration=self)
            return ins
        return self._inputs

    @property
    def outputs(self) -> ElementOutputs:
        if not self._outputs:
            self._outputs = outs = self.app.ElementOutputs(element_iteration=self)
            return outs
        return self._outputs

    @property
    def input_files(self) -> ElementInputFiles:
        if not self._input_files:
            self._input_files = eif = self.app.ElementInputFiles(element_iteration=self)
            return eif
        return self._input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        if not self._output_files:
            self._output_files = eof = self.app.ElementOutputFiles(element_iteration=self)
            return eof
        return self._output_files

    def get_parameter_names(self, prefix: str) -> list[str]:
        """Get parameter types associated with a given prefix.

        For example, with the prefix "inputs", this would return `['p1', 'p2']` for a task
        schema that has input types `p1` and `p2`. For inputs, labels are ignored. For
        example, for a task schema that accepts two inputs of the same type `p1`, with
        labels `one` and `two`, this method would return (for the "inputs" prefix):
        `['p1[one]', 'p1[two]']`.

        This method is distinct from `Action.get_parameter_names` in that it returns
        schema-level inputs/outputs, whereas `Action.get_parameter_names` returns
        action-level input/output/file types/labels.

        Parameters
        ----------
        prefix
            One of "inputs", "outputs".

        """
        single_label_lookup = self.task.template._get_single_label_lookup("inputs")
        return list(
            ".".join(single_label_lookup.get(i, i).split(".")[1:])
            for i in self.schema_parameters
            if i.startswith(prefix)
        )

    @TimeIt.decorator
    def get_data_idx(
        self,
        path: str | None = None,
        action_idx: int | None = None,
        run_idx: int = -1,
    ) -> dict[str, int]:
        """
        Parameters
        ----------
        action_idx
            The index of the action within the schema.
        """

        if not self.actions:
            data_idx = self.data_idx

        elif action_idx is None:
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

        return copy.deepcopy(data_idx)

    def __get_parameter_sources(
        self,
        data_idx: dict[str, int],
        filter_type: str | None,
        use_task_index: bool
    ) -> Mapping[str, ParamSource | list[ParamSource]]:
        # the value associated with `repeats.*` is the repeats index, not a parameter ID:
        for k in list(data_idx.keys()):
            if k.startswith("repeats."):
                data_idx.pop(k)

        out: Mapping[str, ParamSource | list[ParamSource]] = dict_values_process_flat(
            data_idx,
            callable=self.workflow.get_parameter_sources,
        )

        if use_task_index:
            for k, v in out.items():
                assert isinstance(v, dict)
                insert_ID = v.pop("task_insert_ID", None)
                if insert_ID is not None:
                    # Modify the contents of out
                    v["task_idx"] = self.workflow.tasks.get(insert_ID=insert_ID).index

        if not filter_type:
            return out

        # Filter to just the elements that have the right type property
        filtered = (
            (k, self.__filter_param_source_by_type(v, filter_type))
            for k, v in out.items() 
        )
        return {
            k: v
            for k, v in filtered
            if v is not None
        }

    @staticmethod
    def __filter_param_source_by_type(
        value: ParamSource | list[ParamSource], filter_type: str
    ) -> ParamSource | list[ParamSource] | None:
        if isinstance(value, list):
            sources = [src for src in value if src["type"] == filter_type]
            if sources:
                return sources
        else:
            if value["type"] == filter_type:
                return value
        return None

    @overload
    def get_parameter_sources(
        self,
        path: str | None,
        *,
        action_idx: int | None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[True],
        use_task_index: bool = False,
    ) -> dict[str, str]: ...

    @overload
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[False] = False,
        use_task_index: bool = False,
    ) -> Mapping[str, ParamSource | list[ParamSource]]: ...

    @TimeIt.decorator
    def get_parameter_sources(
        self,
        path: str | None = None,
        *,
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> dict[str, str] | Mapping[str, ParamSource | list[ParamSource]]:
        """
        Parameters
        ----------
        use_task_index
            If True, use the task index within the workflow, rather than the task insert
            ID.
        """
        data_idx = self.get_data_idx(path, action_idx, run_idx)
        out = self.__get_parameter_sources(data_idx, typ or "", use_task_index)
        if not as_strings:
            return out

        # format as a dict with compact string values
        task_key = "task_insert_ID"  # TODO: is this right?
        self_task_val = (
            self.task.index if task_key == "task_idx" else self.task.insert_ID
        )
        out_strs: dict[str, str] = {}
        for k, v in out.items():
            assert isinstance(v, dict)
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
        return out_strs

    @TimeIt.decorator
    def get(
        self,
        path: str | None = None,
        action_idx: int | None = None,
        run_idx: int = -1,
        default: Any = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ) -> Any:
        """Get element data from the persistent store."""
        # TODO include a "stats" parameter which when set we know the run has been
        # executed (or if start time is set but not end time, we know it's running or
        # failed.)

        data_idx = self.get_data_idx(action_idx=action_idx, run_idx=run_idx)
        single_label_lookup = self.task.template._get_single_label_lookup(prefix="inputs")

        if single_label_lookup:
            # For any non-multiple `SchemaParameter`s of this task with non-empty labels,
            # remove the trivial label:
            for key in list(data_idx.keys()):
                if (path or "").startswith(key):
                    # `path` uses labelled type, so no need to convert to non-labelled
                    continue
                lookup_val = single_label_lookup.get(key)
                if lookup_val:
                    data_idx[lookup_val] = data_idx.pop(key)

        return self.task._get_merged_parameter_data(
            data_index=data_idx,
            path=path,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
            default=default,
        )

    @overload
    def get_EAR_dependencies(
        self,
        as_objects: Literal[False] = False,
    ) -> list[int]: ...

    @overload
    def get_EAR_dependencies(
        self,
        as_objects: Literal[True],
    ) -> list[ElementActionRun]: ...

    @TimeIt.decorator
    def get_EAR_dependencies(
        self,
        as_objects: bool = False,
    ) -> list[int] | list[ElementActionRun]:
        """Get EARs that this element iteration depends on (excluding EARs of this element
        iteration)."""
        # TODO: test this includes EARs of upstream iterations of this iteration's element
        out: list[int]
        if self.action_runs:
            out = sorted(
                set(
                    EAR_ID
                    for i in self.action_runs
                    for EAR_ID in i.get_EAR_dependencies(as_objects=False)
                    if not EAR_ID in self.EAR_IDs_flat
                )
            )
        else:
            # if an "input-only" task schema, then there will be no action runs, but the
            # ElementIteration can still depend on other EARs if inputs are sourced from
            # upstream tasks:
            out = []
            for src in self.get_parameter_sources(typ="EAR_output").values():
                for src_i in (src if isinstance(src, list) else [src]):
                    EAR_ID_i = src_i["EAR_ID"]
                    assert isinstance(EAR_ID_i, int)
                    out.append(EAR_ID_i)
            out = sorted(set(out))

        if as_objects:
            return self.workflow.get_EARs_from_IDs(out)
        return out

    @overload
    def get_element_iteration_dependencies(
        self, as_objects: Literal[True]
    ) -> list[ElementIteration]: ...

    @overload
    def get_element_iteration_dependencies(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    @TimeIt.decorator
    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> list[int] | list[ElementIteration]:
        """Get element iterations that this element iteration depends on."""
        # TODO: test this includes previous iterations of this iteration's element
        EAR_IDs = self.get_EAR_dependencies(as_objects=False)
        out = sorted(set(self.workflow.get_element_iteration_IDs_from_EAR_IDs(EAR_IDs)))
        if as_objects:
            return self.workflow.get_element_iterations_from_IDs(out)
        return out

    @overload
    def get_element_dependencies(
        self,
        as_objects: Literal[False] = False,
    ) -> list[int]: ...

    @overload
    def get_element_dependencies(
        self,
        as_objects: Literal[True],
    ) -> list[Element]: ...

    @TimeIt.decorator
    def get_element_dependencies(
        self,
        as_objects: bool = False,
    ) -> list[int] | list[Element]:
        """Get elements that this element iteration depends on."""
        # TODO: this will be used in viz.
        EAR_IDs = self.get_EAR_dependencies(as_objects=False)
        out = sorted(set(self.workflow.get_element_IDs_from_EAR_IDs(EAR_IDs)))
        if as_objects:
            return self.workflow.get_elements_from_IDs(out)
        return out

    def get_input_dependencies(self) -> dict[str, dict]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        element iteration depends on."""
        out: dict[str, dict] = {}
        for k, v in self.get_parameter_sources().items():
            for v_i in (v if isinstance(v, list) else [v]):
                if (
                    v_i["type"] in ["local_input", "default_input"]
                    and v_i["task_insert_ID"] != self.task.insert_ID
                ):
                    out[k] = v_i

        return out

    @overload
    def get_task_dependencies(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    @overload
    def get_task_dependencies(
        self, as_objects: Literal[True]
    ) -> list[WorkflowTask]: ...

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> list[int] | list[WorkflowTask]:
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
            return [self.workflow.tasks.get(insert_ID=i) for i in out]

        return out

    @overload
    def get_dependent_EARs(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    @overload
    def get_dependent_EARs(
        self, as_objects: Literal[True]
    ) -> list[ElementActionRun]: ...

    @TimeIt.decorator
    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> list[int] | list[ElementActionRun]:
        """Get EARs of downstream iterations and tasks that depend on this element
        iteration."""
        # TODO: test this includes EARs of downstream iterations of this iteration's element
        deps: list[int] = []
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
            return self.workflow.get_EARs_from_IDs(deps)

        return deps

    @overload
    def get_dependent_element_iterations(
        self, as_objects: Literal[True]
    ) -> list[ElementIteration]: ...

    @overload
    def get_dependent_element_iterations(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    @TimeIt.decorator
    def get_dependent_element_iterations(
        self, as_objects: bool = False
    ) -> list[int] | list[ElementIteration]:
        """Get elements iterations of downstream iterations and tasks that depend on this
        element iteration."""
        # TODO: test this includes downstream iterations of this iteration's element?
        deps: list[int] = []
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
            return self.workflow.get_element_iterations_from_IDs(deps)

        return deps

    @overload
    def get_dependent_elements(
        self,
        as_objects: Literal[True],
    ) -> list[Element]: ...

    @overload
    def get_dependent_elements(
        self,
        as_objects: Literal[False] = False,
    ) -> list[int]: ...

    @TimeIt.decorator
    def get_dependent_elements(
        self,
        as_objects: bool = False,
    ) -> list[int] | list[Element]:
        """Get elements of downstream tasks that depend on this element iteration."""
        deps: list[int] = []
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
            return self.workflow.get_elements_from_IDs(deps)

        return deps

    @overload
    def get_dependent_tasks(
        self,
        as_objects: Literal[True],
    ) -> list[WorkflowTask]: ...

    @overload
    def get_dependent_tasks(
        self,
        as_objects: Literal[False] = False,
    ) -> list[int]: ...

    def get_dependent_tasks(
        self,
        as_objects: bool = False,
    ) -> list[int] | list[WorkflowTask]:
        """Get downstream tasks that depend on this element iteration."""
        deps: list[int] = []
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
            return [self.workflow.tasks.get(insert_ID=i) for i in deps]

        return deps

    def get_template_resources(self) -> dict[str, Any]:
        """Get template-level resources."""
        out = {}
        for res_i in self.workflow.template.resources:
            out[res_i.scope.to_string()] = res_i._get_value()
        return out

    @TimeIt.decorator
    def get_resources(
        self, action: Action, set_defaults: bool = False
    ) -> Mapping[str, Any]:
        """Resolve specific resources for the specified action of this iteration,
        considering all applicable scopes.

        Parameters
        ----------
        set_defaults
            If `True`, include machine-defaults for `os_name`, `shell` and `scheduler`.

        """

        # This method is currently accurate for both `ElementIteration` and `EAR` objects
        # because when generating the EAR data index we copy (from the schema data index)
        # anything that starts with "resources". BUT: when we support adding a run, the
        # user should be able to modify the resources! Which would invalidate this
        # assumption!!!!!

        # --- so need to rethink...
        # question is perhaps "what would the resources be if this action were to become
        # an EAR?" which would then allow us to test a resources-based action rule.

        resource_specs: dict[str, Any] = copy.deepcopy(self.get("resources"))

        env_spec = action.get_environment_spec()
        env_name = env_spec["name"]

        # set default env specifiers, if none set:
        if "any" not in resource_specs:
            resource_specs["any"] = {}
        if "environments" not in resource_specs["any"]:
            resource_specs["any"]["environments"] = {env_name: copy.deepcopy(env_spec)}

        for scope, dat in resource_specs.items():
            if "environments" in dat:
                # keep only relevant user-provided environment specifiers:
                resource_specs[scope]["environments"] = {
                    k: v for k, v in dat["environments"].items() if k == env_name
                }
                # merge user-provided specifiers into action specifiers:
                resource_specs[scope]["environments"][env_name] = {
                    **resource_specs[scope]["environments"].get(env_name, {}),
                    **copy.deepcopy(env_spec),
                }

        resources: dict[str, Any] = {}
        for scope_v in action.get_possible_scopes()[::-1]:
            # loop in reverse so higher-specificity scopes take precedence:
            scope_s = scope_v.to_string()
            scope_res = resource_specs.get(scope_s, {})
            resources.update({k: v for k, v in scope_res.items() if v is not None})

        if set_defaults:
            # used in e.g. `Rule.test` if testing resource rules on element iterations:
            if "os_name" not in resources:
                resources["os_name"] = self.app.ElementResources.get_default_os_name()
            if "shell" not in resources:
                resources["shell"] = self.app.ElementResources.get_default_shell()
            if "scheduler" not in resources:
                resources["scheduler"] = self.app.ElementResources.get_default_scheduler(
                    resources["os_name"], resources["shell"]
                )

        return resources

    def get_resources_obj(
        self, action: Action, set_defaults: bool = False
    ) -> ElementResources:
        return self.app.ElementResources(**self.get_resources(action, set_defaults))


class Element:
    app: ClassVar[BaseApp]
    _app_attr: ClassVar[str] = "app"

    # TODO: use slots
    # TODO:
    #   - add `iterations` property which returns `ElementIteration`
    #   - also map iteration properties of the most recent iteration to this object

    def __init__(
        self,
        id_: int,
        is_pending: bool,
        task: WorkflowTask,
        index: int,
        es_idx: int,
        seq_idx: dict[str, int],
        src_idx: dict[str, int],
        iteration_IDs: list[int],
        iterations: list[Dict],
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
        self._iteration_objs: list[ElementIteration] | None = None

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
    def task(self) -> WorkflowTask:
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
    def element_set(self) -> ElementSet:
        return self.task.template.element_sets[self.element_set_idx]

    @property
    def sequence_idx(self) -> dict[str, int]:
        return self._seq_idx

    @property
    def input_source_idx(self) -> dict[str, int]:
        return self._src_idx

    @property
    def input_sources(self) -> dict[str, InputSource]:
        return {
            k: self.element_set.input_sources[k.split("inputs.")[1]][v]
            for k, v in self.input_source_idx.items()
        }

    @property
    def workflow(self) -> Workflow:
        return self.task.workflow

    @property
    def iteration_IDs(self) -> list[int]:
        return self._iteration_IDs

    @property
    @TimeIt.decorator
    def iterations(self) -> list[ElementIteration]:
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
    def dir_name(self) -> str:
        return f"e_{self.index}"

    @property
    def latest_iteration(self) -> ElementIteration:
        return self.iterations[-1]

    @property
    def inputs(self) -> ElementInputs:
        return self.latest_iteration.inputs

    @property
    def outputs(self) -> ElementOutputs:
        return self.latest_iteration.outputs

    @property
    def input_files(self) -> ElementInputFiles:
        return self.latest_iteration.input_files

    @property
    def output_files(self) -> ElementOutputFiles:
        return self.latest_iteration.output_files

    @property
    def schema_parameters(self) -> list[str]:
        return self.latest_iteration.schema_parameters

    @property
    def actions(self) -> dict[int, ElementAction]:
        return self.latest_iteration.actions

    @property
    def action_runs(self) -> list[ElementActionRun]:
        """Get a list of element action runs from the latest iteration, where only the
        final run is taken for each element action."""
        return self.latest_iteration.action_runs

    def init_loop_index(self, loop_name: str):
        pass

    def to_element_set_data(self) -> tuple[list[InputValue], list[ResourceSpec]]:
        """Generate lists of workflow-bound InputValues and ResourceList."""
        inputs: list[InputValue] = []
        resources: list[ResourceSpec] = []
        for k, v in self.get_data_idx().items():
            k_s = k.split(".")

            if k_s[0] == "inputs":
                inp_val = self.app.InputValue(
                    parameter=k_s[1],
                    path=cast(str, k_s[2:]) or None,
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
        path: str | None = None,
        action_idx: int | None = None,
        run_idx: int = -1,
    ) -> dict[str, int]:
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

    @overload
    def get_parameter_sources(
        self, path: str | None = None, *,
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[False] = False,
        use_task_index: bool = False,
    ) -> Mapping[str, ParamSource | list[ParamSource]]: ...

    @overload
    def get_parameter_sources(
        self, path: str | None = None, *,
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: Literal[True],
        use_task_index: bool = False,
    ) -> dict[str, str]: ...

    def get_parameter_sources(
        self, path: str | None = None, *,
        action_idx: int | None = None,
        run_idx: int = -1,
        typ: str | None = None,
        as_strings: bool = False,
        use_task_index: bool = False,
    ) -> dict[str, str] | Mapping[str, ParamSource | list[ParamSource]]:
        """ "Get the parameter sources of the most recent element iteration.

        Parameters
        ----------
        use_task_index
            If True, use the task index within the workflow, rather than the task insert
            ID.
        """
        if as_strings:
            return self.latest_iteration.get_parameter_sources(
                path=path,
                action_idx=action_idx,
                run_idx=run_idx,
                typ=typ,
                as_strings=True,
                use_task_index=use_task_index,
            )
        return self.latest_iteration.get_parameter_sources(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
            typ=typ,
            use_task_index=use_task_index,
        )

    def get(
        self,
        path: str | None = None,
        action_idx: int | None = None,
        run_idx: int = -1,
        default: Any = None,
        raise_on_missing: bool = False,
        raise_on_unset: bool = False,
    ) -> Any:
        """Get element data of the most recent iteration from the persistent store."""
        return self.latest_iteration.get(
            path=path,
            action_idx=action_idx,
            run_idx=run_idx,
            default=default,
            raise_on_missing=raise_on_missing,
            raise_on_unset=raise_on_unset,
        )

    @overload
    def get_EAR_dependencies(
        self, as_objects: Literal[True]
    ) -> list[ElementActionRun]: ...

    @overload
    def get_EAR_dependencies(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    def get_EAR_dependencies(
        self, as_objects: bool = False
    ) -> list[int] | list[ElementActionRun]:
        """Get EARs that the most recent iteration of this element depends on."""
        if as_objects:
            return self.latest_iteration.get_EAR_dependencies(as_objects=True)
        return self.latest_iteration.get_EAR_dependencies()

    @overload
    def get_element_iteration_dependencies(
        self, as_objects: Literal[True]
    ) -> list[ElementIteration]: ...

    @overload
    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> list[int] | list[ElementIteration]: ...

    def get_element_iteration_dependencies(
        self, as_objects: bool = False
    ) -> list[int] | list[ElementIteration]:
        """Get element iterations that the most recent iteration of this element depends
        on."""
        if as_objects:
            return self.latest_iteration.get_element_iteration_dependencies(
                as_objects=True
            )
        return self.latest_iteration.get_element_iteration_dependencies()

    @overload
    def get_element_dependencies(
        self, as_objects: Literal[True]
    ) -> list[Element]: ...

    @overload
    def get_element_dependencies(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    def get_element_dependencies(
        self, as_objects: bool = False
    ) -> list[int] | list[Element]:
        """Get elements that the most recent iteration of this element depends on."""
        if as_objects:
            return self.latest_iteration.get_element_dependencies(as_objects=True)
        return self.latest_iteration.get_element_dependencies()

    def get_input_dependencies(self) -> dict[str, dict]:
        """Get locally defined inputs/sequences/defaults from other tasks that this
        the most recent iteration of this element depends on."""
        return self.latest_iteration.get_input_dependencies()

    @overload
    def get_task_dependencies(
        self, as_objects: Literal[True]
    ) -> list[WorkflowTask]: ...

    @overload
    def get_task_dependencies(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    def get_task_dependencies(
        self, as_objects: bool = False
    ) -> list[int] | list[WorkflowTask]:
        """Get tasks (insert ID or WorkflowTask objects) that the most recent iteration of
        this element depends on.

        Dependencies may come from either elements from upstream tasks, or from locally
        defined inputs/sequences/defaults from upstream tasks."""
        if as_objects:
            return self.latest_iteration.get_task_dependencies(as_objects=True)
        return self.latest_iteration.get_task_dependencies()

    @overload
    def get_dependent_EARs(
        self, as_objects: Literal[True]
    ) -> list[ElementActionRun]: ...

    @overload
    def get_dependent_EARs(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    def get_dependent_EARs(
        self, as_objects: bool = False
    ) -> list[int] | list[ElementActionRun]:
        """Get EARs that depend on the most recent iteration of this element."""
        if as_objects:
            return self.latest_iteration.get_dependent_EARs(as_objects=True)
        return self.latest_iteration.get_dependent_EARs()

    @overload
    def get_dependent_element_iterations(
        self, as_objects: Literal[True]
    ) -> list[ElementIteration]: ...

    @overload
    def get_dependent_element_iterations(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    def get_dependent_element_iterations(
        self, as_objects: bool = False
    ) -> list[int] | list[ElementIteration]:
        """Get element iterations that depend on the most recent iteration of this
        element."""
        if as_objects:
            return self.latest_iteration.get_dependent_element_iterations(
                as_objects=True
            )
        return self.latest_iteration.get_dependent_element_iterations()

    @overload
    def get_dependent_elements(
        self, as_objects: Literal[True]
    ) -> list[Element]: ...

    @overload
    def get_dependent_elements(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    def get_dependent_elements(
        self, as_objects: bool = False
    ) -> list[int] | list[Element]:
        """Get elements that depend on the most recent iteration of this element."""
        if as_objects:
            return self.latest_iteration.get_dependent_elements(as_objects=True)
        return self.latest_iteration.get_dependent_elements()

    @overload
    def get_dependent_tasks(
        self, as_objects: Literal[True]
    ) -> list[WorkflowTask]: ...

    @overload
    def get_dependent_tasks(
        self, as_objects: Literal[False] = False
    ) -> list[int]: ...

    def get_dependent_tasks(
        self, as_objects: bool = False
    ) -> list[int] | list[WorkflowTask]:
        """Get tasks that depend on the most recent iteration of this element."""
        if as_objects:
            return self.latest_iteration.get_dependent_tasks(as_objects=True)
        return self.latest_iteration.get_dependent_tasks()

    @TimeIt.decorator
    def get_dependent_elements_recursively(
        self, task_insert_ID: int | None = None
    ) -> list[Element]:
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

        def get_deps(element: Element) -> set[int]:
            deps = element.iterations[0].get_dependent_elements(as_objects=False)
            deps_objs = self.workflow.get_elements_from_IDs(deps)
            return set(deps).union(
                dep_j for deps_i in deps_objs for dep_j in get_deps(deps_i)
            )

        all_deps: Iterable[int] = get_deps(self)

        if task_insert_ID is not None:
            elem_ID_subset = self.workflow.tasks.get(insert_ID=task_insert_ID).element_IDs
            all_deps = [i for i in all_deps if i in elem_ID_subset]

        return self.workflow.get_elements_from_IDs(sorted(all_deps))


@dataclass(repr=False, eq=False)
class ElementParameter:
    app: ClassVar[BaseApp]
    _app_attr: ClassVar[str] = "app"

    task: WorkflowTask
    path: str
    parent: Element | ElementAction | ElementActionRun | Parameters
    element: Element | ElementIteration

    @property
    def data_idx(self):
        return self.parent.get_data_idx(path=self.path)

    @property
    def value(self) -> Any:
        assert hasattr(self.parent, "get")
        return self.parent.get(path=self.path)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(element={self.element!r}, path={self.path!r})"

    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, self.__class__):
            return False
        return self.task == __o.task and self.path == __o.path

    @property
    def data_idx_is_set(self):
        return {
            k: self.task.workflow.is_parameter_set(v) for k, v in self.data_idx.items()
        }

    @property
    def is_set(self) -> bool:
        return all(self.data_idx_is_set.values())

    def get_size(self, **store_kwargs):
        raise NotImplementedError


@dataclass
class ElementFilter(JSONLike):
    _child_objects = (ChildObjectSpec(name="rules", is_multiple=True, class_name="Rule"),)

    rules: list[Rule] = field(default_factory=list)

    def filter(
        self, element_iters: list[ElementIteration]
    ) -> list[ElementIteration]:
        out: list[ElementIteration] = []
        for i in element_iters:
            if all(rule_j.test(i) for rule_j in self.rules):
                out.append(i)
        return out


@dataclass
class ElementGroup(JSONLike):
    name: str
    where: ElementFilter | None = None
    group_by_distinct: ParameterPath | None = None

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


@dataclass
class ElementRepeats:
    number: int
    where: ElementFilter | None = None
