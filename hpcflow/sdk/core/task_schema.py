from contextlib import contextmanager
import copy
from dataclasses import dataclass, field
from importlib import import_module
from typing import Dict, List, Optional, Tuple, Union

from rich import print as rich_print
from rich.table import Table
from rich.panel import Panel
from rich.markup import escape
from rich.text import Text

from hpcflow.sdk import app
from hpcflow.sdk.core.parameters import Parameter
from .json_like import ChildObjectSpec, JSONLike
from .parameters import NullDefault, ParameterPropagationMode, SchemaInput
from .utils import check_valid_py_identifier


@dataclass
class TaskObjective(JSONLike):
    _child_objects = (
        ChildObjectSpec(
            name="name",
            is_single_attribute=True,
        ),
    )

    name: str

    def __post_init__(self):
        self.name = check_valid_py_identifier(self.name)


class TaskSchema(JSONLike):
    """Class to represent the inputs, outputs and implementation mechanism of a given
    task.

    Parameters
    ----------
    objective
        This is a string representing the objective of the task schema.
    actions
        A list of Action objects whose commands are to be executed by the task.
    method
        An optional string to label the task schema by its method.
    implementation
        An optional string to label the task schema by its implementation.
    inputs
        A list of SchemaInput objects that define the inputs to the task.
    outputs
        A list of SchemaOutput objects that define the outputs of the task.

    """

    _validation_schema = "task_schema_spec_schema.yaml"
    _hash_value = None
    _validate_actions = True

    _child_objects = (
        ChildObjectSpec(name="objective", class_name="TaskObjective"),
        ChildObjectSpec(
            name="inputs",
            class_name="SchemaInput",
            is_multiple=True,
            parent_ref="_task_schema",
        ),
        ChildObjectSpec(name="outputs", class_name="SchemaOutput", is_multiple=True),
        ChildObjectSpec(
            name="actions",
            class_name="Action",
            is_multiple=True,
            parent_ref="_task_schema",
        ),
    )

    def __init__(
        self,
        objective: Union[app.TaskObjective, str],
        actions: List[app.Action] = None,
        method: Optional[str] = None,
        implementation: Optional[str] = None,
        inputs: Optional[List[Union[app.Parameter, app.SchemaInput]]] = None,
        outputs: Optional[List[Union[app.Parameter, app.SchemaOutput]]] = None,
        version: Optional[str] = None,
        parameter_class_modules: Optional[List[str]] = None,
        _hash_value: Optional[str] = None,
    ):
        self.objective = objective
        self.actions = actions or []
        self.method = method
        self.implementation = implementation
        self.inputs = inputs or []
        self.outputs = outputs or []
        self.parameter_class_modules = parameter_class_modules or []
        self._hash_value = _hash_value

        self._set_parent_refs()

        # process `Action` script_data_in/out formats:
        for i in self.actions:
            i.process_script_data_formats()

        self._validate()
        self.actions = self._expand_actions()
        self.version = version
        self._task_template = None  # assigned by parent Task

        self._update_parameter_value_classes()

        # if version is not None:  # TODO: this seems fragile
        #     self.assign_versions(
        #         version=version,
        #         app_data_obj_list=self.app.task_schemas
        #         if app.is_data_files_loaded
        #         else [],
        #     )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.objective.name!r})"

    def _show_info(self, include=None):
        def _get_param_type_str(parameter) -> str:
            type_fmt = "-"
            if parameter._validation:
                try:
                    type_fmt = parameter._validation.to_tree()[0]["type_fmt"]
                except Exception:
                    pass
            elif parameter._value_class:
                param_cls = parameter._value_class
                cls_url = (
                    f"{self.app.docs_url}/reference/_autosummary/{param_cls.__module__}."
                    f"{param_cls.__name__}"
                )
                type_fmt = f"[link={cls_url}]{param_cls.__name__}[/link]"
            return type_fmt

        def _format_parameter_type(param) -> str:
            param_typ_fmt = param.typ
            if param.typ in self.app.parameters.list_attrs():
                param_url = (
                    f"{self.app.docs_url}/reference/template_components/"
                    f"parameters.html#{param.url_slug}"
                )
                param_typ_fmt = f"[link={param_url}]{param_typ_fmt}[/link]"
            return param_typ_fmt

        if not include:
            include = ("inputs", "outputs", "actions")

        tab = Table(show_header=False, box=None, padding=(0, 0), collapse_padding=True)
        tab.add_column(justify="right")
        tab.add_column()

        from rich.table import box

        tab_ins_outs = None
        if "inputs" in include or "outputs" in include:
            tab_ins_outs = Table(
                show_header=False,
                box=None,
                padding=(0, 1),
            )

            tab_ins_outs.add_column(justify="left")  # row heading ("Inputs" or "Outputs")
            tab_ins_outs.add_column()  # parameter name
            tab_ins_outs.add_column()  # type if available
            tab_ins_outs.add_column()  # default value (inputs only)
            tab_ins_outs.add_row()

        if "inputs" in include:
            if self.inputs:
                tab_ins_outs.add_row(
                    "",
                    Text("parameter", style="italic grey50"),
                    Text("type", style="italic grey50"),
                    Text("default", style="italic grey50"),
                )
            for inp_idx, inp in enumerate(self.inputs):
                def_str = "-"
                if not inp.multiple:
                    if inp.default_value is not NullDefault.NULL:
                        if inp.default_value.value is None:
                            def_str = "None"
                        else:
                            def_str = f"{escape(str(inp.default_value.value))!r}"
                tab_ins_outs.add_row(
                    "" if inp_idx > 0 else "[bold]Inputs[/bold]",
                    _format_parameter_type(inp.parameter),
                    _get_param_type_str(inp.parameter),
                    def_str,
                )

        if "outputs" in include:
            if "inputs" in include:
                tab_ins_outs.add_row()  # for spacing
            else:
                tab_ins_outs.add_row(
                    "",
                    Text("parameter", style="italic grey50"),
                    Text("type", style="italic grey50"),
                    "",
                )
            for out_idx, out in enumerate(self.outputs):
                tab_ins_outs.add_row(
                    "" if out_idx > 0 else "[bold]Outputs[/bold]",
                    _format_parameter_type(out.parameter),
                    _get_param_type_str(out.parameter),
                    "",
                )

        if tab_ins_outs:
            tab.add_row(tab_ins_outs)

        if "actions" in include:
            tab_acts = Table(
                show_header=False, box=None, padding=(1, 1), collapse_padding=True
            )
            tab_acts.add_column()
            tab_acts.add_row("[bold]Actions[/bold]")
            for act in self.actions:
                tab_cmds_i = Table(show_header=False, box=None)
                tab_cmds_i.add_column(justify="right")
                tab_cmds_i.add_column()
                if act.rules:
                    seen_rules = []  # bug: some rules seem to be repeated
                    for act_rule_j in act.rules:
                        if act_rule_j.rule in seen_rules:
                            continue
                        else:
                            seen_rules.append(act_rule_j.rule)
                        r_path = ""
                        if act_rule_j.rule.check_missing:
                            r_cond = f"check missing: {act_rule_j.rule.check_missing}"
                        elif act_rule_j.rule.check_exists:
                            r_cond = f"check exists: {act_rule_j.rule.check_exists}"
                        elif act_rule_j.rule.condition:
                            r_path = f"{act_rule_j.rule.path}: "
                            r_cond = str(act_rule_j.rule.condition.to_json_like())
                        else:
                            continue
                        tab_cmds_i.add_row(
                            "[italic]rule:[/italic]",
                            escape(f"{r_path}{r_cond}"),
                        )
                tab_cmds_i.add_row(
                    "[italic]scope:[/italic]",
                    escape(act.get_precise_scope().to_string()),
                )
                for cmd in act.commands:
                    cmd_str = "cmd" if cmd.command else "exe"
                    tab_cmds_i.add_row(
                        f"[italic]{cmd_str}:[/italic]",
                        escape(cmd.command or cmd.executable),
                    )
                    if cmd.stdout:
                        tab_cmds_i.add_row(
                            "[italic]out:[/italic]",
                            escape(cmd.stdout),
                        )
                    if cmd.stderr:
                        tab_cmds_i.add_row(
                            "[italic]err:[/italic]",
                            escape(cmd.stderr),
                        )

                tab_acts.add_row(tab_cmds_i)
            tab.add_row(tab_acts)
        else:
            tab.add_row()

        panel = Panel(tab, title=f"Task schema: {escape(self.objective.name)!r}")
        rich_print(panel)

    @property
    def basic_info(self):
        """Show inputs and outputs, formatted in a table."""
        return self._show_info(include=("inputs", "outputs"))

    @property
    def info(self):
        """Show inputs, outputs, and actions, formatted in a table."""
        return self._show_info()

    def __eq__(self, other):
        if type(other) is not self.__class__:
            return False
        if (
            self.objective == other.objective
            and self.actions == other.actions
            and self.method == other.method
            and self.implementation == other.implementation
            and self.inputs == other.inputs
            and self.outputs == other.outputs
            and self.version == other.version
            and self._hash_value == other._hash_value
        ):
            return True
        return False

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._task_template = self._task_template
        return obj

    @classmethod
    @contextmanager
    def ignore_invalid_actions(cls):
        try:
            cls._validate_actions = False
            yield
        finally:
            cls._validate_actions = True

    def _validate(self):
        if isinstance(self.objective, str):
            self.objective = self.app.TaskObjective(self.objective)

        if self.method:
            self.method = check_valid_py_identifier(self.method)
        if self.implementation:
            self.implementation = check_valid_py_identifier(self.implementation)

        # coerce Parameters to SchemaInputs
        for idx, i in enumerate(self.inputs):
            if isinstance(
                i, Parameter
            ):  # TODO: doc. that we should use the sdk class for type checking!
                self.inputs[idx] = self.app.SchemaInput(i)

        # coerce Parameters to SchemaOutputs
        for idx, i in enumerate(self.outputs):
            if isinstance(i, Parameter):
                self.outputs[idx] = self.app.SchemaOutput(i)
            elif isinstance(i, SchemaInput):
                self.outputs[idx] = self.app.SchemaOutput(i.parameter)

        # check action input/outputs
        if self._validate_actions:
            has_script = any(
                i.script and not i.input_file_generators and not i.output_file_parsers
                for i in self.actions
            )

            all_outs = []
            extra_ins = set(self.input_types)

            act_ins_lst = [act.get_input_types() for act in self.actions]
            act_outs_lst = [act.get_output_types() for act in self.actions]

            schema_ins = set(self.input_types)
            schema_outs = set(self.output_types)

            all_act_ins = set(j for i in act_ins_lst for j in i)
            all_act_outs = set(j for i in act_outs_lst for j in i)

            non_schema_act_ins = all_act_ins - schema_ins
            non_schema_act_outs = set(all_act_outs - schema_outs)

            extra_act_outs = non_schema_act_outs
            seen_act_outs = []
            for act_idx in range(len(self.actions)):
                for act_in in [
                    i for i in act_ins_lst[act_idx] if i in non_schema_act_ins
                ]:
                    if act_in not in seen_act_outs:
                        raise ValueError(
                            f"Action {act_idx} input {act_in!r} of schema {self.name!r} "
                            f"is not a schema input, but nor is it an action output from "
                            f"a preceding action."
                        )
                seen_act_outs += [
                    i for i in act_outs_lst[act_idx] if i not in seen_act_outs
                ]
                extra_act_outs = extra_act_outs - set(act_ins_lst[act_idx])
                act_inputs = set(act_ins_lst[act_idx])
                act_outputs = set(act_outs_lst[act_idx])
                extra_ins = extra_ins - act_inputs
                all_outs.extend(list(act_outputs))

            if extra_act_outs:
                raise ValueError(
                    f"The following action outputs of schema {self.name!r} are not schema"
                    f" outputs, but nor are they consumed by subsequent actions as "
                    f"action inputs: {tuple(extra_act_outs)!r}."
                )

            if extra_ins and not has_script:
                # TODO: bit of a hack, need to consider script ins/outs later
                # i.e. are all schema inputs "consumed" by an action?

                # consider OFP inputs:
                for act_i in self.actions:
                    for OFP_j in act_i.output_file_parsers:
                        extra_ins = extra_ins - set(OFP_j.inputs or [])

                if self.actions and extra_ins:
                    # allow for no actions (e.g. defining inputs for downstream tasks)
                    raise ValueError(
                        f"Schema {self.name!r} inputs {tuple(extra_ins)!r} are not used "
                        f"by any actions."
                    )

            missing_outs = set(self.output_types) - set(all_outs)
            if missing_outs and not has_script:
                # TODO: bit of a hack, need to consider script ins/outs later
                raise ValueError(
                    f"Schema {self.name!r} outputs {tuple(missing_outs)!r} are not "
                    f"generated by any actions."
                )

    def _expand_actions(self):
        """Create new actions for input file generators and output parsers in existing
        actions."""
        return [j for i in self.actions for j in i.expand()]

    def _update_parameter_value_classes(self):
        # ensure any referenced parameter_class_modules are imported:
        for module in self.parameter_class_modules:
            import_module(module)

        # TODO: support specifying file paths in addition to (instead of?) importable
        # module paths

        for inp in self.inputs:
            inp.parameter._set_value_class()

        for out in self.outputs:
            out.parameter._set_value_class()

    def make_persistent(self, workflow: app.Workflow, source: Dict) -> List[int]:
        new_refs = []
        for input_i in self.inputs:
            for lab_info in input_i.labelled_info():
                if "default_value" in lab_info:
                    _, dat_ref, is_new = lab_info["default_value"].make_persistent(
                        workflow, source
                    )
                    new_refs.extend(dat_ref) if is_new else None
        return new_refs

    @property
    def name(self):
        out = (
            f"{self.objective.name}"
            f"{f'_{self.method}' if self.method else ''}"
            f"{f'_{self.implementation}' if self.implementation else ''}"
        )
        return out

    @property
    def input_types(self):
        return tuple(j for i in self.inputs for j in i.all_labelled_types)

    @property
    def output_types(self):
        return tuple(i.typ for i in self.outputs)

    @property
    def provides_parameters(self) -> Tuple[Tuple[str, str]]:
        out = []
        for schema_inp in self.inputs:
            for labelled_info in schema_inp.labelled_info():
                prop_mode = labelled_info["propagation_mode"]
                if prop_mode is not ParameterPropagationMode.NEVER:
                    out.append(
                        (schema_inp.input_or_output, labelled_info["labelled_type"])
                    )
        for schema_out in self.outputs:
            if schema_out.propagation_mode is not ParameterPropagationMode.NEVER:
                out.append((schema_out.input_or_output, schema_out.typ))
        return tuple(out)

    @property
    def task_template(self):
        return self._task_template

    @classmethod
    def get_by_key(cls, key):
        """Get a config-loaded task schema from a key."""
        return cls.app.task_schemas.get(key)

    def get_parameter_dependence(self, parameter: app.SchemaParameter):
        """Find if/where a given parameter is used by the schema's actions."""
        out = {"input_file_writers": [], "commands": []}
        for act_idx, action in enumerate(self.actions):
            deps = action.get_parameter_dependence(parameter)
            for key in out:
                out[key].extend((act_idx, i) for i in deps[key])
        return out

    def get_key(self):
        return (str(self.objective), self.method, self.implementation)

    def _get_single_label_lookup(self, prefix="") -> Dict[str, str]:
        """Get a mapping between schema input types that have a single label (i.e.
        labelled but with `multiple=False`) and the non-labelled type string.

        For example, if a task schema has a schema input like:
        `SchemaInput(parameter="p1", labels={"one": {}}, multiple=False)`, this method
        would return a dict that includes: `{"p1[one]": "p1"}`. If the `prefix` argument
        is provided, this will be added to map key and value (and a terminating period
        will be added to the end of the prefix if it does not already end in one). For
        example, with `prefix="inputs"`, this method might return:
        `{"inputs.p1[one]": "inputs.p1"}`.

        """
        lookup = {}
        if prefix and not prefix.endswith("."):
            prefix += "."
        for sch_inp in self.inputs:
            if not sch_inp.multiple and sch_inp.single_label:
                labelled_type = sch_inp.single_labelled_type
                lookup[f"{prefix}{labelled_type}"] = f"{prefix}{sch_inp.typ}"
        return lookup

    @property
    def multi_input_types(self) -> List[str]:
        """Get a list of input types that have multiple labels."""
        out = []
        for inp in self.inputs:
            if inp.multiple:
                out.append(inp.parameter.typ)
        return out
