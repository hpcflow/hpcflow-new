from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from hpcflow.app import app as hf
from hpcflow.sdk.core.parameters import ParameterValue


def make_schemas(ins_outs, ret_list=False):
    out = []
    for idx, info in enumerate(ins_outs):
        if len(info) == 2:
            (ins_i, outs_i) = info
            obj = f"t{idx}"
        else:
            (ins_i, outs_i, obj) = info

        # distribute outputs over stdout, stderr and out file parsers:
        stdout = None
        stderr = None
        out_file_parsers = None

        if outs_i:
            stdout = f"<<parameter:{outs_i[0]}>>"
        if len(outs_i) > 1:
            stderr = f"<<parameter:{outs_i[1]}>>"
        if len(outs_i) > 2:
            out_file_parsers = [
                hf.OutputFileParser(
                    output=hf.Parameter(out_i),
                    output_files=[hf.FileSpec(label="file1", name="file1.txt")],
                )
                for out_i in outs_i[2:]
            ]
        cmd = hf.Command(
            " ".join(f"echo $((<<parameter:{i}>> + 100))" for i in ins_i.keys()),
            stdout=stdout,
            stderr=stderr,
        )

        act_i = hf.Action(
            commands=[cmd],
            output_file_parsers=out_file_parsers,
            environments=[hf.ActionEnvironment("env_1")],
        )
        print(f"{ins_i=}")
        out.append(
            hf.TaskSchema(
                objective=obj,
                actions=[act_i],
                inputs=[hf.SchemaInput(k, default_value=v) for k, v in ins_i.items()],
                outputs=[hf.SchemaOutput(k) for k in outs_i],
            )
        )
    if len(ins_outs) == 1 and not ret_list:
        out = out[0]
    return out


def make_parameters(num):
    return [hf.Parameter(f"p{i + 1}") for i in range(num)]


def make_actions(ins_outs: List[Tuple[Union[Tuple, str], str]]) -> List[hf.Action]:
    act_env = hf.ActionEnvironment(environment="env1")
    actions = []
    for ins_outs_i in ins_outs:
        if len(ins_outs_i) == 2:
            ins, out = ins_outs_i
            err = None
        else:
            ins, out, err = ins_outs_i
        if not isinstance(ins, tuple):
            ins = (ins,)
        cmd_str = "doSomething "
        for i in ins:
            cmd_str += f" <<parameter:{i}>>"
        stdout = f"<<parameter:{out}>>"
        stderr = None
        if err:
            stderr = f"<<parameter:{err}>>"
        act = hf.Action(
            commands=[hf.Command(cmd_str, stdout=stdout, stderr=stderr)],
            environments=[act_env],
        )
        actions.append(act)
    return actions


def make_tasks(
    schemas_spec,
    local_inputs=None,
    local_sequences=None,
    local_resources=None,
    nesting_orders=None,
):
    local_inputs = local_inputs or {}
    local_sequences = local_sequences or {}
    local_resources = local_resources or {}
    nesting_orders = nesting_orders or {}
    schemas = make_schemas(schemas_spec, ret_list=True)
    tasks = []
    for s_idx, s in enumerate(schemas):
        inputs = [
            hf.InputValue(hf.Parameter(i), value=int(i[1:]) * 100)
            for i in local_inputs.get(s_idx, [])
        ]
        seqs = [
            hf.ValueSequence(
                path=i[0],
                values=[(int(i[0].split(".")[1][1:]) * 100) + j for j in range(i[1])],
                nesting_order=i[2],
            )
            for i in local_sequences.get(s_idx, [])
        ]
        res = {k: v for k, v in local_resources.get(s_idx, {}).items()}

        task = hf.Task(
            schema=s,
            inputs=inputs,
            sequences=seqs,
            resources=res,
            nesting_order=nesting_orders.get(s_idx, {}),
        )
        tasks.append(task)
    return tasks


def make_workflow(
    schemas_spec,
    path,
    local_inputs=None,
    local_sequences=None,
    local_resources=None,
    nesting_orders=None,
    resources=None,
    name="w1",
    overwrite=False,
    store="zarr",
):
    tasks = make_tasks(
        schemas_spec,
        local_inputs=local_inputs,
        local_sequences=local_sequences,
        local_resources=local_resources,
        nesting_orders=nesting_orders,
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name=name, tasks=tasks, resources=resources),
        path=path,
        name=name,
        overwrite=overwrite,
        store=store,
    )
    return wk


@dataclass
class P1_sub_parameter_cls(ParameterValue):
    _typ = "p1_sub"

    e: int

    def CLI_format(self) -> str:
        return str(self.e)

    @property
    def twice_e(self):
        return self.e * 2

    def prepare_JSON_dump(self) -> Dict:
        return {"e": self.e}

    def dump_to_HDF5_group(self, group):
        group.attrs["e"] = self.e


@dataclass
class P1_sub_parameter_cls_2(ParameterValue):
    _typ = "p1_sub_2"

    f: int


@dataclass
class P1_parameter_cls(ParameterValue):
    _typ = "p1c"
    _sub_parameters = {"sub_param": "p1_sub", "sub_param_2": "p1_sub_2"}

    a: int
    d: Optional[int] = None
    sub_param: Optional[P1_sub_parameter_cls] = None

    def __post_init__(self):
        if self.sub_param is not None and not isinstance(
            self.sub_param, P1_sub_parameter_cls
        ):
            self.sub_param = P1_sub_parameter_cls(**self.sub_param)

    @classmethod
    def from_data(cls, b, c):
        return cls(a=b + c)

    @classmethod
    def from_file(cls, path):
        with Path(path).open("rt") as fh:
            lns = fh.readlines()
            a = int(lns[0])
        return cls(a=a)

    @property
    def twice_a(self):
        return self.a * 2

    @property
    def sub_param_prop(self):
        return P1_sub_parameter_cls(e=4 * self.a)

    def CLI_format(self) -> str:
        return str(self.a)

    @staticmethod
    def CLI_format_group(*objs) -> str:
        pass

    @staticmethod
    def sum(*objs, **kwargs) -> str:
        return str(sum(i.a for i in objs))

    def custom_CLI_format(
        self, add: Optional[str] = None, sub: Optional[str] = None
    ) -> str:
        add = 4 if add is None else int(add)
        sub = 0 if sub is None else int(sub)
        return str(self.a + add - sub)

    def custom_CLI_format_prep(self, reps: Optional[str] = None) -> List[int]:
        """Used for testing custom object CLI formatting.

        For example, with a command like this:

        `<<join[delim=","](parameter:p1c.custom_CLI_format_prep(reps=4))>>`.

        """
        reps = 1 if reps is None else int(reps)
        return [self.a] * reps

    @classmethod
    def CLI_parse(cls, a_str: str, double: Optional[str] = "", e: Optional[str] = None):
        a = int(a_str)
        if double.lower() == "true":
            a *= 2
        if e:
            sub_param = P1_sub_parameter_cls(e=int(e))
        else:
            sub_param = None
        return cls(a=a, sub_param=sub_param)

    def prepare_JSON_dump(self) -> Dict:
        sub_param_js = self.sub_param.prepare_JSON_dump() if self.sub_param else None
        return {"a": self.a, "d": self.d, "sub_param": sub_param_js}

    def dump_to_HDF5_group(self, group):
        group.attrs["a"] = self.a
        if self.d is not None:
            group.attrs["d"] = self.d
        if self.sub_param:
            sub_group = group.add_group("sub_param")
            self.sub_param.dump_to_HDF5_group(sub_group)

    @classmethod
    def save_from_JSON(cls, data, param_id: int, workflow):
        obj = cls(**data)  # TODO: pass sub-param
        workflow.set_parameter_value(param_id=param_id, value=obj, commit=True)

    @classmethod
    def save_from_HDF5_group(cls, group, param_id: int, workflow):
        a = group.attrs["a"].item()
        if "d" in group.attrs:
            d = group.attrs["d"].item()
        else:
            d = None
        if "sub_param" in group:
            sub_group = group.get("sub_param")
            e = sub_group.attrs["e"].item()
            sub_param = P1_sub_parameter_cls(e=e)
        else:
            sub_param = None
        obj = cls(a=a, d=d, sub_param=sub_param)
        workflow.set_parameter_value(param_id=param_id, value=obj, commit=True)
