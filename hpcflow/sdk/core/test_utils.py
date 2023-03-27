from typing import List, Tuple, Union
from hpcflow.api import (
    Action,
    ActionEnvironment,
    Command,
    Environment,
    FileSpec,
    InputValue,
    OutputFileParser,
    Parameter,
    SchemaInput,
    SchemaOutput,
    Task,
    TaskSchema,
    ValueSequence,
    Workflow,
    WorkflowTemplate,
)


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
                OutputFileParser(
                    output=Parameter(out_i),
                    output_files=[FileSpec(label="file1", name="file1.txt")],
                )
                for out_i in outs_i[2:]
            ]
        cmd = Command(
            " ".join(f"<<parameter:{i}>>" for i in ins_i.keys()),
            stdout=stdout,
            stderr=stderr,
        )

        act_i = Action(
            commands=[cmd],
            output_file_parsers=out_file_parsers,
            environments=[ActionEnvironment(Environment(name="env_1"))],
        )
        out.append(
            TaskSchema(
                objective=obj,
                actions=[act_i],
                inputs=[SchemaInput(k, default_value=v) for k, v in ins_i.items()],
                outputs=[SchemaOutput(k) for k in outs_i],
            )
        )
    if len(ins_outs) == 1 and not ret_list:
        out = out[0]
    return out


def make_parameters(num):
    return [Parameter(f"p{i + 1}") for i in range(num)]


def make_actions(ins_outs: List[Tuple[Union[Tuple, str], str]]) -> List[Action]:

    env = Environment("env1")
    act_env = ActionEnvironment(environment=env)
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
        act = Action(
            commands=[Command(cmd_str, stdout=stdout, stderr=stderr)],
            environments=[act_env],
        )
        actions.append(act)
    return actions


def make_tasks(
    schemas_spec,
    local_inputs=None,
    local_sequences=None,
    nesting_orders=None,
):
    local_inputs = local_inputs or {}
    local_sequences = local_sequences or {}
    nesting_orders = nesting_orders or {}
    schemas = make_schemas(schemas_spec, ret_list=True)
    tasks = []
    for s_idx, s in enumerate(schemas):
        inputs = [
            InputValue(Parameter(i), value=int(i[1:]) * 100)
            for i in local_inputs.get(s_idx, [])
        ]
        seqs = [
            ValueSequence(
                path=i[0],
                values=[(int(i[0].split(".")[1][1:]) * 100) + j for j in range(i[1])],
                nesting_order=i[2],
            )
            for i in local_sequences.get(s_idx, [])
        ]

        task = Task(
            schemas=[s],
            inputs=inputs,
            sequences=seqs,
            nesting_order=nesting_orders.get(s_idx, {}),
        )
        tasks.append(task)
    return tasks


def make_workflow(
    schemas_spec,
    path,
    local_inputs=None,
    local_sequences=None,
    nesting_orders=None,
    name="w1",
    store="zarr",
):
    tasks = make_tasks(
        schemas_spec,
        local_inputs=local_inputs,
        local_sequences=local_sequences,
        nesting_orders=nesting_orders,
    )
    wk = Workflow.from_template(
        WorkflowTemplate(name=name, tasks=tasks), path=path, store=store
    )
    return wk
