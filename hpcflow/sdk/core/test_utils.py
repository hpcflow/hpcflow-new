from hpcflow.api import (
    Action,
    ActionEnvironment,
    Command,
    Environment,
    FileSpec,
    OutputFileParser,
    Parameter,
    SchemaInput,
    SchemaOutput,
    TaskSchema,
)


def make_schemas(ins_outs):
    out = []
    for idx, info in enumerate(ins_outs):

        if len(info) == 2:
            (ins_i, outs_i) = info
            obj = f"t{idx}"
        else:
            (ins_i, outs_i, obj) = info

        act_i = Action(
            commands=[Command(" ".join(f"<<parameter:{i}>>" for i in ins_i.keys()))],
            output_file_parsers=[
                OutputFileParser(
                    output=Parameter(out_i),
                    output_files=[FileSpec(label="file1", name="file1.txt")],
                )
                for out_i in outs_i
            ],
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
    if len(ins_outs) == 1:
        out = out[0]
    return out
