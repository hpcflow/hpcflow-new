from importlib import resources

from valida import Schema


def get_schema(filename):
    with resources.open_text("hpcflow.data", filename) as fh:
        schema_dat = fh.read()
    schema = Schema.from_yaml(schema_dat)
    return schema
