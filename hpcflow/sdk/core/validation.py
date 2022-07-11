from importlib import resources

from valida import Schema


def get_schema(filename):
    """Get a valida `Schema` object from the embedded data directory."""
    with resources.open_text("hpcflow.sdk.data", filename) as fh:
        schema_dat = fh.read()
    schema = Schema.from_yaml(schema_dat)
    return schema
