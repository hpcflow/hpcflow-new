"""Click CLI options that are used as decorators in multiple modules."""

import click

from hpcflow.sdk.core import ALL_TEMPLATE_FORMATS
from hpcflow.sdk.persistence import ALL_STORE_FORMATS, DEFAULT_STORE_FORMAT


def sub_tasks_callback(ctx, param, value):
    if value:
        return [int(i) for i in value.split(",")]


format_option = click.option(
    "--format",
    type=click.Choice(ALL_TEMPLATE_FORMATS),
    default=None,
    help=(
        'If specified, one of "json" or "yaml". This forces parsing from a '
        "particular format."
    ),
)
path_option = click.option(
    "--path",
    type=click.Path(exists=True),
    help="The directory path into which the new workflow will be generated.",
)
name_option = click.option(
    "--name",
    help=(
        "The name of the workflow. If specified, the workflow directory will be "
        "`path` joined with `name`. If not specified the workflow template name "
        "will be used, in combination with a date-timestamp."
    ),
)
overwrite_option = click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help=(
        "If True and the workflow directory (`path` + `name`) already exists, "
        "the existing directory will be overwritten."
    ),
)
store_option = click.option(
    "--store",
    type=click.Choice(ALL_STORE_FORMATS),
    help="The persistent store type to use.",
    default=DEFAULT_STORE_FORMAT,
)
ts_fmt_option = click.option(
    "--ts-fmt",
    help=(
        "The datetime format to use for storing datetimes. Datetimes are always "
        "stored in UTC (because Numpy does not store time zone info), so this "
        "should not include a time zone name."
    ),
)
ts_name_fmt_option = click.option(
    "--ts-name-fmt",
    help=(
        "The datetime format to use when generating the workflow name, where it "
        "includes a timestamp."
    ),
)
variables_option = click.option(
    "-v",
    "--var",
    "variables",
    type=(str, str),
    multiple=True,
    help=(
        "Workflow template variable value to be substituted in to the template file or "
        "string. Multiple variable values can be specified."
    ),
)
js_parallelism_option = click.option(
    "--js-parallelism",
    help=(
        "If True, allow multiple jobscripts to execute simultaneously. Raises if "
        "set to True but the store type does not support the "
        "`jobscript_parallelism` feature. If not set, jobscript parallelism will "
        "be used if the store type supports it."
    ),
    type=click.BOOL,
)
wait_option = click.option(
    "--wait",
    help=("If True, this command will block until the workflow execution is complete."),
    is_flag=True,
    default=False,
)
add_to_known_opt = click.option(
    "--add-to-known/--no-add-to-known",
    default=True,
    help="If True, add this submission to the known-submissions file.",
)
print_idx_opt = click.option(
    "--print-idx",
    help="If True, print the submitted jobscript indices for each submission index.",
    is_flag=True,
    default=False,
)
tasks_opt = click.option(
    "--tasks",
    help=(
        "List of comma-separated task indices to include in this submission. By default "
        "all tasks are included."
    ),
    callback=sub_tasks_callback,
)
cancel_opt = click.option(
    "--cancel",
    help="Immediately cancel the submission. Useful for testing and benchmarking.",
    is_flag=True,
    default=False,
)
submit_status_opt = click.option(
    "--status/--no-status",
    help="If True, display a live status to track submission progress.",
    default=True,
)
make_status_opt = click.option(
    "--status/--no-status",
    help="If True, display a live status to track workflow creation progress.",
    default=True,
)
zip_path_opt = click.option(
    "--path",
    default=".",
    help=(
        "Path at which to create the new zipped workflow. If this is an existing "
        "directory, the zip file will be created within this directory. Otherwise, this "
        "path is assumed to be the full file path to the new zip file."
    ),
)
zip_overwrite_opt = click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, any existing file will be overwritten.",
)
zip_log_opt = click.option("--log", help="Path to a log file to use during zipping.")
unzip_path_opt = click.option(
    "--path",
    default=".",
    help=(
        "Path at which to create the new unzipped workflow. If this is an existing "
        "directory, the new workflow directory will be created within this directory. "
        "Otherwise, this path will represent the new workflow directory path."
    ),
)
unzip_log_opt = click.option("--log", help="Path to a log file to use during unzipping.")
