from typing import Dict, List
import click
from colorama import init as colorama_init
from termcolor import colored

from hpcflow import __version__, _app_name
from hpcflow.sdk.config.cli import get_config_CLI
from hpcflow.sdk.config.errors import ConfigError
from hpcflow.sdk.core import ALL_TEMPLATE_FORMATS, DEFAULT_TEMPLATE_FORMAT, utils
from hpcflow.sdk.demo.cli import get_demo_software_CLI
from hpcflow.sdk.helper.cli import get_helper_CLI
from hpcflow.sdk.persistence import ALL_STORE_FORMATS, DEFAULT_STORE_FORMAT
from hpcflow.sdk.submission.shells import ALL_SHELLS

string_option = click.option(
    "--string",
    is_flag=True,
    default=False,
    help="Determines if passing a file path or a string.",
)
format_option = click.option(
    "--format",
    type=click.Choice(ALL_TEMPLATE_FORMATS),
    default=DEFAULT_TEMPLATE_FORMAT,
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
workflow_ref_type_opt = click.option(
    "--ref-type",
    "-r",
    type=click.Choice(["assume-id", "id", "path"]),
    default="assume-id",
)


def parse_jobscript_wait_spec(sub_js_idx: str) -> Dict[int, List[int]]:
    sub_js_idx_dct = {}
    for sub_i in sub_js_idx.split(";"):
        sub_idx_str, js_idx_lst_str = sub_i.split(":")
        sub_js_idx_dct[int(sub_idx_str)] = [int(i) for i in js_idx_lst_str.split(",")]
    return sub_js_idx_dct


def _make_API_CLI(app):
    """Generate the CLI for the main functionality."""

    @click.command(name="make")
    @click.argument("template_file_or_str")
    @string_option
    @format_option
    @path_option
    @name_option
    @overwrite_option
    @store_option
    @ts_fmt_option
    @ts_name_fmt_option
    def make_workflow(
        template_file_or_str,
        string,
        format,
        path,
        name,
        overwrite,
        store,
        ts_fmt=None,
        ts_name_fmt=None,
    ):
        """Generate a new {app_name} workflow.

        TEMPLATE_FILE_OR_STR is either a path to a template file in YAML or JSON
        format, or a YAML/JSON string.

        """
        wk = app.make_workflow(
            template_file_or_str=template_file_or_str,
            is_string=string,
            template_format=format,
            path=path,
            name=name,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
        )
        click.echo(wk.path)

    @click.command(name="go")
    @click.argument("template_file_or_str")
    @string_option
    @format_option
    @path_option
    @name_option
    @overwrite_option
    @store_option
    @ts_fmt_option
    @ts_name_fmt_option
    @js_parallelism_option
    @wait_option
    def make_and_submit_workflow(
        template_file_or_str,
        string,
        format,
        path,
        name,
        overwrite,
        store,
        ts_fmt=None,
        ts_name_fmt=None,
        js_parallelism=None,
        wait=False,
    ):
        """Generate and submit a new {app_name} workflow.

        TEMPLATE_FILE_OR_STR is either a path to a template file in YAML or JSON
        format, or a YAML/JSON string.

        """
        app.make_and_submit_workflow(
            template_file_or_str=template_file_or_str,
            is_string=string,
            template_format=format,
            path=path,
            name=name,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            JS_parallelism=js_parallelism,
            wait=wait,
        )

    @click.command(context_settings={"ignore_unknown_options": True})
    @click.argument("py_test_args", nargs=-1, type=click.UNPROCESSED)
    @click.pass_context
    def test(ctx, py_test_args):
        """Run {app_name} test suite.

        PY_TEST_ARGS are arguments passed on to Pytest.

        """
        ctx.exit(app.run_tests(*py_test_args))

    @click.command(context_settings={"ignore_unknown_options": True})
    @click.argument("py_test_args", nargs=-1, type=click.UNPROCESSED)
    @click.pass_context
    def test_hpcflow(ctx, py_test_args):
        """Run hpcflow test suite.".

        PY_TEST_ARGS are arguments passed on to Pytest.

        """
        ctx.exit(app.run_hpcflow_tests(*py_test_args))

    commands = [
        make_workflow,
        make_and_submit_workflow,
        test,
    ]
    for cmd in commands:
        if cmd.help:
            cmd.help = cmd.help.format(app_name=app.name)

    if app.name != "hpcflow":
        # `test_hpcflow` is the same as `test` for the hpcflow app no need to add both:
        commands.append(test_hpcflow)

    return commands


def _make_workflow_submission_jobscript_CLI(app):
    """Generate the CLI for interacting with existing workflow submission
    jobscripts."""

    @click.group(name="js")
    @click.pass_context
    @click.argument("js_idx", type=click.INT)
    def jobscript(ctx, js_idx):
        """Interact with existing {app_name} workflow submission jobscripts.

        JS_IDX is the jobscript index within the submission object.

        """
        ctx.obj["jobscript"] = ctx.obj["submission"].jobscripts[js_idx]

    @jobscript.command(name="res")
    @click.pass_context
    def resources(ctx):
        """Get resources associated with this jobscript."""
        click.echo(ctx.obj["jobscript"].resources.__dict__)

    @jobscript.command(name="deps")
    @click.pass_context
    def dependencies(ctx):
        """Get jobscript dependencies."""
        click.echo(ctx.obj["jobscript"].dependencies)

    @jobscript.command()
    @click.pass_context
    def path(ctx):
        """Get the file path to the jobscript."""
        click.echo(ctx.obj["jobscript"].jobscript_path)

    @jobscript.command()
    @click.pass_context
    def show(ctx):
        """Show the jobscript file."""
        with ctx.obj["jobscript"].jobscript_path.open("rt") as fp:
            click.echo(fp.read())

    jobscript.help = jobscript.help.format(app_name=app.name)

    return jobscript


def _make_workflow_submission_CLI(app):
    """Generate the CLI for interacting with existing workflow submissions."""

    @click.group(name="sub")
    @click.pass_context
    @click.argument("sub_idx", type=click.INT)
    def submission(ctx, sub_idx):
        """Interact with existing {app_name} workflow submissions.

        SUB_IDX is the submission index.

        """
        ctx.obj["submission"] = ctx.obj["workflow"].submissions[sub_idx]

    @submission.command("status")
    @click.pass_context
    def status(ctx):
        """Get the submission status."""
        click.echo(ctx.obj["submission"].status.name.lower())

    @submission.command("submitted-js")
    @click.pass_context
    def submitted_JS(ctx):
        """Get a list of jobscript indices that have been submitted."""
        click.echo(ctx.obj["submission"].submitted_jobscripts)

    @submission.command("outstanding-js")
    @click.pass_context
    def outstanding_JS(ctx):
        """Get a list of jobscript indices that have not yet been submitted."""
        click.echo(ctx.obj["submission"].outstanding_jobscripts)

    @submission.command("needs-submit")
    @click.pass_context
    def needs_submit(ctx):
        """Check if this submission needs submitting."""
        click.echo(ctx.obj["submission"].needs_submit)

    submission.help = submission.help.format(app_name=app.name)
    submission.add_command(_make_workflow_submission_jobscript_CLI(app))

    return submission


def _make_workflow_CLI(app):
    """Generate the CLI for interacting with existing workflows."""

    @click.group()
    @click.argument("workflow_path", type=click.Path(exists=True))
    @click.pass_context
    def workflow(ctx, workflow_path):
        """Interact with existing {app_name} workflows.

        WORKFLOW_PATH is the path to an existing workflow.

        """
        wk = app.Workflow(workflow_path)
        ctx.ensure_object(dict)
        ctx.obj["workflow"] = wk

    @workflow.command(name="submit")
    @js_parallelism_option
    @wait_option
    @click.pass_context
    def submit_workflow(ctx, js_parallelism=None, wait=False):
        """Submit the workflow."""
        ctx.obj["workflow"].submit(JS_parallelism=js_parallelism, wait=wait)

    @workflow.command(name="wait")
    @click.argument("sub_js_idx")
    @click.pass_context
    def wait(ctx, sub_js_idx):
        ctx.obj["workflow"].wait(parse_jobscript_wait_spec(sub_js_idx))

    @workflow.command(name="get-param")
    @click.argument("index", type=click.INT)
    @click.pass_context
    def get_parameter(ctx, index):
        """Get a parameter value by data index."""
        click.echo(ctx.obj["workflow"].get_parameter_data(index))

    @workflow.command(name="get-param-source")
    @click.argument("index", type=click.INT)
    @click.pass_context
    def get_parameter_source(ctx, index):
        """Get a parameter source by data index."""
        click.echo(ctx.obj["workflow"].get_parameter_source(index))

    @workflow.command(name="get-all-params")
    @click.pass_context
    def get_all_parameters(ctx):
        """Get all parameter values."""
        click.echo(ctx.obj["workflow"].get_all_parameter_data())

    @workflow.command(name="is-param-set")
    @click.argument("index", type=click.INT)
    @click.pass_context
    def is_parameter_set(ctx, index):
        """Check if a parameter specified by data index is set."""
        click.echo(ctx.obj["workflow"].is_parameter_set(index))

    @workflow.command(name="show-all-status")
    @click.pass_context
    def show_all_EAR_statuses(ctx):
        """Show the submission status of all workflow EARs."""
        ctx.obj["workflow"].show_all_EAR_statuses()

    workflow.help = workflow.help.format(app_name=app.name)

    workflow.add_command(_make_workflow_submission_CLI(app))

    return workflow


def _make_submission_CLI(app):
    """Generate the CLI for submission related queries."""

    def OS_info_callback(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        click.echo(app.get_OS_info())
        ctx.exit()

    @click.group()
    @click.option(
        "--os-info",
        help="Print information about the operating system.",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=OS_info_callback,
    )
    @click.pass_context
    def submission(ctx):
        """Submission-related queries."""

    @submission.command("shell-info")
    @click.argument("shell_name", type=click.Choice(ALL_SHELLS))
    @click.option("--exclude-os", is_flag=True, default=False)
    @click.pass_context
    def shell_info(ctx, shell_name, exclude_os):
        click.echo(app.get_shell_info(shell_name, exclude_os))
        ctx.exit()

    return submission


def _make_internal_CLI(app):
    """Generate the CLI for internal use."""

    @click.group()
    def internal(help=True):  # TEMP
        """Internal CLI to be invoked by scripts generated by the app."""
        pass

    @internal.group()
    @click.argument("path", type=click.Path(exists=True))
    @click.pass_context
    def workflow(ctx, path):
        """"""
        wk = app.Workflow(path)
        ctx.ensure_object(dict)
        ctx.obj["workflow"] = wk

    @workflow.command()
    @click.pass_context
    @click.argument("submission_idx", type=click.INT)
    @click.argument("jobscript_idx", type=click.INT)
    @click.argument("js_action_idx", type=click.INT)
    @click.argument("ear_id", type=click.INT)
    def write_commands(
        ctx,
        submission_idx: int,
        jobscript_idx: int,
        js_action_idx: int,
        ear_id: int,
    ):
        app.CLI_logger.info(f"write commands for EAR ID {ear_id!r}.")
        ctx.exit(
            ctx.obj["workflow"].write_commands(
                submission_idx,
                jobscript_idx,
                js_action_idx,
                ear_id,
            )
        )

    @workflow.command()
    @click.pass_context
    @click.argument("name")
    @click.argument("value")
    @click.argument("ear_id", type=click.INT)
    def save_parameter(
        ctx,
        name: str,
        value: str,
        ear_id: int,
    ):
        app.CLI_logger.info(f"save parameter {name!r} for EAR ID {ear_id!r}.")
        ctx.exit(ctx.obj["workflow"].save_parameter(name, value, ear_id))

    @workflow.command()
    @click.pass_context
    @click.argument("ear_id", type=click.INT)
    def set_EAR_start(ctx, ear_id: int):
        app.CLI_logger.info(f"set EAR start for EAR ID {ear_id!r}.")
        ctx.exit(ctx.obj["workflow"].set_EAR_start(ear_id))

    @workflow.command()
    @click.pass_context
    @click.argument("ear_id", type=click.INT)
    @click.argument("exit_code", type=click.INT)
    def set_EAR_end(ctx, ear_id: int, exit_code: int):
        app.CLI_logger.info(
            f"set EAR end for EAR ID {ear_id!r} with exit code {exit_code!r}."
        )
        ctx.exit(ctx.obj["workflow"].set_EAR_end(ear_id, exit_code))

    @workflow.command()
    @click.pass_context
    @click.argument("ear_id", type=click.INT)
    def set_EAR_skip(ctx, ear_id: int):
        app.CLI_logger.info(f"set EAR skip for EAR ID {ear_id!r}.")
        ctx.exit(ctx.obj["workflow"].set_EAR_skip(ear_id))

    @workflow.command()
    @click.pass_context
    @click.argument("ear_id", type=click.INT)
    def get_EAR_skipped(ctx, ear_id: int):
        """Return 1 if the given EAR is to be skipped, else return 0."""
        app.CLI_logger.info(f"get EAR skip for EAR ID {ear_id!r}.")
        click.echo(int(ctx.obj["workflow"].get_EAR_skipped(ear_id)))

    @workflow.command()
    @click.pass_context
    @click.argument("submission_idx", type=click.INT)
    @click.argument("jobscript_idx", type=click.INT)
    def launch_direct_win_js(ctx, submission_idx, jobscript_idx):
        """fuck me."""
        js = ctx.obj["workflow"].submissions[submission_idx].jobscripts[jobscript_idx]
        js._launch_direct_js(write_pid=True)

    # TODO: in general, maybe the workflow command group can expose the simple Workflow
    # properties; maybe use a decorator on the Workflow property object to signify
    # inclusion?

    return internal


def _make_template_components_CLI(app):
    @click.command()
    def tc(help=True):
        """For showing template component data."""
        click.echo(app.template_components)

    return tc


def _make_show_CLI(app):
    @click.command()
    @click.option(
        "--max-recent",
        default=3,
        help="If True, show only recently finished workflows",
    )
    @click.option(
        "--no-update",
        is_flag=True,
        default=False,
        help=(
            "If True, do not update the known-submissions file to remove workflows that "
            "are no longer running."
        ),
    )
    @click.option(
        "-f",
        "--full",
        is_flag=True,
        default=False,
        help="Allow multiple lines per workflow submission.",
    )
    def show(max_recent, full, no_update):
        """Show information about recent workflows."""
        app.show(max_recent=max_recent, full=full, no_update=no_update)

    return show


def _make_zip_CLI(app):
    @click.command(name="zip")
    @click.argument("workflow_ref")
    @click.option("--log")
    @workflow_ref_type_opt
    def zip_workflow(workflow_ref, ref_type, log=None):
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        wk = app.Workflow(workflow_path)
        wk.to_zip(log=log)

    return zip_workflow


def _make_cancel_CLI(app):
    @click.command()
    @click.argument("workflow_ref")
    @workflow_ref_type_opt
    def cancel(workflow_ref, ref_type):
        app.cancel(workflow_ref, ref_type)

    return cancel


def _make_open_CLI(app):
    @click.group(name="open")
    def open_file():
        """Open a file (e.g. {app_name}'s log file) using the default application."""

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def log(path=False):
        """Open the {app_name} log file."""
        file_path = app.config.get("log_file_path")
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def config(path=False):
        """Open the {app_name} config file."""
        file_path = app.config.get("config_file_path")
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @open_file.command()
    @click.option("--name")
    @click.option("--path", is_flag=True, default=False)
    def env_source(name=None, path=False):
        """Open a named environment sources file, or the first one."""
        sources = app.config.get("environment_sources")
        if not sources:
            raise ValueError("No environment sources specified in the config file.")
        file_paths = []
        if not name:
            file_paths = [sources[0]]
        else:
            for i in sources:
                if i.name == name:
                    file_paths.append(i)
        if not file_paths:
            raise ValueError(
                f"No environment source named {name!r} could be found; available "
                f"environment source files have names: {[i.name for i in sources]!r}"
            )

        assert len(file_paths) < 5  # don't open a stupid number of files
        for i in file_paths:
            if path:
                click.echo(i)
            else:
                utils.open_file(i)

    @open_file.command()
    @click.option("--path", is_flag=True, default=False)
    def known_subs(path=False):
        """Open the known-submissions text file."""
        file_path = app.known_subs_file_path
        if path:
            click.echo(file_path)
        else:
            utils.open_file(file_path)

    @open_file.command()
    @click.argument("workflow_ref")
    @workflow_ref_type_opt
    def workflow(workflow_ref, ref_type):
        """Open a workflow directory in e.g. File Explorer in Windows."""
        workflow_path = app._resolve_workflow_reference(workflow_ref, ref_type)
        utils.open_file(workflow_path)

    return open_file


def make_cli(app):
    """Generate the root CLI for the app."""

    colorama_init(autoreset=True)

    def run_time_info_callback(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        click.echo(str(app.run_time_info))
        ctx.exit()

    def clear_known_subs_file_callback(ctx, param, value):
        if not value or ctx.resilient_parsing:
            return
        app.clear_known_submissions_file()
        ctx.exit()

    @click.group(name=app.name)
    @click.version_option(
        version=app.version,
        package_name=app.name,
        prog_name=app.name,
        help=f"Show the version of {app.name} and exit.",
    )
    @click.version_option(
        __version__,
        "--hpcflow-version",
        help="Show the version of hpcflow and exit.",
        package_name="hpcflow",
        prog_name=_app_name,
    )
    @click.help_option()
    @click.option(
        "--run-time-info",
        help="Print run-time information!",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=run_time_info_callback,
    )
    @click.option(
        "--clear-known-subs",
        help="Delete the contents of the known-submissions file",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=clear_known_subs_file_callback,
    )
    @click.option("--config-dir", help="Set the configuration directory.")
    @click.option("--config-invocation-key", help="Set the configuration invocation key.")
    @click.option(
        "--with-config",
        help="Override a config item in the config file",
        nargs=2,
        multiple=True,
    )
    @click.pass_context
    def new_CLI(ctx, config_dir, config_invocation_key, with_config):
        overrides = {kv[0]: kv[1] for kv in with_config}
        try:
            app.load_config(
                config_dir=config_dir,
                config_invocation_key=config_invocation_key,
                **overrides,
            )
        except ConfigError as err:
            click.echo(f"{colored(err.__class__.__name__, 'red')}: {err}")
            ctx.exit(1)

    new_CLI.__doc__ = app.description
    new_CLI.add_command(get_config_CLI(app))
    new_CLI.add_command(get_demo_software_CLI(app))
    new_CLI.add_command(get_helper_CLI(app))
    new_CLI.add_command(_make_workflow_CLI(app))
    new_CLI.add_command(_make_submission_CLI(app))
    new_CLI.add_command(_make_internal_CLI(app))
    new_CLI.add_command(_make_template_components_CLI(app))
    new_CLI.add_command(_make_show_CLI(app))
    new_CLI.add_command(_make_open_CLI(app))
    new_CLI.add_command(_make_cancel_CLI(app))
    new_CLI.add_command(_make_zip_CLI(app))
    for cli_cmd in _make_API_CLI(app):
        new_CLI.add_command(cli_cmd)

    return new_CLI
