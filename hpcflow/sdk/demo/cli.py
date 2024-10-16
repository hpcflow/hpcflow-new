"""
CLI components for demonstration code.
"""
from __future__ import annotations
from pathlib import Path
from random import randint
from typing import TYPE_CHECKING
import click

from hpcflow.sdk.core.utils import get_process_stamp
from hpcflow.sdk.cli_common import (
    format_option,
    path_option,
    name_option,
    overwrite_option,
    store_option,
    ts_fmt_option,
    ts_name_fmt_option,
    variables_option,
    js_parallelism_option,
    wait_option,
    add_to_known_opt,
    print_idx_opt,
    tasks_opt,
    cancel_opt,
    submit_status_opt,
    make_status_opt,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from ..app import BaseApp


def get_demo_software_CLI(app: BaseApp):
    """Generate the CLI to provide an example software."""

    @click.group()
    def demo_software():
        pass

    @demo_software.command("doSomething")
    @click.option("--infile1", "-i1", type=click.Path(exists=True), required=True)
    @click.option("--infile2", "-i2", type=click.Path(exists=True), required=True)
    @click.option("--value", "-v")
    @click.option("--out", "-o")
    def demo_do_something(
        infile1: Path, infile2: Path, value: str | None = None, out: str | None = None
    ):
        click.echo("trying to do something")

        with Path(infile1).open("r") as handle:
            file_id_1 = int(handle.readline().strip())
        with Path(infile2).open("r") as handle:
            file_id_2 = int(handle.readline().strip())

        if out is None:
            out = "outfile.txt"
        out_path = Path(out)
        with out_path.open("a") as handle:
            handle.write("{}\n".format(randint(0, int(1e6))))
            handle.write(
                "{} Generated by `doSomething --infile1 {} --infile2 {}`.\n".format(
                    get_process_stamp(), infile1, infile2
                )
            )
            if value:
                handle.write("{} Value: {}\n".format(get_process_stamp(), value))
            handle.write(
                "{} Original file ID: {}: {}\n".format(
                    get_process_stamp(), infile1, file_id_1
                )
            )
            handle.write(
                "{} Original file ID: {}: {}\n".format(
                    get_process_stamp(), infile2, file_id_2
                )
            )

    return demo_software


def get_demo_workflow_CLI(app: BaseApp):
    """Generate the CLI to provide access to builtin demo workflows."""

    def list_callback(ctx: click.Context, param, value: bool):
        if not value or ctx.resilient_parsing:
            return
        # TODO: format with Rich with a one-line description
        click.echo("\n".join(app.list_demo_workflows()))
        ctx.exit()

    @click.group()
    @click.option(
        "-l",
        "--list",
        help="Print available builtin demo workflows.",
        is_flag=True,
        is_eager=True,
        expose_value=False,
        callback=list_callback,
    )
    def demo_workflow():
        """Interact with builtin demo workflows."""
        pass

    @demo_workflow.command("make")
    @click.argument("workflow_name")
    @format_option
    @path_option
    @name_option
    @overwrite_option
    @store_option
    @ts_fmt_option
    @ts_name_fmt_option
    @variables_option
    @make_status_opt
    def make_demo_workflow(
        workflow_name: str,
        format: str | None,
        path: Path | None,
        name: str | None,
        overwrite: bool,
        store: str,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        variables: Iterable[tuple[str, str]] = (),
        status: bool = True,
    ):
        wk = app.make_demo_workflow(
            workflow_name=workflow_name,
            template_format=format,
            path=path,
            name=name,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            variables=dict(variables),
            status=status,
        )
        click.echo(wk.path)

    @demo_workflow.command("go")
    @click.argument("workflow_name")
    @format_option
    @path_option
    @name_option
    @overwrite_option
    @store_option
    @ts_fmt_option
    @ts_name_fmt_option
    @variables_option
    @js_parallelism_option
    @wait_option
    @add_to_known_opt
    @print_idx_opt
    @tasks_opt
    @cancel_opt
    @submit_status_opt
    def make_and_submit_demo_workflow(
        workflow_name: str,
        format: str | None,
        path: Path | None,
        name: str | None,
        overwrite: bool,
        store: str,
        ts_fmt: str | None = None,
        ts_name_fmt: str | None = None,
        variables: Iterable[tuple[str, str]] = (),
        js_parallelism: bool | None = None,
        wait: bool = False,
        add_to_known: bool = True,
        print_idx: bool = False,
        tasks: list[int] | None = None,
        cancel: bool = False,
        status: bool = True,
    ):
        out = app.make_and_submit_demo_workflow(
            workflow_name=workflow_name,
            template_format=format,
            path=path,
            name=name,
            overwrite=overwrite,
            store=store,
            ts_fmt=ts_fmt,
            ts_name_fmt=ts_name_fmt,
            variables=dict(variables),
            JS_parallelism=js_parallelism,
            wait=wait,
            add_to_known=add_to_known,
            return_idx=print_idx,
            tasks=tasks,
            cancel=cancel,
            status=status,
        )
        if print_idx:
            assert isinstance(out, tuple)
            click.echo(out[1])

    @demo_workflow.command("copy")
    @click.argument("workflow_name")
    @click.argument("destination")
    @click.option("--doc/--no-doc", default=True)
    def copy_demo_workflow(workflow_name: str, destination: str, doc: bool):
        app.copy_demo_workflow(name=workflow_name, dst=destination, doc=doc)

    @demo_workflow.command("show")
    @click.argument("workflow_name")
    @click.option("--syntax/--no-syntax", default=True)
    @click.option("--doc/--no-doc", default=True)
    def show_demo_workflow(workflow_name: str, syntax: bool, doc: bool):
        app.show_demo_workflow(workflow_name, syntax=syntax, doc=doc)

    return demo_workflow
