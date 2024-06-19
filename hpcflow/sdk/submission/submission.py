from __future__ import annotations
from collections import defaultdict

from datetime import datetime, timedelta
import enum
import os
from pathlib import Path
from textwrap import indent
from typing import overload, override, TYPE_CHECKING

from hpcflow.sdk.core.actions import ElementActionRun
from hpcflow.sdk.core.element import ElementResources
from hpcflow.sdk.core.errors import (
    JobscriptSubmissionFailure,
    MissingEnvironmentError,
    MissingEnvironmentExecutableError,
    MissingEnvironmentExecutableInstanceError,
    MultipleEnvironmentsError,
    SubmissionFailure,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.object_list import ObjectListMultipleMatchError
from hpcflow.sdk.core.utils import parse_timestamp
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.jobscript import Jobscript
if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from typing import Any, ClassVar, Literal
    from ..app import BaseApp
    from .jobscript import Jobscript, JobscriptElementState
    from .schedulers import Scheduler
    from .shells import Shell
    from ..core.element import ElementActionRun
    from ..core.environment import Environment
    from ..core.object_list import EnvironmentsList
    from ..core.workflow import Workflow


def timedelta_format(td: timedelta) -> str:
    days, seconds = td.days, td.seconds
    hours = seconds // (60 * 60)
    seconds -= hours * (60 * 60)
    minutes = seconds // 60
    seconds -= minutes * 60
    return f"{days}-{hours:02}:{minutes:02}:{seconds:02}"


def timedelta_parse(td_str: str) -> timedelta:
    days, other = td_str.split("-")
    days_i = int(days)
    hours, mins, secs = [int(i) for i in other.split(":")]
    return timedelta(days=days_i, hours=hours, minutes=mins, seconds=secs)


class SubmissionStatus(enum.Enum):
    PENDING = 0  # not yet submitted
    SUBMITTED = 1  # all jobscripts submitted successfully
    PARTIALLY_SUBMITTED = 2  # some jobscripts submitted successfully


class Submission(JSONLike):
    app: ClassVar[BaseApp]
    _child_objects = (
        ChildObjectSpec(
            name="jobscripts",
            class_name="Jobscript",
            is_multiple=True,
            parent_ref="_submission",
        ),
        ChildObjectSpec(
            name="environments",
            class_name="EnvironmentsList",
        ),
    )

    def __init__(
        self,
        index: int,
        jobscripts: list[Jobscript],
        workflow: Workflow | None = None,
        submission_parts: dict[str, list[int]] | None = None,
        JS_parallelism: bool | None = None,
        environments: EnvironmentsList | None = None,
    ):
        self._index = index
        self._jobscripts = jobscripts
        self._submission_parts = submission_parts or {}
        self._JS_parallelism = JS_parallelism
        self._environments = environments

        self._submission_parts_lst: list[dict[str, Any]] | None = None  # assigned on first access; datetime objects

        if workflow:
            self.workflow = workflow

        self._set_parent_refs()

        for js_idx, js in enumerate(self.jobscripts):
            js._index = js_idx

    @TimeIt.decorator
    def _set_environments(self) -> None:
        filterable = ElementResources.get_env_instance_filterable_attributes()

        # map required environments and executable labels to job script indices:
        req_envs: dict[tuple, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
        for js_idx, js_i in enumerate(self.jobscripts):
            for run in js_i.all_EARs:
                env_spec_h = tuple(zip(*run.env_spec.items()))  # hashable
                for exec_label_j in run.action.get_required_executables():
                    req_envs[env_spec_h][exec_label_j].add(js_idx)
                # Ensure overall element is present
                req_envs[env_spec_h]

        # check these envs/execs exist in app data:
        envs: list[Environment] = []
        app_envs: EnvironmentsList = self.app.envs
        for env_spec_h, exec_js in req_envs.items():
            env_spec = dict(zip(*env_spec_h))
            non_name_spec = {k: v for k, v in env_spec.items() if k != "name"}
            spec_str = f" with specifiers {non_name_spec!r}" if non_name_spec else ""
            env_ref = f"{env_spec['name']!r}{spec_str}"
            try:
                env_i = app_envs.get(**env_spec)
            except ObjectListMultipleMatchError:
                raise MultipleEnvironmentsError(
                    f"Multiple environments {env_ref} are defined on this machine."
                )
            except ValueError:
                raise MissingEnvironmentError(
                    f"The environment {env_ref} is not defined on this machine, so the "
                    f"submission cannot be created."
                ) from None
            else:
                if env_i not in envs:
                    envs.append(env_i)

            for exec_i_lab, js_idx_set in exec_js.items():
                try:
                    exec_i = env_i.executables.get(exec_i_lab)
                except ValueError:
                    raise MissingEnvironmentExecutableError(
                        f"The environment {env_ref} as defined on this machine has no "
                        f"executable labelled {exec_i_lab!r}, which is required for this "
                        f"submission, so the submission cannot be created."
                    ) from None

                # check matching executable instances exist:
                for js_idx_j in js_idx_set:
                    js_j = self.jobscripts[js_idx_j]
                    filter_exec = {j: getattr(js_j.resources, j) for j in filterable}
                    exec_instances = exec_i.filter_instances(**filter_exec)
                    if not exec_instances:
                        raise MissingEnvironmentExecutableInstanceError(
                            f"No matching executable instances found for executable "
                            f"{exec_i_lab!r} of environment {env_ref} for jobscript "
                            f"index {js_idx_j!r} with requested resources "
                            f"{filter_exec!r}."
                        )

        # save env definitions to the environments attribute:
        self._environments = self.app.EnvironmentsList(envs)

    @override
    def to_dict(self):
        dct = super().to_dict()
        del dct["_workflow"]
        del dct["_index"]
        del dct["_submission_parts_lst"]
        return {k.lstrip("_"): v for k, v in dct.items()}

    @property
    def index(self) -> int:
        return self._index

    @property
    def environments(self) -> EnvironmentsList:
        assert self._environments
        return self._environments

    @property
    def submission_parts(self) -> list[dict[str, Any]]:
        # FIXME: use a TypedDict
        if not self._submission_parts:
            return []

        if self._submission_parts_lst is None:
            self._submission_parts_lst = [
                {
                    "submit_time": parse_timestamp(dt, self.workflow.ts_fmt),
                    "jobscripts": js_idx,
                }
                for dt, js_idx in self._submission_parts.items()
            ]
        return self._submission_parts_lst

    @TimeIt.decorator
    def get_start_time(self, submit_time: str) -> datetime | None:
        """Get the start time of a given submission part."""
        times = (
            self.jobscripts[i].start_time
            for i in self._submission_parts[submit_time])
        return min((t for t in times if t is not None), default=None)

    @TimeIt.decorator
    def get_end_time(self, submit_time: str) -> datetime | None:
        """Get the end time of a given submission part."""
        times = (
            self.jobscripts[i].end_time
            for i in self._submission_parts[submit_time])
        return max((t for t in times if t is not None), default=None)

    @property
    @TimeIt.decorator
    def start_time(self) -> datetime | None:
        """Get the first non-None start time over all submission parts."""
        times = (
            self.get_start_time(submit_time)
            for submit_time in self._submission_parts)
        return min((t for t in times if t is not None), default=None)

    @property
    @TimeIt.decorator
    def end_time(self) -> datetime | None:
        """Get the final non-None end time over all submission parts."""
        times = (
            self.get_end_time(submit_time)
            for submit_time in self._submission_parts)
        return max((t for t in times if t is not None), default=None)

    @property
    def jobscripts(self) -> list[Jobscript]:
        return self._jobscripts

    @property
    def JS_parallelism(self) -> bool | None:
        return self._JS_parallelism

    @property
    def workflow(self) -> Workflow:
        return self._workflow

    @workflow.setter
    def workflow(self, wk: Workflow):
        self._workflow = wk

    @property
    def jobscript_indices(self) -> tuple[int, ...]:
        """All associated jobscript indices."""
        return tuple(i.index for i in self.jobscripts)

    @property
    def submitted_jobscripts(self) -> tuple[int, ...]:
        """Jobscript indices that have been successfully submitted."""
        return tuple(j for i in self.submission_parts for j in i["jobscripts"])

    @property
    def outstanding_jobscripts(self) -> tuple[int, ...]:
        """Jobscript indices that have not yet been successfully submitted."""
        return tuple(set(self.jobscript_indices) - set(self.submitted_jobscripts))

    @property
    def status(self) -> SubmissionStatus:
        if not self.submission_parts:
            return SubmissionStatus.PENDING
        elif set(self.submitted_jobscripts) == set(self.jobscript_indices):
            return SubmissionStatus.SUBMITTED
        else:
            return SubmissionStatus.PARTIALLY_SUBMITTED

    @property
    def needs_submit(self) -> bool:
        return self.status in (
            SubmissionStatus.PENDING,
            SubmissionStatus.PARTIALLY_SUBMITTED,
        )

    @property
    def path(self) -> Path:
        return self.workflow.submissions_path / str(self.index)

    @property
    def all_EAR_IDs(self) -> Iterable[int]:
        return (i for js in self.jobscripts for i in js.all_EAR_IDs)

    @property
    def all_EARs(self) -> Iterable[ElementActionRun]:
        return (i for js in self.jobscripts for i in js.all_EARs)

    @property
    @TimeIt.decorator
    def EARs_by_elements(self) -> Mapping[int, Mapping[int, Sequence[ElementActionRun]]]:
        task_elem_EARs: dict[int, dict[int, list[ElementActionRun]]] = \
            defaultdict(lambda: defaultdict(list))
        for i in self.all_EARs:
            task_elem_EARs[i.task.index][i.element.index].append(i)
        return task_elem_EARs

    @property
    def abort_EARs_file_name(self) -> str:
        return f"abort_EARs.txt"

    @property
    def abort_EARs_file_path(self) -> Path:
        return self.path / self.abort_EARs_file_name

    @overload
    def get_active_jobscripts(
        self, as_json: Literal[False] = False
    ) -> dict[int, dict[int, JobscriptElementState]]: ...

    @overload
    def get_active_jobscripts(
        self, as_json: Literal[True]
    ) -> dict[int, dict[int, str]]: ...

    @TimeIt.decorator
    def get_active_jobscripts(
        self, as_json: bool = False
    ) -> dict[int, dict[int, JobscriptElementState]] | dict[int, dict[int, str]]:
        """Get jobscripts that are active on this machine, and their active states."""
        # this returns: {JS_IDX: {JS_ELEMENT_IDX: STATE}}
        # TODO: query the scheduler once for all jobscripts?
        if as_json:
            details = ((js.index, js.get_active_states(as_json=True))
                       for js in self.jobscripts)
            return {
                idx: state for idx, state in details if state
            }
        else:
            dets2 = ((js.index, js.get_active_states(as_json=False))
                     for js in self.jobscripts)
            return {
                idx: state for idx, state in dets2 if state
            }

    def _write_abort_EARs_file(self) -> None:
        with self.abort_EARs_file_path.open(mode="wt", newline="\n") as fp:
            # write a single line for each EAR currently in the workflow:
            fp.write("\n".join("0" for _ in range(self.workflow.num_EARs)) + "\n")

    def _set_run_abort(self, run_ID: int) -> None:
        """Modify the abort runs file to indicate a specified run should be aborted."""
        with self.abort_EARs_file_path.open(mode="rt", newline="\n") as fp:
            lines = fp.read().splitlines()
        lines[run_ID] = "1"

        # write a new temporary run-abort file:
        tmp_suffix = self.abort_EARs_file_path.suffix + ".tmp"
        tmp = self.abort_EARs_file_path.with_suffix(tmp_suffix)
        self.app.submission_logger.debug(f"Creating temporary run abort file: {tmp!r}.")
        with tmp.open(mode="wt", newline="\n") as fp:
            fp.write("\n".join(i for i in lines) + "\n")

        # atomic rename, overwriting original:
        self.app.submission_logger.debug(
            "Replacing original run abort file with new temporary file."
        )
        os.replace(src=tmp, dst=self.abort_EARs_file_path)

    @staticmethod
    def get_unique_schedulers_of_jobscripts(
        jobscripts: list[Jobscript],
    ) -> dict[tuple[tuple[int, int], ...], Scheduler]:
        """Get unique schedulers and which of the passed jobscripts they correspond to.

        Uniqueness is determines only by the `QueuedScheduler.unique_properties` tuple.

        """
        js_idx: list[list[tuple[int, int]]] = []
        schedulers: list[Scheduler] = []

        # list of tuples of scheduler properties we consider to determine "uniqueness",
        # with the first string being the scheduler type (class name):
        seen_schedulers = []

        for js in jobscripts:
            if js.scheduler.unique_properties not in seen_schedulers:
                seen_schedulers.append(js.scheduler.unique_properties)
                schedulers.append(js.scheduler)
                js_idx.append([])
            sched_idx = seen_schedulers.index(js.scheduler.unique_properties)
            js_idx[sched_idx].append((js.submission.index, js.index))

        sched_js_idx = dict(zip((tuple(i) for i in js_idx), schedulers))

        return sched_js_idx

    @TimeIt.decorator
    def get_unique_schedulers(self) -> dict[tuple[tuple[int, int], ...], Scheduler]:
        """Get unique schedulers and which of this submission's jobscripts they
        correspond to."""
        return self.get_unique_schedulers_of_jobscripts(self.jobscripts)

    @TimeIt.decorator
    def get_unique_shells(self) -> dict[tuple[int, ...], Shell]:
        """Get unique shells and which jobscripts they correspond to."""
        js_idx: list[list[int]] = []
        shells: list[Shell] = []

        for js in self.jobscripts:
            if js.shell not in shells:
                shells.append(js.shell)
                js_idx.append([])
            shell_idx = shells.index(js.shell)
            js_idx[shell_idx].append(js.index)

        shell_js_idx = dict(zip((tuple(i) for i in js_idx), shells))

        return shell_js_idx

    def __raise_failure(self, submitted_js_idx, exceptions):
        msg = f"Some jobscripts in submission index {self.index} could not be submitted"
        if submitted_js_idx:
            msg += f" (but jobscripts {submitted_js_idx} were submitted successfully):"
        else:
            msg += ":"

        msg += "\n"
        for sub_err in exceptions:
            msg += (
                f"Jobscript {sub_err.js_idx} at path: {str(sub_err.js_path)!r}\n"
                f"Submit command: {sub_err.submit_cmd!r}.\n"
                f"Reason: {sub_err.message!r}\n"
            )
            if sub_err.subprocess_exc is not None:
                msg += f"Subprocess exception: {sub_err.subprocess_exc}\n"
            if sub_err.job_ID_parse_exc is not None:
                msg += f"Subprocess job ID parse exception: {sub_err.job_ID_parse_exc}\n"
            if sub_err.job_ID_parse_exc is not None:
                msg += f"Job ID parse exception: {sub_err.job_ID_parse_exc}\n"
            if sub_err.stdout:
                msg += f"Submission stdout:\n{indent(sub_err.stdout, '  ')}\n"
            if sub_err.stderr:
                msg += f"Submission stderr:\n{indent(sub_err.stderr, '  ')}\n"

        raise SubmissionFailure(message=msg)

    def _append_submission_part(self, submit_time: str, submitted_js_idx: list[int]):
        self._submission_parts[submit_time] = submitted_js_idx
        self.workflow._store.add_submission_part(
            sub_idx=self.index,
            dt_str=submit_time,
            submitted_js_idx=submitted_js_idx,
        )

    @TimeIt.decorator
    def submit(
        self,
        status,
        ignore_errors: bool = False,
        print_stdout: bool = False,
        add_to_known: bool = True,
    ) -> list[int]:
        """Generate and submit the jobscripts of this submission."""

        # if JS_parallelism explicitly requested but store doesn't support, raise:
        supports_JS_para = self.workflow._store._features.jobscript_parallelism
        if self.JS_parallelism:
            if not supports_JS_para:
                if status:
                    status.stop()
                raise ValueError(
                    f"Store type {self.workflow._store!r} does not support jobscript "
                    f"parallelism."
                )
        elif self.JS_parallelism is None:
            self._JS_parallelism = supports_JS_para

        # set os_name and shell_name for each jobscript:
        for js in self.jobscripts:
            js._set_os_name()
            js._set_shell_name()
            js._set_scheduler_name()

        outstanding = self.outstanding_jobscripts

        # get scheduler, shell and OS version information (also an opportunity to fail
        # before trying to submit jobscripts):
        js_vers_info: dict[int, dict[str, str | list[str]]] = {}
        for js_indices, sched in self.get_unique_schedulers().items():
            try:
                vers_info = sched.get_version_info()
            except Exception:
                if not ignore_errors:
                    raise
                vers_info = {}
            for _, js_idx in js_indices:
                if js_idx in outstanding:
                    js_vers_info.setdefault(js_idx, {}).update(vers_info)

        for js_indices_2, shell in self.get_unique_shells().items():
            try:
                vers_info = shell.get_version_info()
            except Exception:
                if not ignore_errors:
                    raise
                vers_info = {}
            for js_idx in js_indices_2:
                if js_idx in outstanding:
                    js_vers_info.setdefault(js_idx, {}).update(vers_info)

        for js_idx, vers_info_i in js_vers_info.items():
            self.jobscripts[js_idx]._set_version_info(vers_info_i)

        # for direct submission, it's important that os_name/shell_name/scheduler_name
        # are made persistent now, because `Workflow.write_commands`, which might be
        # invoked in a new process before submission has completed, needs to know these:
        self.workflow._store._pending.commit_all()

        # TODO: a submission should only be "submitted" once shouldn't it?
        # no; there could be an IO error (e.g. internet connectivity), so might
        # need to be able to reattempt submission of outstanding jobscripts.
        self.path.mkdir(exist_ok=True)
        if not self.abort_EARs_file_path.is_file():
            self._write_abort_EARs_file()

        # map jobscript `index` to (scheduler job ID or process ID, is_array):
        scheduler_refs: dict[int, tuple[str, bool]] = {}
        submitted_js_idx: list[int] = []
        errs: list[Exception] = []
        for js in self.jobscripts:
            # check not previously submitted:
            if js.index not in outstanding:
                continue

            # check all dependencies were submitted now or previously:
            if not all(
                i in submitted_js_idx or i in self.submitted_jobscripts
                for i in js.dependencies
            ):
                continue

            try:
                if status:
                    status.update(f"Submitting jobscript {js.index}...")
                js_ref_i = js.submit(scheduler_refs, print_stdout=print_stdout)
                scheduler_refs[js.index] = (js_ref_i, js.is_array)
                submitted_js_idx.append(js.index)

            except JobscriptSubmissionFailure as err:
                errs.append(err)
                continue

        if submitted_js_idx:
            dt_str = datetime.utcnow().strftime(self.app._submission_ts_fmt)
            self._append_submission_part(
                submit_time=dt_str,
                submitted_js_idx=submitted_js_idx,
            )
            # add a record of the submission part to the known-submissions file
            if add_to_known:
                self.app._add_to_known_submissions(
                    wk_path=self.workflow.path,
                    wk_id=self.workflow.id_,
                    sub_idx=self.index,
                    sub_time=dt_str,
                )

        if errs and not ignore_errors:
            if status:
                status.stop()
            self.__raise_failure(submitted_js_idx, errs)

        len_js = len(submitted_js_idx)
        print(f"Submitted {len_js} jobscript{'s' if len_js > 1 else ''}.")

        return submitted_js_idx

    @TimeIt.decorator
    def cancel(self):
        act_js = list(self.get_active_jobscripts())
        if not act_js:
            print("No active jobscripts to cancel.")
            return
        for js_indices, sched in self.get_unique_schedulers().items():
            # filter by active jobscripts:
            js_idx = [i[1] for i in js_indices if i[1] in act_js]
            if js_idx:
                print(
                    f"Cancelling jobscripts {js_idx!r} of submission {self.index} of "
                    f"workflow {self.workflow.name!r}."
                )
                jobscripts = [self.jobscripts[i] for i in js_idx]
                sched_refs = [i.scheduler_js_ref for i in jobscripts]
                sched.cancel_jobs(js_refs=sched_refs, jobscripts=jobscripts)
            else:
                print("No active jobscripts to cancel.")
