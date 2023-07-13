from __future__ import annotations
from collections import defaultdict

from datetime import datetime, timedelta, timezone
import enum
from pathlib import Path
from textwrap import indent
from typing import Dict, List, Optional, Tuple

from hpcflow.sdk import app
from hpcflow.sdk.core.errors import JobscriptSubmissionFailure, SubmissionFailure
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike


def timedelta_format(td: timedelta) -> str:
    days, seconds = td.days, td.seconds
    hours = seconds // (60 * 60)
    seconds -= hours * (60 * 60)
    minutes = seconds // 60
    seconds -= minutes * 60
    return f"{days}-{hours:02}:{minutes:02}:{seconds:02}"


def timedelta_parse(td_str: str) -> timedelta:
    days, other = td_str.split("-")
    days = int(days)
    hours, mins, secs = [int(i) for i in other.split(":")]
    return timedelta(days=days, hours=hours, minutes=mins, seconds=secs)


class SubmissionStatus(enum.Enum):
    PENDING = 0  # not yet submitted
    SUBMITTED = 1  # all jobscripts submitted successfully
    PARTIALLY_SUBMITTED = 2  # some jobscripts submitted successfully


class Submission(JSONLike):
    _child_objects = (
        ChildObjectSpec(
            name="jobscripts",
            class_name="Jobscript",
            is_multiple=True,
            parent_ref="_submission",
        ),
    )

    def __init__(
        self,
        index: int,
        jobscripts: List[app.Jobscript],
        workflow: Optional[app.Workflow] = None,
        submission_parts: Optional[Dict] = None,
        JS_parallelism: Optional[bool] = None,
    ):
        self._index = index
        self._jobscripts = jobscripts
        self._submission_parts = submission_parts or {}
        self._JS_parallelism = JS_parallelism

        self._submission_parts_lst = None  # assigned on first access; datetime objects

        if workflow:
            self.workflow = workflow

        self._set_parent_refs()

        for js_idx, js in enumerate(self.jobscripts):
            js._index = js_idx

    def to_dict(self):
        dct = super().to_dict()
        del dct["_workflow"]
        del dct["_index"]
        del dct["_submission_parts_lst"]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        return dct

    @property
    def index(self) -> int:
        return self._index

    @property
    def submission_parts(self) -> List[Dict]:
        if not self._submission_parts:
            return []

        if self._submission_parts_lst is None:
            self._submission_parts_lst = [
                {
                    "submit_time": datetime.strptime(dt, self.workflow.ts_fmt)
                    .replace(tzinfo=timezone.utc)
                    .astimezone(),
                    "jobscripts": js_idx,
                }
                for dt, js_idx in self._submission_parts.items()
            ]
        return self._submission_parts_lst

    def get_start_time(self, submit_time: str) -> Union[datetime, None]:
        """Get the start time of a given submission part."""
        js_idx = self._submission_parts[submit_time]
        all_part_starts = []
        for i in js_idx:
            if self.jobscripts[i].start_time:
                all_part_starts.append(self.jobscripts[i].start_time)
        if all_part_starts:
            return min(all_part_starts)
        else:
            return None

    def get_end_time(self, submit_time: str) -> Union[datetime, None]:
        """Get the end time of a given submission part."""
        js_idx = self._submission_parts[submit_time]
        all_part_ends = []
        for i in js_idx:
            if self.jobscripts[i].end_time:
                all_part_ends.append(self.jobscripts[i].end_time)
        if all_part_ends:
            return max(all_part_ends)
        else:
            return None

    @property
    def start_time(self):
        """Get the first non-None start time over all submission parts."""
        all_start_times = []
        for submit_time in self._submission_parts:
            start_i = self.get_start_time(submit_time)
            if start_i:
                all_start_times.append(start_i)
        if all_start_times:
            return max(all_start_times)
        else:
            return None

    @property
    def end_time(self):
        """Get the final non-None end time over all submission parts."""
        all_end_times = []
        for submit_time in self._submission_parts:
            end_i = self.get_end_time(submit_time)
            if end_i:
                all_end_times.append(end_i)
        if all_end_times:
            return max(all_end_times)
        else:
            return None

    @property
    def jobscripts(self) -> List:
        return self._jobscripts

    @property
    def JS_parallelism(self):
        return self._JS_parallelism

    @property
    def workflow(self) -> List:
        return self._workflow

    @workflow.setter
    def workflow(self, wk):
        self._workflow = wk
        # if JS_parallelism explicitly requested but store doesn't support, raise:
        supports_JS_para = self.workflow._store._features.jobscript_parallelism
        if self.JS_parallelism:
            if not supports_JS_para:
                raise ValueError(
                    f"Store type {self.workflow._store!r} does not support jobscript "
                    f"parallelism."
                )
        elif self.JS_parallelism is None:
            self._JS_parallelism = supports_JS_para

    @property
    def jobscript_indices(self) -> Tuple[int]:
        """All associated jobscript indices."""
        return tuple(i.index for i in self.jobscripts)

    @property
    def submitted_jobscripts(self) -> Tuple[int]:
        """Jobscript indices that have been successfully submitted."""
        return tuple(j for i in self.submission_parts for j in i["jobscripts"])

    @property
    def outstanding_jobscripts(self) -> Tuple[int]:
        """Jobscript indices that have not yet been successfully submitted."""
        return tuple(set(self.jobscript_indices) - set(self.submitted_jobscripts))

    @property
    def status(self):
        if not self.submission_parts:
            return SubmissionStatus.PENDING
        else:
            if set(self.submitted_jobscripts) == set(self.jobscript_indices):
                return SubmissionStatus.SUBMITTED
            else:
                return SubmissionStatus.PARTIALLY_SUBMITTED

    @property
    def needs_submit(self):
        return self.status in (
            SubmissionStatus.PENDING,
            SubmissionStatus.PARTIALLY_SUBMITTED,
        )

    @property
    def path(self):
        return self.workflow.submissions_path / str(self.index)

    @property
    def all_EAR_IDs(self):
        return [i for js in self.jobscripts for i in js.EAR_ID.flatten()]

    @property
    def all_EARs(self):
        return [i for js in self.jobscripts for i in js.all_EARs]

    @property
    def EARs_by_elements(self):
        task_elem_EARs = defaultdict(lambda: defaultdict(list))
        for i in self.all_EARs:
            task_elem_EARs[i.task.index][i.element.index].append(i)
        return task_elem_EARs

    @property
    def abort_EARs_file_name(self):
        return f"abort_EARs.txt"

    @property
    def abort_EARs_file_path(self):
        return self.path / self.abort_EARs_file_name

    def get_active_jobscripts(
        self,
    ) -> List[Tuple[int, Dict[int, JobscriptElementState]]]:
        """Get jobscripts that are active on this machine, and their active states."""
        out = {}
        for js in self.jobscripts:
            active_states = js.get_active_states()
            if active_states:
                out[js.index] = active_states
        return out

    def _write_abort_EARs_file(self):
        with self.abort_EARs_file_path.open(mode="wt", newline="\n") as fp:
            # write a single line for each EAR currently in the workflow:
            fp.write("\n".join("0" for _ in range(self.workflow.num_EARs)) + "\n")

    @staticmethod
    def get_unique_schedulers_of_jobscripts(
        jobscripts: List[Jobscript],
    ) -> Dict[Tuple[Tuple[int, int]], Scheduler]:
        """Get unique schedulers and which of the passed jobscripts they correspond to."""
        js_idx = []
        schedulers = []
        for js in jobscripts:
            if js.scheduler not in schedulers:
                schedulers.append(js.scheduler)
                js_idx.append([])
            sched_idx = schedulers.index(js.scheduler)
            js_idx[sched_idx].append((js.submission.index, js.index))

        sched_js_idx = dict(zip((tuple(i) for i in js_idx), schedulers))

        return sched_js_idx

    def get_unique_schedulers(self) -> Dict[Tuple[int], Scheduler]:
        """Get unique schedulers and which of this submission's jobscripts they
        correspond to."""
        return self.get_unique_schedulers_of_jobscripts(self.jobscripts)

    def get_unique_shells(self) -> Dict[Tuple[int], Shell]:
        """Get unique shells and which jobscripts they correspond to."""
        js_idx = []
        shells = []

        for js in self.jobscripts:
            if js.shell not in shells:
                shells.append(js.shell)
                js_idx.append([])
            shell_idx = shells.index(js.shell)
            js_idx[shell_idx].append(js.index)

        shell_js_idx = dict(zip((tuple(i) for i in js_idx), shells))

        return shell_js_idx

    def _raise_failure(self, submitted_js_idx, exceptions):
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

    def _append_submission_part(self, submit_time: str, submitted_js_idx: List[int]):
        self._submission_parts[submit_time] = submitted_js_idx
        self.workflow._store.add_submission_part(
            sub_idx=self.index,
            dt_str=submit_time,
            submitted_js_idx=submitted_js_idx,
        )

    def submit(
        self,
        status,
        ignore_errors=False,
        print_stdout=False,
    ) -> List[int]:
        """Generate and submit the jobscripts of this submission."""

        # set os_name and shell_name for each jobscript:
        for js in self.jobscripts:
            js._set_os_name()
            js._set_shell_name()
            js._set_scheduler_name()

        outstanding = self.outstanding_jobscripts

        # get scheduler, shell and OS version information (also an opportunity to fail
        # before trying to submit jobscripts):
        js_vers_info = {}
        for js_indices, sched in self.get_unique_schedulers().items():
            try:
                vers_info = sched.get_version_info()
            except Exception as err:
                if ignore_errors:
                    vers_info = {}
                else:
                    raise err
            for _, js_idx in js_indices:
                if js_idx in outstanding:
                    if js_idx not in js_vers_info:
                        js_vers_info[js_idx] = {}
                    js_vers_info[js_idx].update(vers_info)

        for js_indices, shell in self.get_unique_shells().items():
            try:
                vers_info = shell.get_version_info()
            except Exception as err:
                if ignore_errors:
                    vers_info = {}
                else:
                    raise err
            for js_idx in js_indices:
                if js_idx in outstanding:
                    if js_idx not in js_vers_info:
                        js_vers_info[js_idx] = {}
                    js_vers_info[js_idx].update(vers_info)

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
        scheduler_refs = {}
        submitted_js_idx = []
        errs = []
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
            self.app._add_to_known_submissions(
                wk_path=self.workflow.path,
                wk_id=self.workflow.id_,
                sub_idx=self.index,
                sub_time=dt_str,
            )

        if errs and not ignore_errors:
            status.stop()
            self._raise_failure(submitted_js_idx, errs)

        len_js = len(submitted_js_idx)
        print(f"Submitted {len_js} jobscript{'s' if len_js > 1 else ''}.")

        return submitted_js_idx

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
