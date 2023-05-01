from __future__ import annotations

from datetime import timedelta
import enum
from pathlib import Path
from textwrap import indent
from typing import Dict, List, Optional, Tuple

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
        jobscripts: List[Jobscript],
        workflow: Workflow,
        submission_attempts: Optional[List] = None,
        JS_parallelism: Optional[bool] = None,
    ):
        self._index = index
        self._jobscripts = jobscripts
        self._workflow = workflow
        self._submission_attempts = submission_attempts or []
        self._JS_parallelism = JS_parallelism

        self._set_parent_refs()

        for js_idx, js in enumerate(self.jobscripts):
            js._index = js_idx

        # if JS_parallelism explicitly requested but store doesn't support, raise:
        supports_JS_para = self.workflow._store.features.jobscript_parallelism
        if self.JS_parallelism:
            if not supports_JS_para:
                raise ValueError(
                    f"Store type {self.workflow._store!r} does not support jobscript "
                    f"parallelism."
                )
        elif self.JS_parallelism is None:
            self._JS_parallelism = supports_JS_para

    def to_dict(self):
        dct = super().to_dict()
        del dct["_workflow"]
        del dct["_index"]
        dct = {k.lstrip("_"): v for k, v in dct.items()}
        return dct

    @property
    def index(self) -> int:
        return self._index

    @property
    def submission_attempts(self) -> List:
        return self._submission_attempts

    @property
    def jobscripts(self) -> List:
        return self._jobscripts

    @property
    def JS_parallelism(self):
        return self._JS_parallelism

    @property
    def workflow(self) -> List:
        return self._workflow

    @property
    def jobscript_indices(self) -> Tuple[int]:
        """All associated jobscript indices."""
        return tuple(i.index for i in self.jobscripts)

    @property
    def submitted_jobscripts(self) -> Tuple[int]:
        """Jobscript indices that have been successfully submitted."""
        return tuple(j for i in self.submission_attempts for j in i)

    @property
    def outstanding_jobscripts(self) -> Tuple[int]:
        """Jobscript indices that have not yet been successfully submitted."""
        return tuple(set(self.jobscript_indices) - set(self.submitted_jobscripts))

    @property
    def status(self):
        if not self.submission_attempts:
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

    def get_unique_schedulers(self) -> Dict[Tuple[int], Scheduler]:
        """Get a unique schedulers and which jobscripts they correspond to."""
        js_idx = []
        schedulers = []

        for js in self.jobscripts:
            if js.scheduler not in schedulers:
                schedulers.append(js.scheduler)
                js_idx.append([])
            sched_idx = schedulers.index(js.scheduler)
            js_idx[sched_idx].append(js.index)

        sched_js_idx = dict(zip((tuple(i) for i in js_idx), schedulers))

        return sched_js_idx

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

    def prepare_EAR_submission_idx_update(self) -> List[Tuple[int, int, int, int]]:
        """For all EARs in this submission (across all jobscripts), return a tuple of indices
        that can be passed to `Workflow.set_EAR_submission_index`."""
        indices = []
        for js in self.jobscripts:
            for ear_idx_i, ear_idx_j in js.EARs.items():
                # task insert ID, iteration idx, action idx, run idx:
                indices.append((ear_idx_i[0], ear_idx_j[0], ear_idx_j[1], ear_idx_j[2]))
        return indices

    def get_EAR_run_dirs(self) -> Dict[Tuple(int, int, int), Path]:
        indices = []
        for js in self.jobscripts:
            for ear_idx_i, ear_idx_j in js.EARs.items():
                # task insert ID, iteration idx, action idx, run idx:
                indices.append((ear_idx_i[0], ear_idx_j[0], ear_idx_j[1], ear_idx_j[2]))
        return indices

    def _raise_failure(self, submitted_js_idx, exceptions):
        msg = f"Some jobscripts in submission index {self.index} could not be submitted"
        if submitted_js_idx:
            msg += (
                f" (but jobscripts {submitted_js_idx} were submitted " f"successfully):"
            )
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

    def _append_submission_attempt(self, submitted_js_idx: List[int]):
        self._submission_attempts.append(submitted_js_idx)
        self.workflow._store.append_submission_attempt(
            sub_idx=self.index, submitted_js_idx=submitted_js_idx
        )

    def submit(self, task_artifacts_path, ignore_errors=False) -> List[int]:
        """Generate and submit the jobscripts of this submission."""

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
            for js_idx in js_indices:
                if js_idx in outstanding:
                    if js_idx not in js_vers_info:
                        js_vers_info[js_idx] = {}
                    js_vers_info[js_idx].update(vers_info)

                    # self.jobscripts[js_idx]._set_version_info(vers_info)

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

        self.path.mkdir(exist_ok=True)
        scheduler_refs = {}  # map jobscript `index` to scheduler job IDs
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
                scheduler_refs[js.index] = js.submit(task_artifacts_path, scheduler_refs)
                submitted_js_idx.append(js.index)

            except JobscriptSubmissionFailure as err:
                errs.append(err)
                continue

        if submitted_js_idx:
            self._append_submission_attempt(submitted_js_idx)

        if errs and not ignore_errors:
            self._raise_failure(submitted_js_idx, errs)

        len_js = len(submitted_js_idx)
        print(f"Submitted {len_js} jobscript{'s' if len_js > 1 else ''}.")

        return submitted_js_idx
