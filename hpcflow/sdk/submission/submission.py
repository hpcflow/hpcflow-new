from __future__ import annotations
from collections import defaultdict

from datetime import datetime, timedelta, timezone
import enum
import os
import shutil
from pathlib import Path
from textwrap import indent
from typing import Dict, List, Literal, Optional, Tuple, Union
import warnings

from hpcflow.sdk import app
from hpcflow.sdk.core.element import ElementResources
from hpcflow.sdk.core.errors import (
    JobscriptSubmissionFailure,
    MissingEnvironmentError,
    MissingEnvironmentExecutableError,
    MissingEnvironmentExecutableInstanceError,
    MultipleEnvironmentsError,
    SubmissionFailure,
    OutputFileParserNoOutputError,
)
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.object_list import ObjectListMultipleMatchError
from hpcflow.sdk.log import TimeIt


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

    TMP_DIR_NAME = "tmp"
    LOG_DIR_NAME = "app_logs"
    APP_STD_DIR_NAME = "app_std"
    JS_STD_DIR_NAME = "js_std"
    SCRIPTS_DIR_NAME = "scripts"
    COMMANDS_DIR_NAME = "commands"

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
        jobscripts: List[app.Jobscript],
        workflow: Optional[app.Workflow] = None,
        submission_parts: Optional[Dict] = None,
        JS_parallelism: Optional[Union[bool, Literal["direct", "scheduled"]]] = None,
        environments: Optional[app.EnvironmentsList] = None,
    ):
        self._index = index
        self._jobscripts = jobscripts
        self._submission_parts = submission_parts or {}
        self._JS_parallelism = JS_parallelism
        self._environments = environments  # assigned by _set_environments

        self._submission_parts_lst = None  # assigned on first access; datetime objects

        if workflow:
            self.workflow = workflow

        self._set_parent_refs()

    @TimeIt.decorator
    def _set_environments(self):
        filterable = ElementResources.get_env_instance_filterable_attributes()

        # map required environments and executable labels to job script indices:
        req_envs = defaultdict(lambda: defaultdict(set))
        for js_idx, js_i in enumerate(self.jobscripts):
            for run in js_i.all_EARs:
                env_spec_h = run.env_spec_hashable
                for exec_label_j in run.action.get_required_executables():
                    req_envs[env_spec_h][exec_label_j].add(js_idx)
                # add any environment for which an executable was not required:
                if env_spec_h not in req_envs:
                    req_envs[env_spec_h] = defaultdict(set)

        # check these envs/execs exist in app data:
        envs = []
        for env_spec_h, exec_js in req_envs.items():
            env_spec = self.app.Action.env_spec_from_hashable(env_spec_h)
            non_name_spec = {k: v for k, v in env_spec.items() if k != "name"}
            spec_str = f" with specifiers {non_name_spec!r}" if non_name_spec else ""
            env_ref = f"{env_spec['name']!r}{spec_str}"
            try:
                env_i = self.app.envs.get(**env_spec)
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
    def environments(self) -> app.EnvironmentsList:
        return self._environments

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

    @TimeIt.decorator
    def get_start_time(self, submit_time: str) -> Union[datetime, None]:
        """Get the start time of a given submission part."""
        js_idx = self._submission_parts[submit_time]
        all_part_starts = []
        for i in js_idx:
            start_time = self.jobscripts[i].start_time
            if start_time:
                all_part_starts.append(start_time)
        if all_part_starts:
            return min(all_part_starts)
        else:
            return None

    @TimeIt.decorator
    def get_end_time(self, submit_time: str) -> Union[datetime, None]:
        """Get the end time of a given submission part."""
        js_idx = self._submission_parts[submit_time]
        all_part_ends = []
        for i in js_idx:
            end_time = self.jobscripts[i].end_time
            if end_time:
                all_part_ends.append(end_time)
        if all_part_ends:
            return max(all_part_ends)
        else:
            return None

    @property
    @TimeIt.decorator
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
    @TimeIt.decorator
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

    @classmethod
    def get_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return submissions_path / str(sub_idx)

    @classmethod
    def get_tmp_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.TMP_DIR_NAME

    @classmethod
    def get_log_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.LOG_DIR_NAME

    @classmethod
    def get_app_std_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.APP_STD_DIR_NAME

    @classmethod
    def get_js_std_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.JS_STD_DIR_NAME

    @classmethod
    def get_scripts_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.SCRIPTS_DIR_NAME

    @classmethod
    def get_commands_path(cls, submissions_path: Path, sub_idx: int) -> Path:
        return cls.get_path(submissions_path, sub_idx) / cls.COMMANDS_DIR_NAME

    @property
    def path(self) -> Path:
        return self.get_path(self.workflow.submissions_path, self.index)

    @property
    def tmp_path(self) -> Path:
        return self.get_tmp_path(self.workflow.submissions_path, self.index)

    @property
    def log_path(self) -> Path:
        return self.get_log_path(self.workflow.submissions_path, self.index)

    @property
    def app_std_path(self):
        return self.get_app_std_path(self.workflow.submissions_path, self.index)

    @property
    def js_std_path(self):
        return self.get_js_std_path(self.workflow.submissions_path, self.index)

    @property
    def scripts_path(self):
        return self.get_scripts_path(self.workflow.submissions_path, self.index)

    @property
    def commands_path(self):
        return self.get_commands_path(self.workflow.submissions_path, self.index)

    @property
    def all_EAR_IDs(self):
        return [i for js in self.jobscripts for i in js.all_EAR_IDs]

    @property
    def all_EARs(self):
        return [i for js in self.jobscripts for i in js.all_EARs]

    @property
    def all_EARs_by_jobscript(self) -> List:
        ids = [i.all_EAR_IDs for i in self.jobscripts]
        all_EARs = {i.id_: i for i in self.workflow.get_EARs_from_IDs(self.all_EAR_IDs)}
        return [[all_EARs[i] for i in js_ids] for js_ids in ids]

    @property
    @TimeIt.decorator
    def EARs_by_elements(self):
        task_elem_EARs = defaultdict(lambda: defaultdict(list))
        for i in self.all_EARs:
            task_elem_EARs[i.task.index][i.element.index].append(i)
        return task_elem_EARs

    @property
    def is_scheduled(self) -> Tuple[bool]:
        """Return whether each jobscript of this submission uses a scheduler or not."""
        return tuple(i.is_scheduled for i in self.jobscripts)

    @TimeIt.decorator
    def get_active_jobscripts(
        self, as_json: bool = False
    ) -> Dict[int, Dict[int, Dict[int, JobscriptElementState]]]:
        """Get jobscripts that are active on this machine, and their active states."""
        # this returns: {JS_IDX: {BLOCK_IDX: {JS_ELEMENT_IDX: STATE}}}
        # TODO: query the scheduler once for all jobscripts?
        out = {}
        for js in self.jobscripts:
            active_states = js.get_active_states(as_json=as_json)
            if active_states:
                out[js.index] = active_states
        return out

    @TimeIt.decorator
    def _write_scripts(self) -> Dict[int, int]:
        """Write to disk all action scripts associated with this submission."""
        actions_by_schema = defaultdict(lambda: defaultdict(set))
        cmd_hashes = defaultdict(set)

        run_cmd_file_names = {}
        for js_idx, js_runs in enumerate(self.all_EARs_by_jobscript):
            js = self.jobscripts[js_idx]
            for run in js_runs:
                if run.is_snippet_script:
                    actions_by_schema[run.action.task_schema.name][
                        run.element_action.action_idx
                    ].add(run.env_spec_hashable)

                if run.action.commands:
                    hash_i = run.get_commands_file_hash()
                    if hash_i not in cmd_hashes:
                        try:
                            run.try_write_commands(
                                environments=self.environments,
                                jobscript=js,
                            )
                        except OutputFileParserNoOutputError:
                            # no commands to write, might be used just for saving files
                            run_cmd_file_names[run.id_] = None
                    cmd_hashes[hash_i].add(run.id_)
                else:
                    run_cmd_file_names[run.id_] = None

        for run_ids in cmd_hashes.values():
            run_ids_srt = sorted(run_ids)
            root_id = run_ids_srt[0]  # used for command file name for this group
            # TODO: could store multiple IDs to reduce number of files created
            for run_id_i in run_ids_srt:
                if run_id_i not in run_cmd_file_names:
                    run_cmd_file_names[run_id_i] = root_id

        seen = {}
        for task in self.workflow.tasks:
            for schema in task.template.schemas:
                if schema.name in actions_by_schema:
                    for idx, action in enumerate(schema.actions):

                        if not action.script:
                            continue

                        for env_spec_h in actions_by_schema[schema.name][idx]:

                            env_spec = action.env_spec_from_hashable(env_spec_h)
                            name, snip_path, specs = action.get_script_artifact_name(
                                env_spec=env_spec,
                                act_idx=idx,
                                ret_specifiers=True,
                            )
                            script_hash = action.get_script_determinant_hash(specs)
                            script_path = self.scripts_path / name
                            prev_path = seen.get(script_hash)
                            if script_path == prev_path:
                                continue

                            elif prev_path:
                                # try to make a symbolic link to the file previously
                                # created:
                                try:
                                    script_path.symlink_to(prev_path.name)
                                except OSError:
                                    # windows requires admin permission, copy instead:
                                    shutil.copy(prev_path, script_path)
                            else:
                                # write script to disk:
                                source_str = action.compose_source(snip_path)
                                if source_str:
                                    with script_path.open("wt", newline="\n") as fp:
                                        fp.write(source_str)
                                    seen[script_hash] = script_path

        return run_cmd_file_names

    @staticmethod
    def get_unique_schedulers_of_jobscripts(
        jobscripts: List[Jobscript],
    ) -> Dict[Tuple[Tuple[int, int]], Scheduler]:
        """Get unique schedulers and which of the passed jobscripts they correspond to.

        Uniqueness is determined only by the `Scheduler.unique_properties` tuple.

        """
        js_idx = []
        schedulers = []

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
    def get_unique_schedulers(self) -> Dict[Tuple[int], Scheduler]:
        """Get unique schedulers and which of this submission's jobscripts they
        correspond to."""
        return self.get_unique_schedulers_of_jobscripts(self.jobscripts)

    @TimeIt.decorator
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

    @TimeIt.decorator
    def submit(
        self,
        status,
        ignore_errors: Optional[bool] = False,
        print_stdout: Optional[bool] = False,
        add_to_known: Optional[bool] = True,
    ) -> List[int]:
        """Generate and submit the jobscripts of this submission."""

        # if JS_parallelism explicitly requested but store doesn't support, raise:
        supports_JS_para = self.workflow._store._features.jobscript_parallelism
        if self.JS_parallelism:
            # could be: True | "direct" | "scheduled"
            if not supports_JS_para:
                if status:
                    status.stop()
                raise ValueError(
                    f"Store type {self.workflow._store!r} does not support jobscript "
                    f"parallelism."
                )
        elif self.JS_parallelism is None:
            # by default only use JS parallelism for scheduled jobscripts:
            self._JS_parallelism = "scheduled" if supports_JS_para else False
            # TODO: is this value saved?

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
                js_idx in submitted_js_idx or js_idx in self.submitted_jobscripts
                for js_idx, _ in js.dependencies
            ):
                warnings.warn(
                    f"Cannot submit jobscript index {js.index} since not all of its "
                    f"dependencies have been submitted: {js.dependencies!r}"
                )
                continue

            try:
                if status:
                    status.update(
                        f"Submitting jobscript {js.index + 1}/{len(self.jobscripts)}..."
                    )
                js_ref_i = js.submit(scheduler_refs, print_stdout=print_stdout)
                scheduler_refs[js.index] = (js_ref_i, js.is_array)
                submitted_js_idx.append(js.index)

            except JobscriptSubmissionFailure as err:
                errs.append(err)
                continue

            # TODO: some way to handle KeyboardInterrupt during submission?
            #   - stop, and cancel already submitted?

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
            self._raise_failure(submitted_js_idx, errs)

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
