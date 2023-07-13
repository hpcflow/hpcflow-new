from pathlib import Path
import re
import subprocess
from typing import Dict, List, Tuple
from hpcflow.sdk.submission.jobscript_info import JobscriptElementState
from hpcflow.sdk.submission.schedulers import Scheduler
from hpcflow.sdk.submission.schedulers.utils import run_cmd
from hpcflow.sdk.submission.shells.base import Shell


class SGEPosix(Scheduler):
    """

    Notes
    -----
    - runs in serial by default

    References
    ----------
    [1] https://gridscheduler.sourceforge.net/htmlman/htmlman1/qsub.html
    [2] https://softpanorama.org/HPC/Grid_engine/Queues/queue_states.shtml

    """

    _app_attr = "app"

    DEFAULT_SHEBANG_ARGS = ""
    DEFAULT_SUBMIT_CMD = "qsub"
    DEFAULT_SHOW_CMD = ["qstat"]
    DEFAULT_DEL_CMD = "qdel"
    DEFAULT_JS_CMD = "#$"
    DEFAULT_ARRAY_SWITCH = "-t"
    DEFAULT_ARRAY_ITEM_VAR = "SGE_TASK_ID"
    DEFAULT_CWD_SWITCH = "-cwd"

    # maps scheduler states:
    state_lookup = {
        "qw": JobscriptElementState.pending,
        "hq": JobscriptElementState.waiting,
        "hR": JobscriptElementState.waiting,
        "r": JobscriptElementState.running,
        "t": JobscriptElementState.running,
        "Rr": JobscriptElementState.running,
        "Rt": JobscriptElementState.running,
        "s": JobscriptElementState.errored,
        "ts": JobscriptElementState.errored,
        "S": JobscriptElementState.errored,
        "tS": JobscriptElementState.errored,
        "T": JobscriptElementState.errored,
        "tT": JobscriptElementState.errored,
        "Rs": JobscriptElementState.errored,
        "Rt": JobscriptElementState.errored,
        "RS": JobscriptElementState.errored,
        "RT": JobscriptElementState.errored,
        "Eq": JobscriptElementState.errored,
        "Eh": JobscriptElementState.errored,
        "dr": JobscriptElementState.cancelled,
        "dt": JobscriptElementState.cancelled,
        "dR": JobscriptElementState.cancelled,
        "ds": JobscriptElementState.cancelled,
        "dS": JobscriptElementState.cancelled,
        "dT": JobscriptElementState.cancelled,
    }

    def __init__(self, cwd_switch=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cwd_switch = cwd_switch or self.DEFAULT_CWD_SWITCH

    def format_core_request_lines(self, num_cores, parallel_env):
        lns = []
        if num_cores > 1:
            lns.append(f"{self.js_cmd} -pe {parallel_env} {num_cores}")
        return lns

    def format_array_request(self, num_elements):
        return f"{self.js_cmd} {self.array_switch} 1-{num_elements}"

    def format_std_stream_file_option_lines(self, is_array, sub_idx):
        # note: we can't modify the file names
        base = f"./artifacts/submissions/{sub_idx}"
        return [
            f"{self.js_cmd} -o {base}",
            f"{self.js_cmd} -e {base}",
        ]

    def format_options(self, resources, num_elements, is_array, sub_idx):
        # TODO: I think the PEs are set by the sysadmins so they should be set in the
        # config file as a mapping between num_cores/nodes and PE names?
        # `qconf -spl` shows a list of PEs

        opts = []
        opts.append(self.format_switch(self.cwd_switch))
        opts.extend(self.format_core_request_lines(resources.num_cores, "smp.pe"))
        if is_array:
            opts.append(self.format_array_request(num_elements))

        opts.extend(self.format_std_stream_file_option_lines(is_array, sub_idx))
        opts.extend([f"{self.js_cmd} {opt}" for opt in self.options])
        return "\n".join(opts) + "\n"

    def get_version_info(self):
        vers_cmd = self.show_cmd + ["-help"]
        proc = subprocess.run(
            args=vers_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout = proc.stdout.decode().strip()
        version_str = stdout.split("\n")[0].strip()
        name, version = version_str.split()
        out = {
            "scheduler_name": name,
            "scheduler_version": version,
        }
        return out

    def get_submit_command(
        self,
        shell: Shell,
        js_path: str,
        deps: List[Tuple],
    ) -> List[str]:
        cmd = [self.submit_cmd, "-terse"]

        dep_job_IDs = []
        dep_job_IDs_arr = []
        for job_ID, is_array_dep in deps.values():
            if is_array_dep:  # array dependency
                dep_job_IDs_arr.append(str(job_ID))
            else:
                dep_job_IDs.append(str(job_ID))

        if dep_job_IDs:
            cmd.append("-hold_jid")
            cmd.append(",".join(dep_job_IDs))

        if dep_job_IDs_arr:
            cmd.append("-hold_jid_ad")
            cmd.append(",".join(dep_job_IDs_arr))

        cmd.append(f'"{js_path}"')
        return cmd

    def parse_submission_output(self, stdout: str) -> str:
        """Extract scheduler reference for a newly submitted jobscript"""
        match = re.search("^\d+", stdout)
        if match:
            job_ID = match.group()
        else:
            raise RuntimeError(f"Could not parse Job ID from scheduler output {stdout!r}")
        return job_ID

    def get_job_statuses(self):
        """Get information about all of this user's jobscripts that currently listed by
        the scheduler."""

        # TODO: test this works with pending jobs:
        cmd = self.show_cmd + ["-U", "$USER", "-g", "d"]  # "-g d": separate arrays items
        stdout, stderr = run_cmd(cmd)
        if stderr:
            raise ValueError(
                f"Could not get query SGE jobs. Command was: {cmd!r}; stderr was: "
                f"{stderr}"
            )
        else:
            info = {}
            lines = stdout.split("\n")
            # assuming a job name with spaces means we can't split on spaces to get
            # anywhere beyond the job name, so get the column index of the state heading
            # and assume the state is always left-aligned with the heading:
            state_idx = lines[0].index("state")
            task_id_idx = lines[0].index("ja-task-ID")
            for ln in lines[2:]:
                if not ln:
                    continue
                ln_s = ln.split()
                base_job_ID = ln_s[0]

                # states can be one or two chars (for our limited purposes):
                state_str = ln[state_idx : state_idx + 2].strip()
                state = self.state_lookup[state_str]

                arr_idx = ln[task_id_idx:].strip()
                if arr_idx:
                    arr_idx = int(arr_idx) - 1  # zero-index
                else:
                    arr_idx = None

                if base_job_ID not in info:
                    info[base_job_ID] = {}

                info[base_job_ID][arr_idx] = state
        return info

    def get_job_state_info(
        self, js_refs: List[str] = None
    ) -> Dict[str, Dict[int, JobscriptElementState]]:
        """Query the scheduler to get the states of all of this user's jobs, optionally
        filtering by specified job IDs.

        Jobs that are not in the scheduler's status output will not appear in the output
        of this method.

        """

        info = self.get_job_statuses()
        if js_refs:
            info = {k: v for k, v in info.items() if k in js_refs}
        return info

    def cancel_jobs(self, js_ref: List[str], jobscripts: List = None):
        cmd = [self.del_cmd] + js_ref
        self.app.submission_logger.info(
            f"cancelling {self.__class__.__name__} jobscripts with command: {cmd}."
        )
        stdout, stderr = run_cmd(cmd)
        if stderr:
            raise ValueError(
                f"Could not get query SGE {self.__class__.__name__}. Command was: "
                f"{cmd!r}; stderr was: {stderr}"
            )
        self.app.submission_logger.info(
            f"jobscripts cancel command executed; stdout was: {stdout}."
        )
