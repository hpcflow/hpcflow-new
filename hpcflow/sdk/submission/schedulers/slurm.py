import enum
from pathlib import Path
import subprocess
import time
from typing import Dict, List, Tuple
from hpcflow.sdk.submission.jobscript_info import JobscriptElementState
from hpcflow.sdk.submission.schedulers import Scheduler
from hpcflow.sdk.submission.schedulers.utils import run_cmd
from hpcflow.sdk.submission.shells.base import Shell


class JobscriptState(enum.Enum):
    error = -1
    pending = 0
    running = 1
    finished = 2


class SlurmPosix(Scheduler):
    """

    Notes
    -----
    - runs in current working directory by default [2]

    # TODO: consider getting memory usage like: https://stackoverflow.com/a/44143229/5042280

    References
    ----------
    [1] https://manpages.org/sbatch
    [2] https://ri.itservices.manchester.ac.uk/csf4/batch/sge-to-slurm/

    """

    _app_attr = "app"

    DEFAULT_SHELL_EXECUTABLE = "/bin/bash"
    DEFAULT_SHEBANG_ARGS = ""
    DEFAULT_SUBMIT_CMD = "sbatch"
    DEFAULT_SHOW_CMD = ["squeue", "--me"]
    DEFAULT_DEL_CMD = "scancel"
    DEFAULT_JS_CMD = "#SBATCH"
    DEFAULT_ARRAY_SWITCH = "--array"
    DEFAULT_ARRAY_ITEM_VAR = "SLURM_ARRAY_TASK_ID"

    # maps scheduler states:
    state_lookup = {
        "PENDING": JobscriptElementState.pending,
        "RUNNING": JobscriptElementState.running,
        "COMPLETING": JobscriptElementState.running,
        "CANCELLED": JobscriptElementState.cancelled,
        "COMPLETED": JobscriptElementState.finished,
        "FAILED": JobscriptElementState.errored,
        "OUT_OF_MEMORY": JobscriptElementState.errored,
        "TIMEOUT": JobscriptElementState.errored,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format_core_request_lines(self, num_cores, num_nodes):
        # TODO: I think these partition names are set by the sysadmins, so they should
        # be set in the config file as a mapping between num_cores/nodes and partition
        # names. `sinfo -s` shows a list of available partitions

        lns = []
        if num_cores == 1:
            lns.append(f"{self.js_cmd} --partition serial")

        elif num_nodes == 1:
            lns.append(f"{self.js_cmd} --partition multicore")

        elif num_nodes > 1:
            lns.append(f"{self.js_cmd} --partition multinode")
            lns.append(f"{self.js_cmd} --nodes {num_nodes}")

        lns.append(f"{self.js_cmd} --ntasks {num_cores}")

        return lns

    def format_array_request(self, num_elements):
        return f"{self.js_cmd} {self.array_switch} 1-{num_elements}"

    def format_std_stream_file_option_lines(self, is_array, sub_idx):
        base = r"%x_"
        if is_array:
            base += r"%A.%a"
        else:
            base += r"%j"

        base = f"./artifacts/submissions/{sub_idx}/{base}"
        return [
            f"{self.js_cmd} -o {base}.out",
            f"{self.js_cmd} -e {base}.err",
        ]

    def format_options(self, resources, num_elements, is_array, sub_idx):
        opts = []
        opts.extend(
            self.format_core_request_lines(num_cores=resources.num_cores, num_nodes=1)
        )
        if is_array:
            opts.append(self.format_array_request(num_elements))

        opts.extend(self.format_std_stream_file_option_lines(is_array, sub_idx))
        opts.extend([f"{self.js_cmd} {opt}" for opt in self.options])
        return "\n".join(opts) + "\n"

    def get_version_info(self):
        vers_cmd = [self.submit_cmd, "--version"]
        proc = subprocess.run(
            args=vers_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout = proc.stdout.decode().strip()
        name, version = stdout.split()
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
        cmd = [self.submit_cmd, "--parsable"]

        dep_cmd = []
        for job_ID, is_array_dep in deps.values():
            dep_i_str = ""
            if is_array_dep:  # array dependency
                dep_i_str += "aftercorr:"
            else:
                dep_i_str += "afterany:"
            dep_i_str += str(job_ID)
            dep_cmd.append(dep_i_str)

        if dep_cmd:
            cmd.append(f"--dependency")
            cmd.append(",".join(dep_cmd))

        cmd.append(f'"{js_path}"')

        return cmd

    def parse_submission_output(self, stdout: str) -> str:
        """Extract scheduler reference for a newly submitted jobscript"""
        if ";" in stdout:
            job_ID, _ = stdout.split(";")  # since we submit with "--parsable"
        else:
            job_ID = stdout
        return job_ID

    def _parse_job_states(self, stdout) -> Dict[str, Dict[int, JobscriptElementState]]:
        """Parse output from Slurm `squeue` command with a simple format."""
        info = {}
        for ln in stdout.split("\n"):
            if not ln:
                continue
            ln_s = [i.strip() for i in ln.split()]
            job_ID_i = ln_s[0].split("_")
            state = self.state_lookup.get(ln_s[1], None)
            base_job_ID, arr_idx = job_ID_i if len(job_ID_i) == 2 else (job_ID_i[0], None)
            if arr_idx is not None:
                try:
                    arr_idx = [int(arr_idx) - 1]  # zero-index
                except ValueError:
                    # multiple arr indices; e.g. during pending
                    if "-" in arr_idx:
                        arr_0, arr_1 = [
                            int(i) - 1 for i in arr_idx.strip("[]").split("-")
                        ]
                        arr_idx = list(range(arr_0, arr_1 + 1))
                    else:
                        raise ValueError(f"Cannot parse job array index: {arr_idx}.")

            if base_job_ID not in info:
                info[base_job_ID] = {}

            for arr_idx_i in arr_idx or [None]:
                info[base_job_ID][arr_idx_i] = state

        return info

    @staticmethod
    def _query_job_states(job_IDs):
        """Query the state of the specified jobs."""
        cmd = [
            "squeue",
            "--me",
            "--noheader",
            "--format",
            r"%i %T",
            "--jobs",
            ",".join(job_IDs),
        ]
        return run_cmd(cmd)

    @staticmethod
    def _get_job_valid_IDs(job_IDs=None):
        """Get a list of job IDs that are known by the scheduler, optionally filtered by
        specified job IDs."""

        cmd = ["squeue", "--me", "--noheader", "--format", r"%F"]
        stdout, stderr = run_cmd(cmd)
        if stderr:
            raise ValueError(
                f"Could not get query Slurm jobs. Command was: {cmd!r}; stderr was: "
                f"{stderr}"
            )
        else:
            known_jobs = set(i.strip() for i in stdout.split("\n") if i.strip())
        job_IDs = known_jobs.intersection(job_IDs or [])

        return job_IDs

    def get_job_state_info(
        self, js_refs: List[str] = None
    ) -> Dict[str, Dict[int, JobscriptElementState]]:
        """Query the scheduler to get the states of all of this user's jobs, optionally
        filtering by specified job IDs.

        Jobs that are not in the scheduler's status output will not appear in the output
        of this method.

        """

        # if job_IDs are passed, then assume they are existant, otherwise retrieve valid
        # jobs:
        if not js_refs:
            js_refs = self._get_job_valid_IDs()
            if not js_refs:
                return {}

        stdout, stderr = self._query_job_states(js_refs)
        count = 0
        while stderr:
            if "Invalid job id specified" in stderr and count < 5:
                # the job might have finished; this only seems to happen if a single
                # non-existant job ID is specified; for multiple non-existant jobs, no
                # error is produced;
                print(
                    f"A specified job ID is non-existant; refreshing known job IDs..."
                )  # TODO: log instead of print
                time.sleep(2)
                js_refs = self._get_job_valid_IDs(js_refs)
                if not js_refs:
                    return {}
                stdout, stderr = self._query_job_states(js_refs)
                count += 1
            else:
                raise ValueError(f"Could not get Slurm job states. Stderr was: {stderr}")

        info = self._parse_job_states(stdout)
        return info

    def cancel_jobs(self, js_ref: List[str], jobscripts: List = None):
        cmd = [self.del_cmd, "--me"] + js_ref
        self.app.submission_logger.info(
            f"cancelling {self.__class__.__name__} jobscripts with command: {cmd}."
        )
        stdout, stderr = run_cmd(cmd)
        if stderr:
            raise ValueError(
                f"Could not get query {self.__class__.__name__} jobs. Command was: "
                f"{cmd!r}; stderr was: {stderr}"
            )
        self.app.submission_logger.info(
            f"jobscripts cancel command executed; stdout was: {stdout}."
        )
