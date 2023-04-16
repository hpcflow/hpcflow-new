from pathlib import Path
import subprocess
from typing import List, Tuple
from hpcflow.sdk.submission.schedulers import Scheduler
from hpcflow.sdk.submission.schedulers.shells import get_bash_version_info


class SlurmPosix(Scheduler):
    """

    Notes
    -----
    - runs in current working directory by default [2]


    References
    ----------
    [1] https://manpages.org/sbatch
    [2] https://ri.itservices.manchester.ac.uk/csf4/batch/sge-to-slurm/

    """

    DEFAULT_SHELL_EXECUTABLE = "/bin/bash"
    DEFAULT_SUBMIT_CMD = "sbatch"
    DEFAULT_SHOW_CMD = "squeue --me"
    DEFAULT_DEL_CMD = "scancel"
    DEFAULT_JS_CMD = "#SBATCH"
    DEFAULT_ARRAY_SWITCH = "--array"
    DEFAULT_ARRAY_ITEM_VAR = "SLURM_ARRAY_TASK_ID"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def format_core_request_lines(self, num_cores, num_nodes):

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

    def format_options(self, resources, num_elements, is_array):
        opts = []
        opts.extend(
            self.format_core_request_lines(num_cores=resources.num_cores, num_nodes=1)
        )
        if is_array:
            opts.append(self.format_array_request(num_elements))
        return "\n".join(opts)

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
            "scheduler_versions": version,
        }
        out.update(get_bash_version_info())

        return out

    def get_submit_command(self, js_path: Path, deps: List[Tuple]) -> List[str]:

        cmd = [self.submit_cmd, "--parsable"]

        dep_cmd = []
        for job_ID, is_array_dep in deps:
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

        cmd.append(str(js_path))

        return cmd

    def parse_submission_output(self, stdout: str, stderr: str) -> str:
        """Extract scheduler reference for a newly submitted jobscript"""
        job_ID, _ = stdout.split(";")  # since we submit with "--parsable"
        return job_ID
