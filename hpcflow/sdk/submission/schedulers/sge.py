from pathlib import Path
import subprocess
from typing import List, Tuple
from hpcflow.sdk.submission.schedulers import Scheduler
from hpcflow.sdk.submission.schedulers.shells import get_bash_version_info


class SGEPosix(Scheduler):
    """

    Notes
    -----
    - runs in serial by default

    References
    ----------
    [1] https://gridscheduler.sourceforge.net/htmlman/htmlman1/qsub.html

    """

    DEFAULT_SHELL_EXECUTABLE = "/bin/bash"
    DEFAULT_SUBMIT_CMD = "qsub"
    DEFAULT_SHOW_CMD = "qstat"
    DEFAULT_DEL_CMD = "qdel"
    DEFAULT_JS_CMD = "#$"
    DEFAULT_ARRAY_SWITCH = "-t"
    DEFAULT_ARRAY_ITEM_VAR = "SGE_TASK_ID"
    DEFAULT_CWD_SWITCH = "-cwd"

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

    def format_options(self, resources, num_elements, is_array):
        opts = []
        opts.append(self.format_switch(self.cwd_switch))
        opts.extend(self.format_core_request_lines(resources.num_cores, "smp.pe"))
        if is_array:
            opts.append(self.format_array_request(num_elements))
        return "\n".join(opts)

    def get_version_info(self):
        vers_cmd = [self.show_cmd, "-help"]
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
        out.update(get_bash_version_info())

        return out

    def get_submit_command(self, js_path: Path, deps: List[Tuple]) -> List[str]:

        cmd = [self.submit_cmd, "-terse"]

        dep_job_IDs = []
        dep_job_IDs_arr = []
        for job_ID, is_array_dep in deps:
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

        cmd.append(str(js_path))
        return cmd

    def parse_submission_output(self, stdout: str) -> str:
        """Extract scheduler reference for a newly submitted jobscript"""
        job_ID = stdout  # since we submit with "-terse"
        return job_ID
