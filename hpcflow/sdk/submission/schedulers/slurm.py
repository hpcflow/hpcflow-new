from pathlib import Path
import subprocess
from typing import List, Tuple
from hpcflow.sdk.submission.schedulers import Scheduler
from hpcflow.sdk.submission.shells.base import Shell


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

    DEFAULT_SHELL_EXECUTABLE = "/bin/bash"
    DEFAULT_SHEBANG_ARGS = ""
    DEFAULT_SUBMIT_CMD = "sbatch"
    DEFAULT_SHOW_CMD = "squeue --me"
    DEFAULT_DEL_CMD = "scancel"
    DEFAULT_JS_CMD = "#SBATCH"
    DEFAULT_ARRAY_SWITCH = "--array"
    DEFAULT_ARRAY_ITEM_VAR = "SLURM_ARRAY_TASK_ID"

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

        cmd.append(js_path)

        return cmd

    def parse_submission_output(self, stdout: str) -> str:
        """Extract scheduler reference for a newly submitted jobscript"""
        if ";" in stdout:
            job_ID, _ = stdout.split(";")  # since we submit with "--parsable"
        else:
            job_ID = stdout
        return job_ID
