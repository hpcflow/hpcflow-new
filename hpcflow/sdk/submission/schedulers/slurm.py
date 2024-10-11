"""
An interface to SLURM.
"""

from __future__ import annotations
import subprocess
import time
from typing import TYPE_CHECKING
from typing_extensions import override
from hpcflow.sdk.core.enums import ParallelMode
from hpcflow.sdk.core.errors import (
    IncompatibleParallelModeError,
    IncompatibleSLURMArgumentsError,
    IncompatibleSLURMPartitionError,
    UnknownSLURMPartitionError,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers import QueuedScheduler
from hpcflow.sdk.submission.schedulers.utils import run_cmd

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from typing import Any, ClassVar
    from ...config.types import SchedulerConfigDescriptor, SLURMPartitionsDescriptor
    from ...core.element import ElementResources
    from ..jobscript import Jobscript
    from ..shells.base import Shell


class SlurmPosix(QueuedScheduler):
    """
    A scheduler that uses SLURM.

    Keyword Args
    ------------
    shell_args: str
        Arguments to pass to the shell. Pre-quoted.
    shebang_args: str
        Arguments to set on the shebang line. Pre-quoted.
    options: dict
        Options to the jobscript command.

    Notes
    -----
    - runs in current working directory by default [2]

    Todo
    ----
    - consider getting memory usage like: https://stackoverflow.com/a/44143229/5042280

    References
    ----------
    [1] https://manpages.org/sbatch
    [2] https://ri.itservices.manchester.ac.uk/csf4/batch/sge-to-slurm/

    """

    #: Default shell.
    DEFAULT_SHELL_EXECUTABLE: ClassVar[str] = "/bin/bash"
    #: Default args for shebang line.
    DEFAULT_SHEBANG_ARGS: ClassVar[str] = ""
    #: Default submission command.
    DEFAULT_SUBMIT_CMD: ClassVar[str] = "sbatch"
    #: Default command to show the queue state.
    DEFAULT_SHOW_CMD: ClassVar[Sequence[str]] = ("squeue", "--me")
    #: Default cancel command.
    DEFAULT_DEL_CMD: ClassVar[str] = "scancel"
    #: Default job control directive prefix.
    DEFAULT_JS_CMD: ClassVar[str] = "#SBATCH"
    #: Default prefix to enable array processing.
    DEFAULT_ARRAY_SWITCH: ClassVar[str] = "--array"
    #: Default shell variable with array ID.
    DEFAULT_ARRAY_ITEM_VAR: ClassVar[str] = "SLURM_ARRAY_TASK_ID"
    #: Number of times to try when querying the state.
    NUM_STATE_QUERY_TRIES: ClassVar[int] = 5
    #: Delay (in seconds) between attempts to query the state.
    INTER_STATE_QUERY_DELAY: ClassVar[float] = 0.5

    #: Maps scheduler state codes to :py:class:`JobscriptElementState` values.
    state_lookup: ClassVar[Mapping[str, JobscriptElementState]] = {
        "PENDING": JobscriptElementState.pending,
        "RUNNING": JobscriptElementState.running,
        "COMPLETING": JobscriptElementState.running,
        "CANCELLED": JobscriptElementState.cancelled,
        "COMPLETED": JobscriptElementState.finished,
        "FAILED": JobscriptElementState.errored,
        "OUT_OF_MEMORY": JobscriptElementState.errored,
        "TIMEOUT": JobscriptElementState.errored,
    }

    @classmethod
    @override
    @TimeIt.decorator
    def process_resources(
        cls, resources: ElementResources, scheduler_config: SchedulerConfigDescriptor
    ) -> None:
        """Perform scheduler-specific processing to the element resources.

        Note
        ----
        This mutates `resources`.
        """
        if resources.is_parallel:
            if resources.parallel_mode is None:
                # set default parallel mode:
                resources.parallel_mode = ParallelMode.DISTRIBUTED

            if resources.parallel_mode is ParallelMode.SHARED:
                if (resources.num_nodes and resources.num_nodes > 1) or (
                    resources.SLURM_num_nodes and resources.SLURM_num_nodes > 1
                ):
                    raise IncompatibleParallelModeError(
                        f"For the {resources.parallel_mode.name.lower()} parallel mode, "
                        f"only a single node may be requested."
                    )
                # consider `num_cores` and `num_threads` synonyms in this case:
                if resources.SLURM_num_tasks and resources.SLURM_num_tasks != 1:
                    raise IncompatibleSLURMArgumentsError(
                        f"For the {resources.parallel_mode.name.lower()} parallel mode, "
                        f"`SLURM_num_tasks` must be set to 1 (to ensure all requested "
                        f"cores reside on the same node)."
                    )
                resources.SLURM_num_tasks = 1

                if resources.SLURM_num_cpus_per_task == 1:
                    raise IncompatibleSLURMArgumentsError(
                        f"For the {resources.parallel_mode.name.lower()} parallel mode, "
                        f"if `SLURM_num_cpus_per_task` is set, it must be set to the "
                        f"number of threads/cores to use, and so must be greater than 1, "
                        f"but {resources.SLURM_num_cpus_per_task!r} was specified."
                    )
                resources.num_threads = resources.num_threads or resources.num_cores
                if not resources.num_threads and not resources.SLURM_num_cpus_per_task:
                    raise ValueError(
                        f"For the {resources.parallel_mode.name.lower()} parallel "
                        f"mode, specify `num_threads` (or its synonym for this "
                        f"parallel mode: `num_cores`), or the SLURM-specific "
                        f"parameter `SLURM_num_cpus_per_task`."
                    )
                elif (resources.num_threads and resources.SLURM_num_cpus_per_task) and (
                    resources.num_threads != resources.SLURM_num_cpus_per_task
                ):
                    raise IncompatibleSLURMArgumentsError(
                        f"Incompatible parameters for `num_cores`/`num_threads` "
                        f"({resources.num_threads}) and `SLURM_num_cpus_per_task` "
                        f"({resources.SLURM_num_cpus_per_task}) for the "
                        f"{resources.parallel_mode.name.lower()} parallel mode."
                    )
                resources.SLURM_num_cpus_per_task = resources.num_threads

            elif resources.parallel_mode is ParallelMode.DISTRIBUTED:
                if resources.num_threads:
                    raise ValueError(
                        f"For the {resources.parallel_mode.name.lower()} parallel "
                        f"mode, specifying `num_threads` is not permitted."
                    )
                if (
                    resources.SLURM_num_tasks
                    and resources.num_cores
                    and resources.SLURM_num_tasks != resources.num_cores
                ):
                    raise IncompatibleSLURMArgumentsError(
                        f"Incompatible parameters for `num_cores` ({resources.num_cores})"
                        f" and `SLURM_num_tasks` ({resources.SLURM_num_tasks}) for the "
                        f"{resources.parallel_mode.name.lower()} parallel mode."
                    )
                elif not resources.SLURM_num_tasks and resources.num_cores:
                    resources.SLURM_num_tasks = resources.num_cores
                elif (
                    resources.SLURM_num_tasks_per_node
                    and resources.num_cores_per_node
                    and resources.SLURM_num_tasks_per_node != resources.num_cores_per_node
                ):
                    raise IncompatibleSLURMArgumentsError(
                        f"Incompatible parameters for `num_cores_per_node` "
                        f"({resources.num_cores_per_node}) and `SLURM_num_tasks_per_node`"
                        f" ({resources.SLURM_num_tasks_per_node}) for the "
                        f"{resources.parallel_mode.name.lower()} parallel mode."
                    )
                elif (
                    not resources.SLURM_num_tasks_per_node
                    and resources.num_cores_per_node
                ):
                    resources.SLURM_num_tasks_per_node = resources.num_cores_per_node

                if (
                    resources.SLURM_num_nodes
                    and resources.num_nodes
                    and resources.SLURM_num_nodes != resources.num_nodes
                ):
                    raise IncompatibleSLURMArgumentsError(
                        f"Incompatible parameters for `num_nodes` ({resources.num_nodes})"
                        f" and `SLURM_num_nodes` ({resources.SLURM_num_nodes}) for the "
                        f"{resources.parallel_mode.name.lower()} parallel mode."
                    )
                elif not resources.SLURM_num_nodes and resources.num_nodes:
                    resources.SLURM_num_nodes = resources.num_nodes

            elif resources.parallel_mode is ParallelMode.HYBRID:
                raise NotImplementedError("hybrid parallel mode not yet supported.")

        else:
            if resources.SLURM_is_parallel:
                raise IncompatibleSLURMArgumentsError(
                    "Some specified SLURM-specific arguments (which indicate a parallel "
                    "job) conflict with the scheduler-agnostic arguments (which "
                    "indicate a serial job)."
                )
            if not resources.SLURM_num_tasks:
                resources.SLURM_num_tasks = 1

            if resources.SLURM_num_tasks_per_node:
                resources.SLURM_num_tasks_per_node = None

            if not resources.SLURM_num_nodes:
                resources.SLURM_num_nodes = 1

            if not resources.SLURM_num_cpus_per_task:
                resources.SLURM_num_cpus_per_task = 1

        num_cores = resources.num_cores or resources.SLURM_num_tasks
        num_cores_per_node = (
            resources.num_cores_per_node or resources.SLURM_num_tasks_per_node
        )
        num_nodes = resources.num_nodes or resources.SLURM_num_nodes
        para_mode = resources.parallel_mode

        # select matching partition if possible:
        all_parts = scheduler_config.get("partitions", {})
        if resources.SLURM_partition is not None:
            # check user-specified partition is valid and compatible with requested
            # cores/nodes:
            try:
                part = all_parts[resources.SLURM_partition]
            except KeyError:
                raise UnknownSLURMPartitionError(
                    f"The SLURM partition {resources.SLURM_partition!r} is not "
                    f"specified in the configuration. Specified partitions are "
                    f"{list(all_parts.keys())!r}."
                )
            # TODO: we when we support ParallelMode.HYBRID, these checks will have to
            # consider the total number of cores requested per node
            # (num_cores_per_node * num_threads)?
            part_num_cores = part.get("num_cores", [])
            part_num_cores_per_node = part.get("num_cores_per_node", [])
            part_num_nodes = part.get("num_nodes", [])
            part_para_modes = part.get("parallel_modes", [])
            if cls.__is_present_unsupported(num_cores, part_num_cores):
                raise IncompatibleSLURMPartitionError(
                    f"The SLURM partition {resources.SLURM_partition!r} is not "
                    f"compatible with the number of cores requested: {num_cores!r}."
                )
            if cls.__is_present_unsupported(num_cores_per_node, part_num_cores_per_node):
                raise IncompatibleSLURMPartitionError(
                    f"The SLURM partition {resources.SLURM_partition!r} is not "
                    f"compatible with the number of cores per node requested: "
                    f"{num_cores_per_node!r}."
                )
            if cls.__is_present_unsupported(num_nodes, part_num_nodes):
                raise IncompatibleSLURMPartitionError(
                    f"The SLURM partition {resources.SLURM_partition!r} is not "
                    f"compatible with the number of nodes requested: {num_nodes!r}."
                )
            if para_mode and para_mode.name.lower() not in part_para_modes:
                raise IncompatibleSLURMPartitionError(
                    f"The SLURM partition {resources.SLURM_partition!r} is not "
                    f"compatible with the parallel mode requested: {para_mode!r}."
                )
        else:
            # find the first compatible partition if one exists:
            # TODO: bug here? not finding correct partition?
            for part_name, part_info in all_parts.items():
                if cls.__partition_matches(
                    num_cores, num_cores_per_node, num_nodes, para_mode, part_info
                ):
                    resources.SLURM_partition = str(part_name)
                    break
            else:
                part_names = ", ".join(all_parts.keys())
                raise IncompatibleSLURMPartitionError(
                    f"No known SLURM partition ({part_names}) matches the number of "
                    f"cores and nodes requested."
                )

    @classmethod
    def __is_present_unsupported(
        cls, num_req: int | None, part_have: list[int] | None
    ) -> bool:
        """
        Test if information is present on both sides, but doesn't match.
        """
        return bool(
            num_req and part_have and not cls.is_num_cores_supported(num_req, part_have)
        )

    @classmethod
    def __is_present_supported(
        cls, num_req: int | None, part_have: list[int] | None
    ) -> bool:
        """
        Test if information is present on both sides, and also matches.
        """
        return bool(
            num_req and part_have and cls.is_num_cores_supported(num_req, part_have)
        )

    @classmethod
    def __partition_matches(
        cls,
        num_cores: int | None,
        num_cores_per_node: int | None,
        num_nodes: int | None,
        para_mode: ParallelMode | None,
        part_info: SLURMPartitionsDescriptor,
    ) -> bool:
        """
        Check whether a partition (part_name, part_info) matches the requested number
        of cores and nodes.
        """
        part_num_cores = part_info.get("num_cores", [])
        part_num_cores_per_node = part_info.get("num_cores_per_node", [])
        part_num_nodes = part_info.get("num_nodes", [])
        part_para_modes = part_info.get("parallel_modes", [])
        if (
            not cls.__is_present_supported(num_cores, part_num_cores)
            or not cls.__is_present_supported(num_cores_per_node, part_num_cores_per_node)
            or not cls.__is_present_supported(num_nodes, part_num_nodes)
        ):
            return False
        # FIXME: Does the next check come above or below the check below?
        # Surely not both!
        part_match = True
        if part_match:
            return True
        if para_mode and para_mode.name.lower() not in part_para_modes:
            return False
        if part_match:
            return True
        return False

    def _format_core_request_lines(self, resources: ElementResources) -> Iterator[str]:
        if resources.SLURM_partition:
            yield f"{self.js_cmd} --partition {resources.SLURM_partition}"
        if resources.SLURM_num_nodes:  # TODO: option for --exclusive ?
            yield f"{self.js_cmd} --nodes {resources.SLURM_num_nodes}"
        if resources.SLURM_num_tasks:
            yield f"{self.js_cmd} --ntasks {resources.SLURM_num_tasks}"
        if resources.SLURM_num_tasks_per_node:
            yield f"{self.js_cmd} --ntasks-per-node {resources.SLURM_num_tasks_per_node}"
        if resources.SLURM_num_cpus_per_task:
            yield f"{self.js_cmd} --cpus-per-task {resources.SLURM_num_cpus_per_task}"

    def _format_array_request(self, num_elements: int, resources: ElementResources):
        # TODO: Slurm docs start indices at zero, why are we starting at one?
        #   https://slurm.schedmd.com/sbatch.html#OPT_array
        max_str = f"%{resources.max_array_items}" if resources.max_array_items else ""
        return f"{self.js_cmd} {self.array_switch} 1-{num_elements}{max_str}"

    def _format_std_stream_file_option_lines(
        self, is_array: bool, sub_idx: int
    ) -> Iterator[str]:
        pattern = R"%x_%A.%a" if is_array else R"%x_%j"
        base = f"./artifacts/submissions/{sub_idx}/{pattern}"
        yield f"{self.js_cmd} -o {base}.out"
        yield f"{self.js_cmd} -e {base}.err"

    @override
    def format_options(
        self, resources: ElementResources, num_elements: int, is_array: bool, sub_idx: int
    ) -> str:
        """
        Format the options to the scheduler.
        """
        opts: list[str] = []
        opts.extend(self._format_core_request_lines(resources))

        if is_array:
            opts.append(self._format_array_request(num_elements, resources))

        opts.extend(self._format_std_stream_file_option_lines(is_array, sub_idx))

        for opt_k, opt_v in self.options.items():
            if isinstance(opt_v, list):
                for i in opt_v:
                    opts.append(f"{self.js_cmd} {opt_k} {i}")
            elif opt_v:
                opts.append(f"{self.js_cmd} {opt_k} {opt_v}")
            elif opt_v is None:
                opts.append(f"{self.js_cmd} {opt_k}")

        return "\n".join(opts) + "\n"

    @override
    @TimeIt.decorator
    def get_version_info(self):
        vers_cmd = [self.submit_cmd, "--version"]
        proc = subprocess.run(
            args=vers_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout = proc.stdout.decode().strip()
        stderr = proc.stderr.decode().strip()
        if stderr:
            print(stderr)
        name, version = stdout.split()
        return {
            "scheduler_name": name,
            "scheduler_version": version,
        }

    @override
    def get_submit_command(
        self,
        shell: Shell,
        js_path: str,
        deps: dict[Any, tuple[Any, ...]],
    ) -> list[str]:
        """
        Get the command to use to submit a job to the scheduler.

        Returns
        -------
        List of argument words.
        """
        cmd = [self.submit_cmd, "--parsable"]

        dep_cmd: list[str] = []
        for job_ID, is_array_dep in deps.values():
            dep_i_str = ""
            if is_array_dep:  # array dependency
                dep_i_str += "aftercorr:"
            else:
                dep_i_str += "afterany:"
            dep_i_str += str(job_ID)
            dep_cmd.append(dep_i_str)

        if dep_cmd:
            cmd.append("--dependency")
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

    @staticmethod
    def _parse_job_IDs(job_ID_str: str) -> tuple[str, None | list[int]]:
        """Parse the job ID column from the `squeue` command (the `%i` format option)."""
        parts = job_ID_str.split("_")
        base_job_ID, arr_idx = parts if len(parts) == 2 else (parts[0], None)
        assert base_job_ID is not None
        if arr_idx is None:
            return base_job_ID, None
        try:
            return base_job_ID, [int(arr_idx) - 1]  # zero-index
        except ValueError:
            # split on commas (e.g. "[5,8-40]")
            _arr_idx: list[int] = []
            for i_range_str in arr_idx.strip("[]").split(","):
                if "-" in i_range_str:
                    range_parts = i_range_str.split("-")
                    if "%" in range_parts[1]:
                        # indicates max concurrent array items; not needed
                        range_parts[1] = range_parts[1].split("%")[0]
                    i_args = [int(j) - 1 for j in range_parts]
                    _arr_idx.extend(range(i_args[0], i_args[1] + 1))
                else:
                    _arr_idx.append(int(i_range_str) - 1)
            return base_job_ID, _arr_idx

    def _parse_job_states(
        self, stdout: str
    ) -> dict[str, dict[int | None, JobscriptElementState]]:
        """Parse output from Slurm `squeue` command with a simple format."""
        info: dict[str, dict[int | None, JobscriptElementState]] = {}
        for ln in stdout.split("\n"):
            if not ln:
                continue
            ln_s = [i.strip() for i in ln.split()]
            base_job_ID, arr_idx = self._parse_job_IDs(ln_s[0])
            state = self.state_lookup.get(ln_s[1], JobscriptElementState.errored)

            entry = info.setdefault(base_job_ID, {})
            for arr_idx_i in arr_idx or ():
                entry[arr_idx_i] = state

        return info

    def _query_job_states(self, job_IDs: list[str]) -> tuple[str, str]:
        """Query the state of the specified jobs."""
        cmd = [
            "squeue",
            "--me",
            "--noheader",
            "--format",
            R"%40i %30T",
            "--jobs",
            ",".join(job_IDs),
        ]
        return run_cmd(cmd, logger=self._app.submission_logger)

    def _get_job_valid_IDs(self, job_IDs: list[str] | None = None) -> list[str]:
        """Get a list of job IDs that are known by the scheduler, optionally filtered by
        specified job IDs."""

        cmd = ["squeue", "--me", "--noheader", "--format", r"%F"]
        stdout, stderr = run_cmd(cmd, logger=self._app.submission_logger)
        if stderr:
            raise ValueError(
                f"Could not get query Slurm jobs. Command was: {cmd!r}; stderr was: "
                f"{stderr}"
            )
        else:
            known_jobs = set(i.strip() for i in stdout.split("\n") if i.strip())
        if job_IDs is None:
            return list(known_jobs)
        return list(known_jobs.intersection(job_IDs))

    @override
    def get_job_state_info(
        self, *, js_refs: list[str] | None = None, num_js_elements: int = 0
    ) -> Mapping[str, Mapping[int | None, JobscriptElementState]]:
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

        count = 0
        while True:
            stdout, stderr = self._query_job_states(js_refs)
            if not stderr:
                return self._parse_job_states(stdout)
            if "Invalid job id specified" not in stderr:
                raise ValueError(f"Could not get Slurm job states. Stderr was: {stderr}")
            if count >= self.NUM_STATE_QUERY_TRIES:
                raise ValueError(f"Could not get Slurm job states. Stderr was: {stderr}")

            # the job might have finished; this only seems to happen if a single
            # non-existant job ID is specified; for multiple non-existant jobs, no
            # error is produced;
            self._app.submission_logger.info(
                "A specified job ID is non-existant; refreshing known job IDs..."
            )
            time.sleep(self.INTER_STATE_QUERY_DELAY)
            js_refs = self._get_job_valid_IDs(js_refs)
            if not js_refs:
                return {}
            count += 1

    @override
    def cancel_jobs(
        self,
        js_refs: list[str],
        jobscripts: list[Jobscript] | None = None,
        num_js_elements: int = 0,  # Ignored!
    ):
        """
        Cancel submitted jobs.
        """
        cmd = [self.del_cmd] + js_refs
        self._app.submission_logger.info(
            f"cancelling {self.__class__.__name__} jobscripts with command: {cmd}."
        )
        stdout, stderr = run_cmd(cmd, logger=self._app.submission_logger)
        if stderr:
            raise ValueError(
                f"Could not get query {self.__class__.__name__} jobs. Command was: "
                f"{cmd!r}; stderr was: {stderr}"
            )
        self._app.submission_logger.info(
            f"jobscripts cancel command executed; stdout was: {stdout}."
        )
