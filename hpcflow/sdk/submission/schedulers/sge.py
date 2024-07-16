from __future__ import annotations
from collections.abc import Sequence
import re
from typing import TYPE_CHECKING
from typing_extensions import override
from hpcflow.sdk.core.errors import (
    IncompatibleSGEPEError,
    NoCompatibleSGEPEError,
    UnknownSGEPEError,
)
from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.submission.jobscript_info import JobscriptElementState
from hpcflow.sdk.submission.schedulers import QueuedScheduler
from hpcflow.sdk.submission.schedulers.utils import run_cmd

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from typing import Any, ClassVar
    from ...app import BaseApp
    from ...config.config import SchedulerConfigDescriptor
    from ...core.element import ElementResources
    from ..jobscript import Jobscript
    from ..shells.base import Shell


class SGEPosix(QueuedScheduler):
    """

    Notes
    -----
    - runs in serial by default

    References
    ----------
    [1] https://gridscheduler.sourceforge.net/htmlman/htmlman1/qsub.html
    [2] https://softpanorama.org/HPC/Grid_engine/Queues/queue_states.shtml

    """

    app: ClassVar[BaseApp]
    _app_attr: ClassVar[str] = "app"

    DEFAULT_SHEBANG_ARGS: ClassVar[str] = ""
    DEFAULT_SUBMIT_CMD: ClassVar[str] = "qsub"
    DEFAULT_SHOW_CMD: ClassVar[Sequence[str]] = "qstat"
    DEFAULT_DEL_CMD: ClassVar[str] = "qdel"
    DEFAULT_JS_CMD: ClassVar[str] = "#$"
    DEFAULT_ARRAY_SWITCH: ClassVar[str] = "-t"
    DEFAULT_ARRAY_ITEM_VAR: ClassVar[str] = "SGE_TASK_ID"
    DEFAULT_CWD_SWITCH: ClassVar[str] = "-cwd"
    DEFAULT_LOGIN_NODES_CMD: ClassVar[Sequence[str]] = ("qconf", "-sh")

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

    def __init__(self, cwd_switch: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cwd_switch = cwd_switch or self.DEFAULT_CWD_SWITCH

    @classmethod
    @override
    @TimeIt.decorator
    def process_resources(
        cls, resources: ElementResources, scheduler_config: SchedulerConfigDescriptor
    ) -> None:
        """Perform scheduler-specific processing to the element resources.

        Note: this mutates `resources`.

        """
        if resources.num_nodes is not None:
            raise ValueError(
                f"Specifying `num_nodes` for the {cls.__name__!r} scheduler is not "
                f"supported."
            )

        para_envs = scheduler_config.get("parallel_environments", {})

        if resources.SGE_parallel_env is not None:
            # check user-specified `parallel_env` is valid and compatible with
            # `num_cores`:
            if resources.num_cores and resources.num_cores > 1:
                raise ValueError(
                    f"An SGE parallel environment should not be specified if `num_cores` "
                    f"is 1 (`SGE_parallel_env` was specified as "
                    f"{resources.SGE_parallel_env!r})."
                )

            try:
                env = para_envs[resources.SGE_parallel_env]
            except KeyError:
                raise UnknownSGEPEError(
                    f"The SGE parallel environment {resources.SGE_parallel_env!r} is not "
                    f"specified in the configuration. Specified parallel environments "
                    f"are {list(para_envs.keys())!r}."
                )
            if not cls.is_num_cores_supported(resources.num_cores, env["num_cores"]):
                raise IncompatibleSGEPEError(
                    f"The SGE parallel environment {resources.SGE_parallel_env!r} is not "
                    f"compatible with the number of cores requested: "
                    f"{resources.num_cores!r}."
                )
        else:
            # find the first compatible PE:
            for pe_name, pe_info in para_envs.items():
                if cls.is_num_cores_supported(resources.num_cores, pe_info["num_cores"]):
                    resources.SGE_parallel_env = pe_name
                    break
            else:
                raise NoCompatibleSGEPEError(
                    f"No compatible SGE parallel environment could be found for the "
                    f"specified `num_cores` ({resources.num_cores!r})."
                )

    def get_login_nodes(self) -> list[str]:
        """Return a list of hostnames of login/administrative nodes as reported by the
        scheduler."""
        get_login = self.login_nodes_cmd
        assert isinstance(get_login, Sequence) and len(get_login) >= 1
        stdout, stderr = run_cmd(get_login)
        if stderr:
            print(stderr)
        return stdout.strip().split("\n")

    def _format_core_request_lines(self, resources: ElementResources) -> Iterator[str]:
        if resources.num_cores and resources.num_cores > 1:
            yield f"{self.js_cmd} -pe {resources.SGE_parallel_env} {resources.num_cores}"
        if resources.max_array_items:
            yield f"{self.js_cmd} -tc {resources.max_array_items}"

    def _format_array_request(self, num_elements: int) -> str:
        return f"{self.js_cmd} {self.array_switch} 1-{num_elements}"

    def _format_std_stream_file_option_lines(
        self, is_array: bool, sub_idx: int
    ) -> Iterator[str]:
        # note: we can't modify the file names
        yield f"{self.js_cmd} -o ./artifacts/submissions/{sub_idx}"
        yield f"{self.js_cmd} -e ./artifacts/submissions/{sub_idx}"

    @override
    def format_options(
        self, resources: ElementResources, num_elements: int, is_array: bool, sub_idx: int
    ) -> str:
        opts: list[str] = []
        opts.append(self.format_switch(self.cwd_switch))
        opts.extend(self._format_core_request_lines(resources))
        if is_array:
            opts.append(self._format_array_request(num_elements))

        opts.extend(self._format_std_stream_file_option_lines(is_array, sub_idx))

        for opt_k, opt_v in self.options.items():
            if opt_v is None:
                opts.append(f"{self.js_cmd} {opt_k}")
            elif isinstance(opt_v, list):
                for i in opt_v:
                    opts.append(f"{self.js_cmd} {opt_k} {i}")
            elif opt_v:
                opts.append(f"{self.js_cmd} {opt_k} {opt_v}")

        return "\n".join(opts) + "\n"

    @override
    @TimeIt.decorator
    def get_version_info(self):
        vers_cmd = self.show_cmd + ["-help"]
        stdout, stderr = run_cmd(vers_cmd)
        if stderr:
            print(stderr)
        version_str = stdout.split("\n")[0].strip()
        name, version = version_str.split()
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
        cmd = [self.submit_cmd, "-terse"]

        dep_job_IDs: list[str] = []
        dep_job_IDs_arr: list[str] = []
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

        cmd.append(js_path)
        return cmd

    def parse_submission_output(self, stdout: str) -> str:
        """Extract scheduler reference for a newly submitted jobscript"""
        match = re.search(r"^\d+", stdout)
        if match:
            job_ID = match.group()
        else:
            raise RuntimeError(f"Could not parse Job ID from scheduler output {stdout!r}")
        return job_ID

    def get_job_statuses(self) -> dict[str, dict[int | None, JobscriptElementState]]:
        """Get information about all of this user's jobscripts that currently listed by
        the scheduler."""
        cmd = [*self.show_cmd, "-u", "$USER", "-g", "d"]  # "-g d": separate arrays items
        stdout, stderr = run_cmd(cmd, logger=self.app.submission_logger)
        if stderr:
            raise ValueError(
                f"Could not get query SGE jobs. Command was: {cmd!r}; stderr was: "
                f"{stderr}"
            )
        elif not stdout:
            return {}

        info: dict[str, dict[int | None, JobscriptElementState]] = {}
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

            arr_idx_s = ln[task_id_idx:].strip()
            arr_idx = (
                int(arr_idx_s) - 1  # We are using zero-indexed info
                if arr_idx_s
                else None
            )

            info.setdefault(base_job_ID, {})[arr_idx] = state
        return info

    @override
    def get_job_state_info(
        self, *, js_refs: list[str] | None = None, num_js_elements: int = 0
    ) -> Mapping[str, Mapping[int | None, JobscriptElementState]]:
        """Query the scheduler to get the states of all of this user's jobs, optionally
        filtering by specified job IDs.

        Jobs that are not in the scheduler's status output will not appear in the output
        of this method.

        """
        info = self.get_job_statuses()
        if js_refs:
            return {k: v for k, v in info.items() if k in js_refs}
        return info

    @override
    def cancel_jobs(
        self,
        js_refs: list[str],
        jobscripts: list[Jobscript] | None = None,
        num_js_elements: int = 0,  # Ignored!
    ):
        cmd = [self.del_cmd] + js_refs
        self.app.submission_logger.info(
            f"cancelling {self.__class__.__name__} jobscripts with command: {cmd}."
        )
        stdout, stderr = run_cmd(cmd, logger=self.app.submission_logger)
        if stderr:
            raise ValueError(
                f"Could not get query SGE {self.__class__.__name__}. Command was: "
                f"{cmd!r}; stderr was: {stderr}"
            )
        self.app.submission_logger.info(
            f"jobscripts cancel command executed; stdout was: {stdout}."
        )
