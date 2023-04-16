from hpcflow.sdk.submission.schedulers import NullScheduler
from hpcflow.sdk.submission.schedulers.shells import (
    get_bash_version_info,
    get_powershell_version_info,
)


class DirectPosix(NullScheduler):

    DEFAULT_SHELL_EXECUTABLE = "/bin/bash"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_version_info(self):
        return get_bash_version_info(executable=self.shell_executable)


class DirectWindows(NullScheduler):

    DEFAULT_SHELL_EXECUTABLE = "powershell.exe"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_version_info(self):
        return get_powershell_version_info(executable=self.shell_executable)
