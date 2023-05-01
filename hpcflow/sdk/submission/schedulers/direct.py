from pathlib import Path
from typing import List, Tuple
from hpcflow.sdk.submission.schedulers import NullScheduler


class DirectPosix(NullScheduler):

    DEFAULT_SHELL_EXECUTABLE = "/bin/bash"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DirectWindows(NullScheduler):

    DEFAULT_SHELL_EXECUTABLE = "powershell.exe"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
