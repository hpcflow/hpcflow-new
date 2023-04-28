from abc import ABC, abstractmethod
import subprocess
from typing import Dict, List, Optional


class Shell(ABC):
    """Class to represent a shell and templates for jobscript composition.

    This class represents a combination of a shell and an OS. For example, running
    bash on a POSIX OS, and provides snippets that are used to compose a jobscript for
    that combination.

    """

    def __init__(self, executable=None, os_args=None):
        self._executable = executable or self.DEFAULT_EXE
        self.os_args = os_args

    @property
    def executable(self) -> List[str]:
        return [self._executable]

    @abstractmethod
    def get_version_info(self, exclude_os: Optional[bool] = False) -> Dict:
        """Get shell and operating system information."""
