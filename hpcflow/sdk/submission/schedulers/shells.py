import subprocess
from typing import Dict

# TODO: jobscript is a function of (scheduler, OS and shell), so should reflect that in how
# we compose it.


def get_bash_version_info(executable: str = "/bin/bash") -> Dict:
    """Get the bash version number as a string.

    Example first line of output from --version is:
    "GNU bash, version 4.2.46(2)-release (x86_64-redhat-linux-gnu)" for which this
    function will return `{"version": "4.2.46(2)-release"}`.

    """
    proc = subprocess.run(
        args=[executable, "--version"],
        stdout=subprocess.PIPE,
        text=True,
    )
    if proc.returncode == 0:
        first_line = proc.stdout.splitlines()[0]
        version = first_line.split(" ")[3]
    else:
        raise RuntimeError("Failed to parse bash version information.")

    return {
        "shell_name": "bash",
        "shell_executable": executable,
        "shell_version": version,
    }


def get_powershell_version_info(executable: str = "powershell.exe") -> Dict:
    """Get the powershell version number as a string."""

    proc = subprocess.run(
        args=[executable, "$PSVersionTable.PSVersion.ToString()"],
        stdout=subprocess.PIPE,
        text=True,
    )
    if proc.returncode == 0:
        version = proc.stdout.strip()
    else:
        raise RuntimeError("Failed to parse bash version information.")

    return {
        "shell_name": "powershell",
        "shell_executable": executable,
        "shell_version": version,
    }
