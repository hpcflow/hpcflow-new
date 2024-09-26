"""
Operating system information discovery helpers.
"""

from __future__ import annotations
import platform
import re
import subprocess

DEFAULT_LINUX_RELEASE_FILE = "/etc/os-release"


def get_OS_info() -> dict[str, str]:
    """
    Get basic operating system version info.
    """
    uname = platform.uname()
    return {
        "OS_name": uname.system,
        "OS_version": uname.version,
        "OS_release": uname.release,
    }


def get_OS_info_windows() -> dict[str, str]:
    """
    Get operating system version info: Windows version.
    """
    return get_OS_info()


def get_OS_info_POSIX(
    WSL_executable: list[str] | None = None,
    use_py: bool = True,
    linux_release_file: str | None = None,
) -> dict[str, str]:
    """
    Get operating system version info: POSIX version.

    Parameters
    ----------
    WSL_executable:
        Executable to run subprocess calls via WSL on Windows.
    use_py:
        If True, use the :py:func:`platform.uname` Python function to get the OS
        information. Otherwise use subprocess to call ``uname``. We set this to False
        when getting OS info in WSL on Windows, since we need to call the WSL executable.
    linux_release_file:
        If on Linux, record the name and version fields from this file.

    """

    def try_subprocess_call(*args: str) -> str:
        exc = None
        command = [*WSL_exe, *args]
        try:
            proc = subprocess.run(
                args=command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except Exception as err:
            exc = err

        if proc.returncode or exc:
            raise RuntimeError(
                f"Failed to get POSIX OS info. Command was: {command!r}. Subprocess "
                f"exception was: {exc!r}. Stderr was: {proc.stderr!r}."
            )
        else:
            return proc.stdout

    WSL_exe = WSL_executable or []
    out: dict[str, str] = {}
    if use_py:
        out.update(**get_OS_info())

    else:
        OS_name = try_subprocess_call("uname", "-s").strip()
        OS_release = try_subprocess_call("uname", "-r").strip()
        OS_version = try_subprocess_call("uname", "-v").strip()

        out["OS_name"] = OS_name
        out["OS_release"] = OS_release
        out["OS_version"] = OS_version

    if out["OS_name"] == "Linux":
        # get linux distribution name and version:
        linux_release_file = linux_release_file or DEFAULT_LINUX_RELEASE_FILE
        release_out = try_subprocess_call("cat", linux_release_file)

        name_match = re.search(r"^NAME=\"(.*)\"", release_out, flags=re.MULTILINE)
        if name_match:
            lin_name = name_match.group(1)
        else:
            raise RuntimeError(
                f"Failed to get Linux distribution name from file `{linux_release_file}`."
            )

        version_match = re.search(r"^VERSION=\"(.*)\"", release_out, flags=re.MULTILINE)
        if version_match:
            lin_version = version_match.group(1)
        else:
            raise RuntimeError(
                f"Failed to get Linux distribution version from file "
                f"`{linux_release_file}`."
            )

        out["linux_release_file"] = linux_release_file
        out["linux_distribution_name"] = lin_name
        out["linux_distribution_version"] = lin_version

    return out
