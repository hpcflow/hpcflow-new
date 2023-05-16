"""Module to define an extensible hpcFlow application class."""
import logging
import os
import sys

_SDK_CONSOLE_LOG_LEVEL = os.environ.get("HPCFLOW_SDK_CONSOLE_LOG_LEVEL", "WARNING")


def get_SDK_logger(name=None):
    """Get a logger with prefix of "hpcflow_sdk" instead of "hpcflow.sdk" to ensure the
    handlers of the SDK logger and app logger are distinct."""
    name = ".".join(["hpcflow_sdk"] + (name or __name__).split(".")[2:])
    return logging.getLogger(name)


_SDK_logger = get_SDK_logger()
_SDK_logger.setLevel("DEBUG")

_sh = logging.StreamHandler()
_sh.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
_sh.setLevel(_SDK_CONSOLE_LOG_LEVEL)
_SDK_logger.addHandler(_sh)

if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
    import multiprocessing

    multiprocessing.freeze_support()

from hpcflow.sdk.app import App
from hpcflow.sdk.config import ConfigOptions
