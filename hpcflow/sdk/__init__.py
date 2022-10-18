"""Module to define an extensible hpcFlow application class."""
import logging
import os

_SDK_CONSOLE_LOG_LEVEL = os.environ.get("HPCFLOW_SDK_CONSOLE_LOG_LEVEL", "ERROR")

SDK_logger = logging.getLogger()
SDK_logger.setLevel("DEBUG")

_sh = logging.StreamHandler()
_sh.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
_sh.setLevel(_SDK_CONSOLE_LOG_LEVEL)
SDK_logger.addHandler(_sh)

from hpcflow.sdk.app import App
from hpcflow.sdk.config import ConfigOptions
