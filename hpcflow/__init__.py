from hpcflow._version import __version__
from hpcflow.app_log import AppLog
from hpcflow.runtime import RunTimeInfo

log = AppLog(__name__)
RUN_TIME_INFO = RunTimeInfo(__name__, __version__)

from hpcflow.config import ConfigLoader  # uses RUN_TIME_INFO
from hpcflow.hpcflow import HPCFlowApp  # uses AppLog


def set_config_dir(config_dir):
    ConfigLoader(config_dir=config_dir)
