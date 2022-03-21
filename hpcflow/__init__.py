from hpcflow._version import __version__
from hpcflow.app_log import AppLog
from hpcflow.runtime import RunTimeInfo

log = AppLog(__name__)
RUN_TIME_INFO = RunTimeInfo(__name__)

from hpcflow.hpcflow import HPCFlowApp
