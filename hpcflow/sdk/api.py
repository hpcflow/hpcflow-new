"""API functions, which are dynamically added to the BaseApp class on __init__"""
from __future__ import annotations

import importlib
import os
from typing import Optional, TypeVar, Union
from hpcflow.sdk.core import ALL_TEMPLATE_FORMATS, DEFAULT_TEMPLATE_FORMAT
from hpcflow.sdk.persistence import DEFAULT_STORE_FORMAT

from hpcflow.sdk.submission.shells import get_shell
from hpcflow.sdk.submission.shells.os_version import (
    get_OS_info_POSIX,
    get_OS_info_windows,
)
from hpcflow.sdk.typing import PathLike
from hpcflow.sdk import app as sdk_app

AppType = TypeVar("AppType", sdk_app.BaseApp, sdk_app.App)
