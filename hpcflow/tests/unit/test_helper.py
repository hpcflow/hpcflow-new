import pytest
from datetime import datetime, timedelta
import time

import io
import sys

from click.testing import CliRunner

from hpcflow.sdk.helper import helper
from tempfile import gettempdir
from hpcflow.api import hpcflow, load_config


@pytest.fixture
def app():
    load_config(config_dir=gettempdir())
    return hpcflow


def test_modify_helper(app):
    tstart = datetime.now() - timedelta(seconds=0.2)

    helper.start_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)
    time.sleep(0.5)

    # This checks that parameters already in the file are being compared to new inputs
    so = io.StringIO()  # Create StringIO object
    sys.stdout = so  # Redirect stdout.
    helper.modify_helper(app, timeout=60, timeout_check_interval=1, watch_interval=3)
    assert so.getvalue().splitlines()[-1] == "Helper parameters already met."
    sys.stdout = sys.__stdout__  # Reset stdout.

    helper.modify_helper(app, timeout=60, timeout_check_interval=2, watch_interval=1)
    # This checks if the file was written with new variables
    args = helper.read_helper_args(app)
    assert args["timeout"] == 60
    assert args["timeout_check_interval"] == 2
    assert args["watch_interval"] == 1
    time.sleep(1.5)

    helper.modify_helper(app, timeout=5, timeout_check_interval=2, watch_interval=1)
    time.sleep(3.5)
    # If the parameters have been loaded correctly, then it should have timed out by now.
    pid = helper.get_helper_PID(app)
    assert pid == None

    # This checks the logs were updated correctly and without repetition.
    logfile = helper.get_helper_log_path(app)
    mod_count = 0
    update_count = 0
    timeout = 0
    with open(logfile, "r") as lf:
        for line in lf:
            if " - INFO - " in line:
                (t, m) = line.split(" - INFO - ")
                logt = datetime.strptime(t[0:22], "%Y-%m-%d %H:%M:%S,%f")
                if logt > tstart:
                    if "Modifying" in m:
                        mod_count = mod_count + 1
                    elif "Updated" in m:
                        update_count = update_count + 1
                    elif "Helper exiting due to timeout" in m:
                        timeout = 1
    assert timeout == 1
    assert update_count == 3
    assert mod_count == 3


def test_modify_helper_cli(app):
    tstart = datetime.now() - timedelta(seconds=0.2)
    r = CliRunner()

    so = cli(
        r, args="helper start --timeout 60 --timeout-check-interval 1 --watch-interval 3"
    )
    assert so == ""
    time.sleep(0.5)
    so = cli(
        args="helper modify --timeout 60 --timeout-check-interval 1 --watch-interval 3"
    )
    assert so == "Helper parameters already met."

    so = cli(
        r, args="helper modify --timeout 60 --timeout-check-interval 2 --watch-interval 1"
    )
    assert so == ""
    time.sleep(1.5)
    so = cli(
        r, args="helper modify --timeout 60 --timeout-check-interval 2 --watch-interval 1"
    )
    assert so == "Helper parameters already met."

    so = cli(
        r, args="helper modify --timeout 5 --timeout-check-interval 2 --watch-interval 1"
    )
    assert so == ""
    time.sleep(2.5)
    so = cli(
        r, args="helper modify --timeout 5 --timeout-check-interval 2 --watch-interval 1"
    )
    assert so == "Helper parameters already met."

    time.sleep(1)
    so = cli(r, args="helper pid")
    assert so == "Helper not running!"

    logfile = cli(r=r, args="helper log-path")
    mod_count = 0
    update_count = 0
    timeout = 0
    with open(logfile, "r") as lf:
        for line in lf:
            if " - INFO - " in line:
                (t, m) = line.split(" - INFO - ")
                logt = datetime.strptime(t[0:22], "%Y-%m-%d %H:%M:%S,%f")
                if logt > tstart:
                    if "Modifying" in m:
                        mod_count = mod_count + 1
                    elif "Updated" in m:
                        update_count = update_count + 1
                    elif "Helper exiting due to timeout" in m:
                        timeout = 1
    assert timeout == 1
    assert update_count == 3
    assert mod_count == 2


def cli(r=CliRunner(), args=""):
    so = r.invoke(hpcflow.CLI, args)
    return so.output.strip()
