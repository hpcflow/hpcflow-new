import pytest

from click.testing import CliRunner

from hpcflow import __version__
from hpcflow.api import hpcflow


def test_version():
    runner = CliRunner()
    result = runner.invoke(hpcflow.CLI, args="--version")
    assert result.output.strip() == f"hpcflow, version {__version__}"
