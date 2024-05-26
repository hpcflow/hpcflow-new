import pytest

from click.testing import CliRunner
import click.exceptions

from hpcflow import __version__
from hpcflow.app import app as hf
from hpcflow.sdk.cli_common import BoolOrString


def test_version():
    runner = CliRunner()
    result = runner.invoke(hf.cli, args="--version")
    assert result.output.strip() == f"hpcFlow, version {__version__}"


def test_BoolOrString_convert():
    param_type = BoolOrString(["a"])
    assert param_type.convert(True, None, None) == True
    assert param_type.convert(False, None, None) == False
    assert param_type.convert("yes", None, None) == True
    assert param_type.convert("no", None, None) == False
    assert param_type.convert("on", None, None) == True
    assert param_type.convert("off", None, None) == False
    assert param_type.convert("a", None, None) == "a"
    with pytest.raises(click.exceptions.BadParameter):
        param_type.convert("b", None, None)
