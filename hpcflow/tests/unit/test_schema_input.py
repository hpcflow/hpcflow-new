import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.parameters import NullDefault


def test_null_default_value():
    p1 = hf.Parameter("p1")
    p1_inp = hf.SchemaInput(parameter=p1)
    assert "default_value" not in p1_inp.labels[""]


def test_none_default_value():
    """A `None` default value is set with a value of `None`"""
    p1 = hf.Parameter("p1")
    p1_inp = hf.SchemaInput(parameter=p1, default_value=None)
    def_val_exp = hf.InputValue(parameter=p1, label="", value=None)
    def_val_exp._schema_input = p1_inp
    assert p1_inp.labels[""]["default_value"] == def_val_exp


def test_from_json_like_labels_and_default():
    json_like = {
        "parameter": "p1",
        "labels": {"0": {}},
        "default_value": None,
    }
    inp = hf.SchemaInput.from_json_like(
        json_like=json_like,
        shared_data=hf.template_components,
    )
    assert inp.labels["0"]["default_value"].value == None
