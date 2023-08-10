from hpcflow.app import app as hf


def test_in_pytest():
    assert hf.run_time_info.in_pytest
