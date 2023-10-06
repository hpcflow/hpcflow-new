from hpcflow.app import app as hf


def test_equivalent_init_with_rule_args():
    rule_args = {
        "path": "resources.os_name",
        "condition": {"value.equal_to": "posix"},
    }
    assert hf.ActionRule(rule=hf.Rule(**rule_args)) == hf.ActionRule(**rule_args)


def test_equivalent_init_json_like_with_rule_args():
    rule_args = {
        "path": "resources.os_name",
        "condition": {"value.equal_to": "posix"},
    }
    assert hf.ActionRule.from_json_like(
        {"rule": rule_args}
    ) == hf.ActionRule.from_json_like(rule_args)
