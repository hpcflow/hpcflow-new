import pytest
from hpcflow.app import app as hf


def test_init_scope_equivalence_simple():
    rs1 = hf.ResourceSpec(scope=hf.ActionScope.any(), num_cores=1)
    rs2 = hf.ResourceSpec(scope="any", num_cores=1)
    assert rs1 == rs2


def test_init_scope_equivalence_with_kwargs():
    rs1 = hf.ResourceSpec(
        scope=hf.ActionScope.input_file_generator(file="my_file"), num_cores=1
    )
    rs2 = hf.ResourceSpec(scope="input_file_generator[file=my_file]", num_cores=1)
    assert rs1 == rs2


def test_init_no_args():
    rs1 = hf.ResourceSpec()
    rs2 = hf.ResourceSpec(scope="any")
    assert rs1 == rs2


def test_resource_list_raise_on_identical_scopes():
    with pytest.raises(ValueError):
        hf.ResourceList.normalise([{"scope": "any"}, {"scope": "any"}])


def test_merge_template_resources_same_scope():
    res_lst_1 = hf.ResourceList.from_json_like({"any": {"num_cores": 1}})
    res_lst_2 = hf.ResourceList.from_json_like({"any": {}})
    res_lst_2.merge_template_resources(res_lst_1)
    assert res_lst_2 == hf.ResourceList.from_json_like({"any": {"num_cores": 1}})


def test_merge_template_resources_same_scope_no_overwrite():
    res_lst_1 = hf.ResourceList.from_json_like({"any": {"num_cores": 1}})
    res_lst_2 = hf.ResourceList.from_json_like({"any": {"num_cores": 2}})
    res_lst_2.merge_template_resources(res_lst_1)
    assert res_lst_2 == hf.ResourceList.from_json_like({"any": {"num_cores": 2}})


def test_merge_template_resources_multi_scope():
    res_lst_1 = hf.ResourceList.from_json_like({"any": {"num_cores": 1}})
    res_lst_2 = hf.ResourceList.from_json_like({"any": {}, "main": {"num_cores": 3}})
    res_lst_2.merge_template_resources(res_lst_1)
    assert res_lst_2 == hf.ResourceList.from_json_like(
        {"any": {"num_cores": 1}, "main": {"num_cores": 3}}
    )
