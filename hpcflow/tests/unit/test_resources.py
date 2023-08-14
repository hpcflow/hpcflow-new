import pytest
from hpcflow.app import app as hf


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


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


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_use_persistent_resource_spec(null_config, tmp_path, store):
    # create a workflow from which we can use a resource spec in a new workflow:
    num_cores_check = 2
    wk_base = hf.Workflow.from_template_data(
        template_name="wk_base",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schemas=[hf.task_schemas.test_t1_ps],
                inputs=[hf.InputValue("p1", 101)],
                resources={"any": {"num_cores": num_cores_check}},
            )
        ],
    )
    resource_spec = wk_base.tasks[0].template.element_sets[0].resources[0]

    wk = hf.Workflow.from_template_data(
        template_name="wk",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schemas=[hf.task_schemas.test_t1_ps],
                inputs=[hf.InputValue("p1", 101)],
            ),
        ],
        resources=[resource_spec],
    )

    assert wk.tasks[0].template.element_sets[0].resources[0].num_cores == num_cores_check


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_use_persistent_resource_list(null_config, tmp_path, store):
    # create a workflow from which we can use the resource list in a new workflow:
    num_cores_check = 2
    wk_base = hf.Workflow.from_template_data(
        template_name="wk_base",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schemas=[hf.task_schemas.test_t1_ps],
                inputs=[hf.InputValue("p1", 101)],
                resources={"any": {"num_cores": num_cores_check}},
            )
        ],
    )
    resource_list = wk_base.tasks[0].template.element_sets[0].resources

    wk = hf.Workflow.from_template_data(
        template_name="wk",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schemas=[hf.task_schemas.test_t1_ps],
                inputs=[hf.InputValue("p1", 101)],
            ),
        ],
        resources=resource_list[:],  # must pass a list!
    )

    assert wk.tasks[0].template.element_sets[0].resources[0].num_cores == num_cores_check
