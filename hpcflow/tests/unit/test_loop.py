import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import LoopAlreadyExistsError
from hpcflow.sdk.core.test_utils import make_workflow


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_loop_tasks_obj_insert_ID_equivalence(tmp_path, store):
    wk_1 = make_workflow(
        schemas_spec=[[{"p1": None}, ("p1",), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    lp_0 = hf.Loop(tasks=[wk_1.tasks.t1], num_iterations=2)
    lp_1 = hf.Loop(tasks=[0], num_iterations=2)
    assert lp_0.task_insert_IDs == lp_1.task_insert_IDs


def test_raise_on_add_loop_same_name(tmp_path):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, ("p1",), "t1"], [{"p2": None}, ("p2",), "t2"]],
        local_inputs={0: ("p1",), 1: ("p2",)},
        path=tmp_path,
        store="json",
    )
    lp_0 = hf.Loop(name="my_loop", tasks=[0], num_iterations=2)
    lp_1 = hf.Loop(name="my_loop", tasks=[1], num_iterations=2)

    wk.add_loop(lp_0)
    with pytest.raises(LoopAlreadyExistsError):
        wk.add_loop(lp_1)


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_data_idx_single_task_single_element_single_parameter_three_iters(
    tmp_path, store
):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, ("p1",), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[wk.tasks.t1], num_iterations=3))
    iter_0, iter_1, iter_2 = wk.tasks.t1.elements[0].iterations

    p1_idx_i0_out = iter_0.get_data_idx()["outputs.p1"]
    p1_idx_i1_in = iter_1.get_data_idx()["inputs.p1"]
    p1_idx_i1_out = iter_1.get_data_idx()["outputs.p1"]
    p1_idx_i2_in = iter_2.get_data_idx()["inputs.p1"]

    assert p1_idx_i0_out == p1_idx_i1_in and p1_idx_i1_out == p1_idx_i2_in


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_EARs_initialised_single_task_single_element_single_parameter_three_iters(
    tmp_path, store
):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, ("p1",), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[wk.tasks.t1], num_iterations=3))
    iter_0, iter_1, iter_2 = wk.tasks.t1.elements[0].iterations
    assert iter_0.EARs_initialised and iter_1.EARs_initialised and iter_2.EARs_initialised


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_data_idx_single_task_multi_element_single_parameter_three_iters(
    tmp_path, store
):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, ("p1",), "t1"]],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[wk.tasks.t1], num_iterations=3))
    e0_iter_0, e0_iter_1, e0_iter_2 = wk.tasks.t1.elements[0].iterations
    e1_iter_0, e1_iter_1, e1_iter_2 = wk.tasks.t1.elements[1].iterations

    e0_p1_idx_i0_out = e0_iter_0.get_data_idx()["outputs.p1"]
    e0_p1_idx_i1_in = e0_iter_1.get_data_idx()["inputs.p1"]
    e0_p1_idx_i1_out = e0_iter_1.get_data_idx()["outputs.p1"]
    e0_p1_idx_i2_in = e0_iter_2.get_data_idx()["inputs.p1"]

    e1_p1_idx_i0_out = e1_iter_0.get_data_idx()["outputs.p1"]
    e1_p1_idx_i1_in = e1_iter_1.get_data_idx()["inputs.p1"]
    e1_p1_idx_i1_out = e1_iter_1.get_data_idx()["outputs.p1"]
    e1_p1_idx_i2_in = e1_iter_2.get_data_idx()["inputs.p1"]

    assert (
        e0_p1_idx_i0_out == e0_p1_idx_i1_in
        and e0_p1_idx_i1_out == e0_p1_idx_i2_in
        and e1_p1_idx_i0_out == e1_p1_idx_i1_in
        and e1_p1_idx_i1_out == e1_p1_idx_i2_in
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_data_idx_multi_task_single_element_single_parameter_two_iters(
    tmp_path, store
):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p1",), "t1"],
            [{"p1": None}, ("p1",), "t2"],
            [{"p1": None}, ("p1",), "t3"],
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[0, 1, 2], num_iterations=2))
    t1_iter_0, t1_iter_1 = wk.tasks.t1.elements[0].iterations
    t2_iter_0, t2_iter_1 = wk.tasks.t2.elements[0].iterations
    t3_iter_0, t3_iter_1 = wk.tasks.t3.elements[0].iterations

    in_key = "inputs.p1"
    out_key = "outputs.p1"

    t1_i0_p1_idx_out = t1_iter_0.get_data_idx()[out_key]
    t2_i0_p1_idx_in = t2_iter_0.get_data_idx()[in_key]
    t2_i0_p1_idx_out = t2_iter_0.get_data_idx()[out_key]
    t3_i0_p1_idx_in = t3_iter_0.get_data_idx()[in_key]
    t3_i0_p1_idx_out = t3_iter_0.get_data_idx()[out_key]

    t1_i1_p1_idx_in = t1_iter_1.get_data_idx()[in_key]
    t1_i1_p1_idx_out = t1_iter_1.get_data_idx()[out_key]
    t2_i1_p1_idx_in = t2_iter_1.get_data_idx()[in_key]
    t2_i1_p1_idx_out = t2_iter_1.get_data_idx()[out_key]
    t3_i1_p1_idx_in = t3_iter_1.get_data_idx()[in_key]

    assert (
        t1_i0_p1_idx_out == t2_i0_p1_idx_in
        and t2_i0_p1_idx_out == t3_i0_p1_idx_in
        and t3_i0_p1_idx_out == t1_i1_p1_idx_in
        and t1_i1_p1_idx_out == t2_i1_p1_idx_in
        and t2_i1_p1_idx_out == t3_i1_p1_idx_in
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_data_idx_single_task_single_element_single_parameter_three_iters_non_iterable_param(
    tmp_path, store
):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, ("p1",), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(
        hf.Loop(tasks=[wk.tasks.t1], num_iterations=3, non_iterable_parameters=["p1"])
    )
    iter_0, iter_1, iter_2 = wk.tasks.t1.elements[0].iterations

    p1_idx_i0_out = iter_0.get_data_idx()["outputs.p1"]
    p1_idx_i1_in = iter_1.get_data_idx()["inputs.p1"]
    p1_idx_i1_out = iter_1.get_data_idx()["outputs.p1"]
    p1_idx_i2_in = iter_2.get_data_idx()["inputs.p1"]

    assert p1_idx_i0_out != p1_idx_i1_in and p1_idx_i1_out != p1_idx_i2_in


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_iterable_parameters(tmp_path, store):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None, "p2": None}, ("p1", "p2"), "t1"],
            [{"p1": None}, ("p1",), "t2"],
            [{"p1": None, "p2": None}, ("p1", "p2"), "t3"],
        ],
        local_inputs={0: ("p1", "p2"), 1: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[0, 1, 2], num_iterations=2))
    assert dict(sorted(wk.loops[0].iterable_parameters.items(), key=lambda x: x[0])) == {
        "p1": {"input_task": 0, "output_tasks": [0, 1, 2]},
        "p2": {"input_task": 0, "output_tasks": [0, 2]},
    }


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_input_sources_including_local_single_element_two_iters(tmp_path, store):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None, "p2": None}, ("p1", "p2"), "t1"],
            [{"p1": None}, ("p1",), "t2"],
            [{"p1": None, "p2": None}, ("p1", "p2"), "t3"],
        ],
        local_inputs={0: ("p1", "p2"), 1: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[0, 1, 2], num_iterations=2))

    t2_iter_0 = wk.tasks.t2.elements[0].iterations[0]
    t3_iter_0 = wk.tasks.t3.elements[0].iterations[0]
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1]
    t2_iter_1 = wk.tasks.t2.elements[0].iterations[1]

    t3_p1_i0_out = t3_iter_0.get_data_idx()["outputs.p1"]
    t3_p2_i0_out = t3_iter_0.get_data_idx()["outputs.p2"]

    t1_p1_i1_in = t1_iter_1.get_data_idx()["inputs.p1"]
    t1_p2_i1_in = t1_iter_1.get_data_idx()["inputs.p2"]

    # local input defined in task 2 is not an input task of the iterative parameter p1,
    # so it is sourced in all iterations from the original local input:
    t2_p1_i0_in = t2_iter_0.get_data_idx()["inputs.p1"]
    t2_p1_i1_in = t2_iter_1.get_data_idx()["inputs.p1"]

    assert (
        t3_p1_i0_out == t1_p1_i1_in
        and t3_p2_i0_out == t1_p2_i1_in
        and t2_p1_i0_in == t2_p1_i1_in
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_iteration_task_pathway_single_task_single_element_three_iters(
    tmp_path, store
):
    wk = make_workflow(
        schemas_spec=[[{"p1": None}, ("p1",), "t1"]],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(name="loop_0", tasks=[wk.tasks.t1], num_iterations=3))

    assert wk.get_iteration_task_pathway() == [
        (0, {"loop_0": 0}),
        (0, {"loop_0": 1}),
        (0, {"loop_0": 2}),
    ]
