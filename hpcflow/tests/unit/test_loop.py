import pytest

from valida.conditions import Value


from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import LoopAlreadyExistsError, LoopTaskSubsetError
from hpcflow.sdk.core.test_utils import P1_parameter_cls, make_workflow


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


def test_get_iteration_task_pathway_nested_loops_multi_iter(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
        ],
        loops=[
            hf.Loop(name="inner_loop", tasks=[2], num_iterations=2),
            hf.Loop(name="outer_loop", tasks=[1, 2], num_iterations=2),
        ],
    )
    assert wk.get_iteration_task_pathway() == [
        (0, {}),
        (1, {"outer_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 1}),
        (1, {"outer_loop": 1}),
        (2, {"outer_loop": 1, "inner_loop": 0}),
        (2, {"outer_loop": 1, "inner_loop": 1}),
    ]


@pytest.mark.skip(
    reason="second set of asserts fail; need to re-source inputs on adding iterations."
)
def test_get_iteration_task_pathway_nested_loops_multi_iter_jagged(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
        ],
        loops=[
            hf.Loop(name="inner_loop", tasks=[2], num_iterations=2),
            hf.Loop(name="outer_loop", tasks=[1, 2], num_iterations=2),
        ],
    )
    wk.loops.inner_loop.add_iteration(parent_loop_indices={"outer_loop": 1})
    wk.loops.inner_loop.add_iteration(parent_loop_indices={"outer_loop": 1})
    assert wk.get_iteration_task_pathway() == [
        (0, {}),
        (1, {"outer_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 1}),
        (1, {"outer_loop": 1}),
        (2, {"outer_loop": 1, "inner_loop": 0}),
        (2, {"outer_loop": 1, "inner_loop": 1}),
        (2, {"outer_loop": 1, "inner_loop": 2}),
        (2, {"outer_loop": 1, "inner_loop": 3}),
        (3, {}),
    ]
    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)
    assert pathway[1][2][0]["inputs.p1"] == pathway[0][2][0]["outputs.p1"]
    assert pathway[2][2][0]["inputs.p1"] == pathway[1][2][0]["outputs.p1"]
    assert pathway[3][2][0]["inputs.p1"] == pathway[2][2][0]["outputs.p1"]
    assert pathway[4][2][0]["inputs.p1"] == pathway[3][2][0]["outputs.p1"]
    assert pathway[5][2][0]["inputs.p1"] == pathway[4][2][0]["outputs.p1"]
    assert pathway[6][2][0]["inputs.p1"] == pathway[5][2][0]["outputs.p1"]
    assert pathway[7][2][0]["inputs.p1"] == pathway[6][2][0]["outputs.p1"]
    assert pathway[8][2][0]["inputs.p1"] == pathway[7][2][0]["outputs.p1"]

    # FAILS currently:
    assert pathway[9][2][0]["inputs.p1"] == pathway[8][2][0]["outputs.p1"]


def test_get_iteration_task_pathway_nested_loops_multi_iter_add_outer_iter(
    null_config, tmp_path
):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
        ],
        loops=[
            hf.Loop(name="inner_loop", tasks=[2], num_iterations=2),
            hf.Loop(name="outer_loop", tasks=[1, 2], num_iterations=2),
        ],
    )
    wk.loops.outer_loop.add_iteration()
    assert wk.get_iteration_task_pathway() == [
        (0, {}),
        (1, {"outer_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 1}),
        (1, {"outer_loop": 1}),
        (2, {"outer_loop": 1, "inner_loop": 0}),
        (2, {"outer_loop": 1, "inner_loop": 1}),
        (1, {"outer_loop": 2}),
        (2, {"outer_loop": 2, "inner_loop": 0}),
        (2, {"outer_loop": 2, "inner_loop": 1}),
    ]


@pytest.mark.skip(
    reason="second set of asserts fail; need to re-source inputs on adding iterations."
)
def test_get_iteration_task_pathway_unconnected_loops(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
        ],
        loops=[
            hf.Loop(name="loop_A", tasks=[0, 1], num_iterations=2),
            hf.Loop(name="loop_B", tasks=[2, 3], num_iterations=2),
        ],
    )
    assert wk.get_iteration_task_pathway() == [
        (0, {"loop_A": 0}),
        (1, {"loop_A": 0}),
        (0, {"loop_A": 1}),
        (1, {"loop_A": 1}),
        (2, {"loop_B": 0}),
        (3, {"loop_B": 0}),
        (2, {"loop_B": 1}),
        (3, {"loop_B": 1}),
    ]

    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)
    assert pathway[1][2][0]["inputs.p1"] == pathway[0][2][0]["outputs.p1"]
    assert pathway[2][2][0]["inputs.p1"] == pathway[1][2][0]["outputs.p1"]
    assert pathway[3][2][0]["inputs.p1"] == pathway[2][2][0]["outputs.p1"]
    assert pathway[5][2][0]["inputs.p1"] == pathway[4][2][0]["outputs.p1"]
    assert pathway[6][2][0]["inputs.p1"] == pathway[5][2][0]["outputs.p1"]
    assert pathway[7][2][0]["inputs.p1"] == pathway[6][2][0]["outputs.p1"]

    # FAILS currently:
    assert pathway[4][2][0]["inputs.p1"] == pathway[3][2][0]["outputs.p1"]


def test_wk_loop_input_sources_including_non_iteration_task_source(null_config, tmp_path):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + 100))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2"), hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p2>> + <<parameter:p3>>))",
                        stdout="<<int(parameter:p4)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    ts3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput("p3"), hf.SchemaInput("p4")],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p3>> + <<parameter:p4>>))",
                        stdout="<<int(parameter:p3)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts2, inputs={"p3": 301}),
            hf.Task(schema=ts3),
        ],
    )
    wk.add_loop(hf.Loop(tasks=[1, 2], num_iterations=2))
    t1 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t2_iter_0 = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t3_iter_0 = wk.tasks.t3.elements[0].iterations[0].get_data_idx()
    t2_iter_1 = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t3_iter_1 = wk.tasks.t3.elements[0].iterations[1].get_data_idx()

    assert t2_iter_0["inputs.p2"] == t2_iter_1["inputs.p2"] == t1["outputs.p2"]
    assert t3_iter_0["inputs.p3"] == t2_iter_0["inputs.p3"]
    assert t3_iter_0["inputs.p4"] == t2_iter_0["outputs.p4"]
    assert t3_iter_1["inputs.p3"] == t2_iter_1["inputs.p3"]
    assert t3_iter_1["inputs.p4"] == t2_iter_1["outputs.p4"]
    assert t2_iter_1["inputs.p3"] == t3_iter_0["outputs.p3"]


def test_wk_loop_input_sources_default(null_config, tmp_path):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2", default_value=2)],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + <<parameter:p2>>))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
    )
    wk.add_loop(hf.Loop(tasks=[0], num_iterations=2))
    t1_iter_0 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1].get_data_idx()

    assert t1_iter_0["inputs.p2"] == t1_iter_1["inputs.p2"]


def test_wk_loop_input_sources_iterable_param_default(null_config, tmp_path):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1", default_value=1)],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + 10))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
    )
    wk.add_loop(hf.Loop(tasks=[0], num_iterations=3))
    # first iteration should be the default value, second and third iterations should
    # be from previous iteration outputs:
    t1_iter_0 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t1_iter_2 = wk.tasks.t1.elements[0].iterations[2].get_data_idx()

    assert t1_iter_0["inputs.p1"] != t1_iter_1["inputs.p1"]
    assert t1_iter_1["inputs.p1"] != t1_iter_2["inputs.p1"]
    assert t1_iter_1["inputs.p1"] == t1_iter_0["outputs.p1"]
    assert t1_iter_2["inputs.p1"] == t1_iter_1["outputs.p1"]


def test_wk_loop_input_sources_iterable_param_default_conditional_action(
    null_config, tmp_path
):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput("p1", default_value=1),
            hf.SchemaInput("p2", default_value=None),
        ],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + 10))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
                environments=[act_env],
            ),
            hf.Action(
                commands=[hf.Command("Write-Output ((<<parameter:p2>> + 10))")],
                environments=[act_env],
                rules=[
                    hf.ActionRule(path="inputs.p2", condition=Value.not_equal_to(None))
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
    )
    wk.add_loop(hf.Loop(tasks=[0], num_iterations=3))
    # first iteration should be the default value, second and third iterations should
    # be from previous iteration outputs:
    t1_iter_0 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t1_iter_2 = wk.tasks.t1.elements[0].iterations[2].get_data_idx()

    assert t1_iter_0["inputs.p1"] != t1_iter_1["inputs.p1"]
    assert t1_iter_1["inputs.p1"] != t1_iter_2["inputs.p1"]
    assert t1_iter_1["inputs.p1"] == t1_iter_0["outputs.p1"]
    assert t1_iter_2["inputs.p1"] == t1_iter_1["outputs.p1"]


def test_wk_loop_input_sources_including_non_iteration_task_source_with_groups(
    null_config, tmp_path
):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + 100))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2"), hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p2>> + <<parameter:p3>>))",
                        stdout="<<int(parameter:p4)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    ts3 = hf.TaskSchema(
        objective="t3",
        inputs=[
            hf.SchemaInput("p3", labels={"": {"group": "my_group"}}),
            hf.SchemaInput("p4", labels={"": {"group": "my_group"}}),
        ],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<sum(parameter:p3)>> + <<sum(parameter:p4)>>))",
                        stdout="<<int(parameter:p3)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(
                schema=ts2,
                sequences=[hf.ValueSequence(path="inputs.p3", values=[301, 302])],
                groups=[hf.ElementGroup(name="my_group")],
            ),
            hf.Task(schema=ts3),
        ],
    )
    wk.add_loop(hf.Loop(tasks=[1, 2], num_iterations=2))

    t2_elem_0_iter_0 = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_elem_1_iter_0 = wk.tasks.t2.elements[1].iterations[0].get_data_idx()
    t2_elem_0_iter_1 = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t2_elem_1_iter_1 = wk.tasks.t2.elements[1].iterations[1].get_data_idx()

    t3_iter_0 = wk.tasks.t3.elements[0].iterations[0].get_data_idx()
    t3_iter_1 = wk.tasks.t3.elements[0].iterations[1].get_data_idx()
    assert len(t3_iter_0["inputs.p3"]) == len(t3_iter_1["inputs.p3"]) == 2
    assert len(t3_iter_0["inputs.p4"]) == len(t3_iter_1["inputs.p4"]) == 2
    assert t3_iter_0["inputs.p3"] == [
        t2_elem_0_iter_0["inputs.p3"],
        t2_elem_1_iter_0["inputs.p3"],
    ]
    assert t3_iter_0["inputs.p4"] == [
        t2_elem_0_iter_0["outputs.p4"],
        t2_elem_1_iter_0["outputs.p4"],
    ]
    assert t3_iter_1["inputs.p3"] == [
        t2_elem_0_iter_1["inputs.p3"],
        t2_elem_1_iter_1["inputs.p3"],
    ]
    assert t3_iter_1["inputs.p4"] == [
        t2_elem_0_iter_1["outputs.p4"],
        t2_elem_1_iter_1["outputs.p4"],
    ]


def test_loop_local_sub_parameters(null_config, tmp_path):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1c")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1c.a>> + 100))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p1c")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p2>> + 100))",
                        stdout="<<parameter:p1c>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(
                schema=ts1,
                inputs=[
                    hf.InputValue(parameter="p1c", value=P1_parameter_cls(a=101)),
                    hf.InputValue(parameter="p1c", path="d", value=9),
                ],
            ),
            hf.Task(schema=ts2),
        ],
    )
    wk.add_loop(hf.Loop(tasks=[0, 1], num_iterations=2))

    t1_iter_0 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t2_iter_0 = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t2_iter_1 = wk.tasks.t2.elements[0].iterations[1].get_data_idx()

    assert t2_iter_0["inputs.p2"] == t1_iter_0["outputs.p2"]
    assert t1_iter_1["inputs.p1c"] == t2_iter_0["outputs.p1c"]
    assert t2_iter_1["inputs.p2"] == t1_iter_1["outputs.p2"]
    assert t1_iter_0["inputs.p1c.d"] == t1_iter_1["inputs.p1c.d"]


def test_nested_loop_iter_loop_idx(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
        loops=[
            hf.Loop(name="outer_loop", tasks=[0], num_iterations=1),
            hf.Loop(name="inner_loop", tasks=[0], num_iterations=1),
        ],
    )
    assert wk.tasks[0].elements[0].iterations[0].loop_idx == {
        "inner_loop": 0,
        "outer_loop": 0,
    }


def test_schema_input_with_group_sourced_from_prev_iteration(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<parameter:p1>> + 1 ))", stdout="<<parameter:p2>>"
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2", group="my_group")],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<parameter:p2>> + 2 ))", stdout="<<parameter:p3>>"
                    )
                ]
            )
        ],
    )
    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<parameter:p3>> + 3 ))", stdout="<<parameter:p2>>"
                    )
                ]
            )
        ],
    )

    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2, 3])],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(schema=s2)
    t3 = hf.Task(
        schema=s3,
        repeats=3,
        groups=[hf.ElementGroup(name="my_group")],
    )

    l1 = hf.Loop(name="my_loop", tasks=[1, 2], num_iterations=2)

    wk = hf.Workflow.from_template_data(
        template_name="test_loops",
        path=tmp_path,
        tasks=[t1, t2, t3],
        loops=[l1],
    )

    assert wk.tasks.t2.elements[0].iterations[0].get_data_idx()["inputs.p2"] == [
        i.get_data_idx()["outputs.p2"] for i in wk.tasks.t1.elements
    ]
    assert [
        i.iterations[0].get_data_idx()["inputs.p3"] for i in wk.tasks.t3.elements
    ] == [wk.tasks.t2.elements[0].iterations[0].get_data_idx()["outputs.p3"]] * 3
    assert wk.tasks.t2.elements[0].iterations[1].get_data_idx()["inputs.p2"] == [
        i.iterations[0].get_data_idx()["outputs.p2"] for i in wk.tasks.t3.elements
    ]
    assert [
        i.iterations[1].get_data_idx()["inputs.p3"] for i in wk.tasks.t3.elements
    ] == [wk.tasks.t2.elements[0].iterations[1].get_data_idx()["outputs.p3"]] * 3


def test_loop_downstream_tasks(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p2>> + 100)",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
            hf.Task(schema=ts2, inputs={"p2": 201}),
        ],
        loops=[
            hf.Loop(name="my_loop", tasks=[1, 2], num_iterations=2),
        ],
    )
    assert wk.loops.my_loop.downstream_tasks == [wk.tasks[3]]
    assert wk.loops.my_loop.upstream_tasks == [wk.tasks[0]]


def test_raise_loop_task_subset_error(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    with pytest.raises(LoopTaskSubsetError):
        hf.Workflow.from_template_data(
            template_name="test_loop",
            path=tmp_path,
            tasks=[
                hf.Task(schema=ts1, inputs={"p1": 101}),
                hf.Task(schema=ts1),
                hf.Task(schema=ts1),
            ],
            loops=[
                hf.Loop(name="my_loop", tasks=[2, 1], num_iterations=2),
            ],
        )


def test_raise_downstream_task_with_iterable_parameter(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    with pytest.raises(NotImplementedError):
        hf.Workflow.from_template_data(
            template_name="test_loop",
            path=tmp_path,
            tasks=[
                hf.Task(schema=ts1, inputs={"p1": 101}),
                hf.Task(schema=ts1),
                hf.Task(schema=ts1),
            ],
            loops=[
                hf.Loop(name="my_loop", tasks=[1], num_iterations=2),
            ],
        )


def test_adjacent_loops_iteration_pathway(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p2>> + 100)",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts2, inputs={"p2": 201}),
        ],
        loops=[
            hf.Loop(name="loop_A", tasks=[0, 1], num_iterations=2),
            hf.Loop(name="loop_B", tasks=[2], num_iterations=2),
        ],
    )
    assert wk.get_iteration_task_pathway() == [
        (0, {"loop_A": 0}),
        (1, {"loop_A": 0}),
        (0, {"loop_A": 1}),
        (1, {"loop_A": 1}),
        (2, {"loop_B": 0}),
        (2, {"loop_B": 1}),
    ]


def test_get_child_loops_ordered_by_depth(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
        ],
        loops=[
            hf.Loop(name="inner", tasks=[0], num_iterations=1),
            hf.Loop(name="middle", tasks=[0], num_iterations=1),
            hf.Loop(name="outer", tasks=[0], num_iterations=1),
        ],
    )
    assert wk.loops.inner.get_child_loops() == []
    assert wk.loops.middle.get_child_loops() == [wk.loops.inner]
    assert wk.loops.outer.get_child_loops() == [wk.loops.middle, wk.loops.inner]


def test_multi_nested_loops(null_config, tmp_path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
        loops=[
            hf.Loop(name="inner", tasks=[0], num_iterations=2),
            hf.Loop(name="middle_1", tasks=[0], num_iterations=3),
            hf.Loop(name="middle_2", tasks=[0], num_iterations=2),
            hf.Loop(name="outer", tasks=[0], num_iterations=2),
        ],
    )
    pathway = wk.get_iteration_task_pathway(ret_iter_IDs=True)
    assert len(pathway) == 2 * 3 * 2 * 2
    assert wk.get_iteration_task_pathway(ret_iter_IDs=True) == [
        (0, {"inner": 0, "middle_1": 0, "middle_2": 0, "outer": 0}, (0,)),
        (0, {"inner": 1, "middle_1": 0, "middle_2": 0, "outer": 0}, (1,)),
        (0, {"inner": 0, "middle_1": 1, "middle_2": 0, "outer": 0}, (2,)),
        (0, {"inner": 1, "middle_1": 1, "middle_2": 0, "outer": 0}, (3,)),
        (0, {"inner": 0, "middle_1": 2, "middle_2": 0, "outer": 0}, (4,)),
        (0, {"inner": 1, "middle_1": 2, "middle_2": 0, "outer": 0}, (5,)),
        (0, {"inner": 0, "middle_1": 0, "middle_2": 1, "outer": 0}, (6,)),
        (0, {"inner": 1, "middle_1": 0, "middle_2": 1, "outer": 0}, (7,)),
        (0, {"inner": 0, "middle_1": 1, "middle_2": 1, "outer": 0}, (8,)),
        (0, {"inner": 1, "middle_1": 1, "middle_2": 1, "outer": 0}, (9,)),
        (0, {"inner": 0, "middle_1": 2, "middle_2": 1, "outer": 0}, (10,)),
        (0, {"inner": 1, "middle_1": 2, "middle_2": 1, "outer": 0}, (11,)),
        (0, {"inner": 0, "middle_1": 0, "middle_2": 0, "outer": 1}, (12,)),
        (0, {"inner": 1, "middle_1": 0, "middle_2": 0, "outer": 1}, (13,)),
        (0, {"inner": 0, "middle_1": 1, "middle_2": 0, "outer": 1}, (14,)),
        (0, {"inner": 1, "middle_1": 1, "middle_2": 0, "outer": 1}, (15,)),
        (0, {"inner": 0, "middle_1": 2, "middle_2": 0, "outer": 1}, (16,)),
        (0, {"inner": 1, "middle_1": 2, "middle_2": 0, "outer": 1}, (17,)),
        (0, {"inner": 0, "middle_1": 0, "middle_2": 1, "outer": 1}, (18,)),
        (0, {"inner": 1, "middle_1": 0, "middle_2": 1, "outer": 1}, (19,)),
        (0, {"inner": 0, "middle_1": 1, "middle_2": 1, "outer": 1}, (20,)),
        (0, {"inner": 1, "middle_1": 1, "middle_2": 1, "outer": 1}, (21,)),
        (0, {"inner": 0, "middle_1": 2, "middle_2": 1, "outer": 1}, (22,)),
        (0, {"inner": 1, "middle_1": 2, "middle_2": 1, "outer": 1}, (23,)),
    ]
