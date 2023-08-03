import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import P1_parameter_cls as P1


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


def test_inputs_value_sequence_label_added_to_path():
    seq = hf.ValueSequence(path="inputs.p1.a", values=[0, 1], nesting_order=0, label=0)
    assert seq.path == "inputs.p1[0].a"


def test_inputs_value_sequence_no_label_added_to_path():
    seq = hf.ValueSequence(path="inputs.p1.a", values=[0, 1], nesting_order=0, label="")
    assert seq.path == "inputs.p1.a"


def test_inputs_value_sequence_label_attr_added():
    seq = hf.ValueSequence(path="inputs.p1[1].a", values=[0, 1], nesting_order=0)
    assert seq.label == "1"


def test_inputs_value_sequence_label_path_unmodified():
    path = "inputs.p1[1].a"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path


def test_raise_on_inputs_value_sequence_label_path_unequal():
    with pytest.raises(ValueError):
        hf.ValueSequence(path="inputs.p1[1].a", values=[0, 1], nesting_order=0, label="2")


def test_no_raise_on_inputs_value_sequence_label_path_equal():
    hf.ValueSequence(path="inputs.p1[1].a", values=[0, 1], nesting_order=0, label="1")


def test_no_raise_on_inputs_value_sequence_label_path_cast_equal():
    hf.ValueSequence(path="inputs.p1[1].a", values=[0, 1], nesting_order=0, label=1)


def test_raise_on_resources_value_sequence_with_path_label():
    with pytest.raises(ValueError):
        hf.ValueSequence(path="resources.main[1]", values=[0, 1], nesting_order=0)


def test_raise_on_resources_value_sequence_with_label_arg():
    with pytest.raises(ValueError):
        hf.ValueSequence(path="resources.main", values=[0, 1], nesting_order=0, label=1)


def test_inputs_value_sequence_simple_path_attributes():
    path = "inputs.p1"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path
    assert seq.labelled_type == "p1"
    assert seq.normalised_path == "inputs.p1"
    assert seq.normalised_inputs_path == "p1"
    assert seq.path_type == "inputs"
    assert seq.input_type == "p1"
    assert seq.input_path == ""
    assert seq.resource_scope is None


def test_inputs_value_sequence_path_attributes():
    path = "inputs.p1.a.b"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path
    assert seq.labelled_type == "p1"
    assert seq.normalised_path == "inputs.p1.a.b"
    assert seq.normalised_inputs_path == "p1.a.b"
    assert seq.path_type == "inputs"
    assert seq.input_type == "p1"
    assert seq.input_path == "a.b"
    assert seq.resource_scope is None


def test_inputs_value_sequence_with_path_label_path_attributes():
    path = "inputs.p1[1].a.b"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path
    assert seq.labelled_type == "p1[1]"
    assert seq.normalised_path == "inputs.p1[1].a.b"
    assert seq.normalised_inputs_path == "p1[1].a.b"
    assert seq.path_type == "inputs"
    assert seq.input_type == "p1"
    assert seq.input_path == "a.b"
    assert seq.resource_scope is None


def test_inputs_value_sequence_with_arg_label_path_attributes():
    path = "inputs.p1.a.b"
    new_path = "inputs.p1[1].a.b"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0, label=1)
    assert seq.path == new_path
    assert seq.labelled_type == "p1[1]"
    assert seq.normalised_path == "inputs.p1[1].a.b"
    assert seq.normalised_inputs_path == "p1[1].a.b"
    assert seq.path_type == "inputs"
    assert seq.input_type == "p1"
    assert seq.input_path == "a.b"
    assert seq.resource_scope is None


def test_resources_value_sequence_path_attributes():
    path = "resources.main.num_cores"
    seq = hf.ValueSequence(path=path, values=[0, 1], nesting_order=0)
    assert seq.path == path
    assert seq.labelled_type is None
    assert seq.normalised_path == "resources.main.num_cores"
    assert seq.normalised_inputs_path is None
    assert seq.path_type == "resources"
    assert seq.input_type is None
    assert seq.input_path is None
    assert seq.resource_scope == "main"


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_value_sequence_object_values_during_workflow_init(null_config, tmp_path, store):
    p1 = hf.Parameter("p1")
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=p1)])
    obj = P1(a=101)
    seq = hf.ValueSequence(path="inputs.p1", values=[obj], nesting_order=0)
    values_exp = [P1(a=101, d=None)]

    t1 = hf.Task(
        schemas=[s1],
        sequences=[seq],
    )
    # before workflow initialisation:
    assert seq.values == values_exp

    wk = hf.Workflow.from_template_data(
        tasks=[],
        path=tmp_path,
        template_name="temp",
        store=store,
    )

    with wk.batch_update():
        wk.add_task(t1)
        # after workflow initialisation but before store commit:
        assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp

    # after initialisation and store commit:
    assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_value_sequence_object_values_class_method_during_workflow_init(
    null_config, tmp_path, store
):
    p1 = hf.Parameter("p1")
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=p1)])
    obj = P1.from_data(b=50, c=51)
    seq = hf.ValueSequence(path="inputs.p1", values=[obj], nesting_order=0)
    values_exp = [P1(a=101, d=None)]

    t1 = hf.Task(
        schemas=[s1],
        sequences=[seq],
    )
    # before workflow initialisation:
    assert seq.values == values_exp

    wk = hf.Workflow.from_template_data(
        tasks=[],
        path=tmp_path,
        template_name="temp",
        store=store,
    )

    with wk.batch_update():
        wk.add_task(t1)
        # after workflow initialisation but before store commit:
        assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp

    # after initialisation and store commit:
    assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_value_sequence_object_values_named_class_method_during_workflow_init(
    null_config, tmp_path, store
):
    p1 = hf.Parameter("p1")
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=p1)])
    data = {"b": 50, "c": 51}
    seq = hf.ValueSequence(
        path="inputs.p1", values=[data], nesting_order=0, value_class_method="from_data"
    )
    values_exp = [data]

    t1 = hf.Task(
        schemas=[s1],
        sequences=[seq],
    )
    # before workflow initialisation:
    assert seq.values == values_exp

    wk = hf.Workflow.from_template_data(
        tasks=[],
        path=tmp_path,
        template_name="temp",
        store=store,
    )

    with wk.batch_update():
        wk.add_task(t1)
        # after workflow initialisation but before store commit:
        assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp

    # after initialisation and store commit:
    assert wk.tasks[0].template.element_sets[0].sequences[0].values == values_exp
