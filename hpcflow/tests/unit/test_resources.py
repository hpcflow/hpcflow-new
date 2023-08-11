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
