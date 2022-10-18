# import pytest

# from hpcflow.errors import (
#     MissingInputs,
#     TaskTemplateInvalidNesting,
#     TaskTemplateUnexpectedInput,
#     TaskTemplateMultipleInputValues,
#     TaskTemplateMultipleSchemaObjectives,
# )
# from hpcflow.parameters import InputValue, Parameter
# from hpcflow.task import TaskTemplate
# from hpcflow.task_schema import TaskObjective, TaskSchema


# def test_raise_on_multiple_schema_objectives():

#     generate_RVE_schema = TaskSchema(objective=TaskObjective("generate_volume_element"))
#     simulate_schema = TaskSchema(objective=TaskObjective("simulate"))
#     with pytest.raises(TaskTemplateMultipleSchemaObjectives):
#         TaskTemplate(schema=[generate_RVE_schema, simulate_schema])


# def test_raise_on_unexpected_inputs():

#     grid_size = Parameter("grid_size")
#     unknown_input = Parameter("unknown_input")

#     generate_RVE_schema = TaskSchema(
#         objective=TaskObjective("generate_volume_element"), inputs=[grid_size]
#     )

#     with pytest.raises(TaskTemplateUnexpectedInput):
#         TaskTemplate(
#             schema=generate_RVE_schema,
#             input_values=[
#                 InputValue(grid_size, value=[8, 8, 8]),
#                 InputValue(unknown_input, value=4),
#             ],
#         )


# def test_raise_on_multiple_input_values():

#     grid_size = Parameter("grid_size")

#     generate_RVE_schema = TaskSchema(
#         objective=TaskObjective("generate_volume_element"), inputs=[grid_size]
#     )

#     with pytest.raises(TaskTemplateMultipleInputValues):
#         TaskTemplate(
#             schema=generate_RVE_schema,
#             input_values=[
#                 InputValue(grid_size, value=[8, 8, 8]),
#                 InputValue(grid_size, value=7),
#             ],
#         )


# def test_expected_return_defined_and_undefined_input_types():

#     generate_RVE = TaskObjective("generate_volume_element")
#     grid_size = Parameter("grid_size")
#     size = Parameter("size")
#     generate_RVE_schema = TaskSchema(objective=generate_RVE, inputs=[grid_size, size])

#     task = TaskTemplate(
#         schema=generate_RVE_schema, input_values=[InputValue(grid_size, value=[8, 8, 8])]
#     )

#     assert task.defined_input_types == {"grid_size"} and task.undefined_input_types == {
#         "size"
#     }


# def test_expected_return_all_schema_input_types_single_schema():

#     generate_RVE = TaskObjective("generate_volume_element")
#     grid_size = Parameter("grid_size")
#     size = Parameter("size")
#     generate_RVE_schema = TaskSchema(objective=generate_RVE, inputs=[grid_size, size])

#     task = TaskTemplate(schema=generate_RVE_schema)
#     assert task.all_schema_input_types == {"grid_size", "size"}


# def test_expected_return_all_schema_input_types_multiple_schemas():

#     generate_RVE = TaskObjective("generate_volume_element")
#     grid_size = Parameter("grid_size")
#     size = Parameter("size")
#     buffer_layer = Parameter("buffer_layer")

#     generate_RVE_schema_1 = TaskSchema(objective=generate_RVE, inputs=[grid_size, size])
#     generate_RVE_schema_2 = TaskSchema(
#         objective=generate_RVE, inputs=[grid_size, buffer_layer]
#     )

#     task = TaskTemplate(schema=[generate_RVE_schema_1, generate_RVE_schema_2])
#     assert task.all_schema_input_types == {"grid_size", "size", "buffer_layer"}


# def test_expected_unique_name_single_schema():
#     schema = TaskSchema(objective=TaskObjective("simulate"))
#     task = TaskTemplate(schema)
#     assert task.unique_name == "simulate"


# def test_expected_unique_name_single_schema_with_method():
#     schema = TaskSchema(objective=TaskObjective("simulate"), method="method1")
#     task = TaskTemplate(schema)
#     assert task.unique_name == "simulate_method1"


# def test_expected_unique_name_single_schema_with_implementation():
#     schema = TaskSchema(objective=TaskObjective("simulate"), implementation="i1")
#     task = TaskTemplate(schema)
#     assert task.unique_name == "simulate_i1"


# def test_expected_unique_name_single_schema_with_method_and_implementation():
#     schema = TaskSchema(
#         objective=TaskObjective("simulate"), method="method1", implementation="i1"
#     )
#     task = TaskTemplate(schema)
#     assert task.unique_name == "simulate_method1_i1"


# def test_expected_unique_name_multiple_schemas():
#     objective = TaskObjective("simulate")
#     schema_1 = TaskSchema(objective)
#     schema_2 = TaskSchema(objective)
#     task = TaskTemplate([schema_1, schema_2])
#     assert task.unique_name == "simulate"


# def test_expected_unique_name_two_schemas_first_with_method():
#     objective = TaskObjective("simulate")
#     schema_1 = TaskSchema(objective, method="m1")
#     schema_2 = TaskSchema(objective)
#     task = TaskTemplate([schema_1, schema_2])
#     assert task.unique_name == "simulate_m1"


# def test_expected_unique_name_two_schemas_first_with_method_and_implementation():
#     objective = TaskObjective("simulate")
#     schema_1 = TaskSchema(objective, method="m1", implementation="i1")
#     schema_2 = TaskSchema(objective)
#     task = TaskTemplate([schema_1, schema_2])
#     assert task.unique_name == "simulate_m1_i1"


# def test_expected_unique_name_two_schemas_both_with_method():
#     objective = TaskObjective("simulate")
#     schema_1 = TaskSchema(objective, method="m1")
#     schema_2 = TaskSchema(objective, method="m2")
#     task = TaskTemplate([schema_1, schema_2])
#     assert task.unique_name == "simulate_m1_and_m2"


# def test_expected_unique_name_two_schemas_first_with_method_second_with_implementation():
#     objective = TaskObjective("simulate")
#     schema_1 = TaskSchema(objective, method="m1")
#     schema_2 = TaskSchema(objective, implementation="i2")
#     task = TaskTemplate([schema_1, schema_2])
#     assert task.unique_name == "simulate_m1_and_i2"


# def test_expected_unique_name_two_schemas_first_with_implementation_second_with_method():
#     objective = TaskObjective("simulate")
#     schema_1 = TaskSchema(objective, implementation="i1")
#     schema_2 = TaskSchema(objective, method="m2")
#     task = TaskTemplate([schema_1, schema_2])
#     assert task.unique_name == "simulate_i1_and_m2"


# def test_expected_unique_name_two_schemas_both_with_method_and_implementation():
#     objective = TaskObjective("simulate")
#     schema_1 = TaskSchema(objective, method="m1", implementation="i1")
#     schema_2 = TaskSchema(objective, method="m2", implementation="i2")
#     task = TaskTemplate([schema_1, schema_2])
#     assert task.unique_name == "simulate_m1_i1_and_m2_i2"


# def test_resolve_elements():
#     p1 = Parameter("p1")
#     schema = TaskSchema("simulate", inputs=[p1])
#     task = TaskTemplate(schema, input_values=[InputValue(p1, 1)])
#     task.resolve_elements()


# def test_resolve_elements_raise_on_missing_inputs():
#     schema = TaskSchema("simulate", inputs=[Parameter("p1")])
#     task = TaskTemplate(schema)
#     with pytest.raises(MissingInputs):
#         task.resolve_elements()


# def test_raise_on_negative_nesting_order():
#     schema = TaskSchema("simulate", inputs=[Parameter("p1")])
#     with pytest.raises(TaskTemplateInvalidNesting):
#         TaskTemplate(schema, nesting_order={"p1": -1})
