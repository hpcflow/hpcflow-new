# from hpcflow.parameters import InputValue, Parameter
# from hpcflow.task import TaskTemplate
# from hpcflow.task_schema import TaskObjective, TaskSchema


# def test_expected_return_resolve_elements():

#     generate_RVE = TaskObjective("generate_volume_element")
#     simulate = TaskObjective("simulate_volume_element_loading")

#     grid_size = Parameter("grid_size")
#     size = Parameter("size")
#     volume_element = Parameter("volume_element")
#     load_case = Parameter("load_case")
#     volume_element_response = Parameter("volume_element_response")

#     generate_RVE_schema = TaskSchema(
#         objective=generate_RVE, inputs=[grid_size, size], outputs=[volume_element]
#     )
#     simulate_schema = TaskSchema(
#         objective=simulate,
#         inputs=[volume_element, load_case],
#         outputs=[volume_element_response],
#     )

#     task_1 = TaskTemplate(
#         schema=generate_RVE_schema,
#         input_values=[
#             InputValue(grid_size, value=[8, 8, 8]),
#             InputValue(size, value=[1, 1, 1]),
#         ],
#     )
#     task_2 = TaskTemplate(
#         schema=simulate_schema, input_values=[InputValue(load_case, value=10)]
#     )
