import pytest
from hpcflow.sdk.core.parameters import InputSource, InputSourceType, TaskSourceType


def test_input_source_class_method_local():
    assert InputSource.local() == InputSource(InputSourceType.LOCAL)


def test_input_source_class_method_default():
    assert InputSource.default() == InputSource(InputSourceType.DEFAULT)


def test_input_source_class_method_task():
    task_ref = 0
    assert InputSource.task(task_ref) == InputSource(
        source_type=InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_class_method_import():
    import_ref = (
        0  # TODO: interface to imports (and so how to reference) is not yet decided
    )
    assert InputSource.import_(import_ref) == InputSource(
        InputSourceType.IMPORT, import_ref=import_ref
    )


def test_input_source_class_method_task_same_default_task_source_type():
    task_ref = 0
    assert (
        InputSource(InputSourceType.TASK, task_ref=task_ref).task_source_type
        == InputSource.task(task_ref=task_ref).task_source_type
    )


def test_input_source_validate_source_type_string_local():
    assert InputSource("local") == InputSource(InputSourceType.LOCAL)


def test_input_source_validate_source_type_string_default():
    assert InputSource("default") == InputSource(InputSourceType.DEFAULT)


def test_input_source_validate_source_type_string_task():
    task_ref = 0
    assert InputSource("task", task_ref=task_ref) == InputSource(
        InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_validate_source_type_string_import():
    import_ref = (
        0  # TODO: interface to imports (and so how to reference) is not yet decided
    )
    assert InputSource("import", import_ref=import_ref) == InputSource(
        InputSourceType.IMPORT, import_ref=import_ref
    )


def test_input_source_validate_source_type_raise_on_unknown_string():
    with pytest.raises(ValueError):
        InputSource("bad_source_type")


def test_input_source_validate_task_source_type_string_any():
    task_ref = 0
    assert InputSource(
        InputSourceType.TASK, task_ref=task_ref, task_source_type="any"
    ) == InputSource(
        InputSourceType.TASK, task_ref=task_ref, task_source_type=TaskSourceType.ANY
    )


def test_input_source_validate_task_source_type_string_input():
    task_ref = 0
    assert InputSource(
        InputSourceType.TASK, task_ref=task_ref, task_source_type="input"
    ) == InputSource(
        InputSourceType.TASK, task_ref=task_ref, task_source_type=TaskSourceType.INPUT
    )


def test_input_source_validate_task_source_type_string_output():
    task_ref = 0
    assert InputSource(
        InputSourceType.TASK, task_ref=task_ref, task_source_type="output"
    ) == InputSource(
        InputSourceType.TASK, task_ref=task_ref, task_source_type=TaskSourceType.OUTPUT
    )


def test_input_source_validate_task_source_type_raise_on_unknown_string():
    task_ref = 0
    with pytest.raises(ValueError):
        InputSource(
            InputSourceType.TASK,
            task_ref=task_ref,
            task_source_type="bad_task_source_type",
        )


def test_input_source_to_string_local():
    assert InputSource.local().to_string() == "local"


def test_input_source_to_string_default():
    assert InputSource.default().to_string() == "default"


def test_input_source_to_string_task_output():
    task_ref = 0
    assert (
        InputSource.task(task_ref, task_source_type="output").to_string()
        == f"task.{task_ref}.output"
    )


def test_input_source_to_string_task_input():
    task_ref = 0
    assert (
        InputSource.task(task_ref, task_source_type="input").to_string()
        == f"task.{task_ref}.input"
    )


def test_input_source_to_string_task_any():
    task_ref = 0
    assert (
        InputSource.task(task_ref, task_source_type="any").to_string()
        == f"task.{task_ref}.any"
    )


def test_input_source_to_string_import():
    import_ref = 0
    assert InputSource.import_(import_ref).to_string() == f"import.{import_ref}"


def test_input_source_from_string_local():
    assert InputSource.from_string("local") == InputSource(InputSourceType.LOCAL)


def test_input_source_from_string_default():
    assert InputSource.from_string("default") == InputSource(InputSourceType.DEFAULT)


def test_input_source_from_string_task():
    assert InputSource.from_string("task.0.output") == InputSource(
        InputSourceType.TASK, task_ref=0, task_source_type=TaskSourceType.OUTPUT
    )


def test_input_source_from_string_task_same_default_task_source():
    task_ref = 0
    assert InputSource.from_string(f"task.{task_ref}") == InputSource(
        InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_from_string_import():
    import_ref = 0
    assert InputSource.from_string(f"import.{import_ref}") == InputSource(
        InputSourceType.IMPORT, import_ref=import_ref
    )
