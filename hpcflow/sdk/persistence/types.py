"""
Types used in type-checking the persistence subsystem.
"""
from __future__ import annotations
from typing import Any, Generic, TypeVar, TYPE_CHECKING
from typing_extensions import TypedDict, NotRequired, TypeAlias

if TYPE_CHECKING:
    from .base import StoreTask, StoreElement, StoreElementIter, StoreEAR, StoreParameter
    from ..core.json_like import JSONDocument
    from ..core.parameters import ParameterValue
    from ..core.types import IterableParam
    from ..typing import DataIndex, ParamSource

#: Bound type variable: :class:`StoreTask`.
AnySTask = TypeVar("AnySTask", bound="StoreTask")
#: Bound type variable: :class:`StoreElement`.
AnySElement = TypeVar("AnySElement", bound="StoreElement")
#: Bound type variable: :class:`StoreElementITer`.
AnySElementIter = TypeVar("AnySElementIter", bound="StoreElementIter")
#: Bound type variable: :class:`StoreEAR`.
AnySEAR = TypeVar("AnySEAR", bound="StoreEAR")
#: Bound type variable: :class:`StoreParameter`.
AnySParameter = TypeVar("AnySParameter", bound="StoreParameter")
#: Type of possible stored parameters.
ParameterTypes: TypeAlias = (
    "ParameterValue | list | tuple | set | dict | int | float | str | None | Any"
)


class File(TypedDict):
    """
    Descriptor for file metadata.
    """

    #: Whether to store the contents.
    store_contents: bool
    #: The path to the file.
    path: str


class FileDescriptor(TypedDict):
    """
    Descriptor for file metadata.
    """

    #: Whether this is an input file.
    is_input: bool
    #: Whether to store the contents.
    store_contents: bool
    #: Where the file will go.
    dst_path: str
    #: The path to the file.
    path: str | None
    #: Whether to delete the file after processing.
    clean_up: bool
    # The contents of the file.
    contents: NotRequired[str]


class LoopDescriptor(TypedDict):
    """
    Descriptor for loop metadata.
    """

    #: The parameters iterated over by the loop.
    iterable_parameters: dict[str, IterableParam]
    #: The template data from which the loop was created.
    loop_template: NotRequired[dict[str, Any]]
    #: The number of iterations generated by a loop.
    #: Note that the type is really ``list[tuple[tuple[int, ...], int]]``
    #: but the persistence implementations don't handle tuples usefully.
    num_added_iterations: list[list[list[int] | int]]
    #: The parents of the loop.
    parents: list[str]


# TODO: This type looks familiar...
class StoreCreationInfo(TypedDict):
    """
    Information about the creation of the persistence store.
    """

    #: Information about the application.
    app_info: dict[str, Any]
    #: When the persistence store was created.
    create_time: str
    #: The unique identifier for for the store/workflow.
    id: str


class ElemMeta(TypedDict):
    """
    The kwargs supported for a StoreElement.
    """

    #: The ID of the element.
    id_: int
    #: The index of the element.
    index: int
    #: The index of the element in its element set.
    es_idx: int
    #: The indices of the element in the sequences that contain it.
    seq_idx: dict[str, int]
    #: The indices of the element's sources.
    src_idx: dict[str, int]
    #: The task associated with the element.
    task_ID: int
    #: The iteration IDs.
    iteration_IDs: list[int]


class IterMeta(TypedDict):
    """
    The kwargs supported for a StoreElementIter.
    """

    #: The index of the iteration.
    data_idx: DataIndex
    #: The EARs associated with the iteration.
    EAR_IDs: dict[int, list[int]]
    #: Whether the EARs have been initialised.
    EARs_initialised: bool
    #: The ID of the element.
    element_ID: int
    #: The loops containing the iteration.
    loop_idx: dict[str, int]
    #: The schema parameters being iterated over.
    schema_parameters: list[str]


class RunMeta(TypedDict):
    """
    The kwargs supported for StoreEAR.
    """

    #: The ID of the EAR.
    id_: int
    #: The ID of the element iteration containing the EAR.
    elem_iter_ID: int
    #: The index of the action that generated the EAR.
    action_idx: int
    #: The commands that the EAR will run.
    commands_idx: list[int]
    #: The data handled by the EAR.
    data_idx: DataIndex
    #: Metadata about the EAR.
    metadata: Metadata | None
    #: When the EAR ended, if known.
    end_time: NotRequired[str | None]
    #: The exit code of the EAR, if known.
    exit_code: int | None
    #: When the EAR started, if known.
    start_time: NotRequired[str | None]
    #: Working directory snapshot at start.
    snapshot_start: dict[str, Any] | None
    #: Working directory snapshot at end.
    snapshot_end: dict[str, Any] | None
    #: The index of the EAR in the submissions.
    submission_idx: int | None
    #: Where the EAR is set to run.
    run_hostname: str | None
    #: Whether the EAR succeeded, if known.
    success: bool | None
    #: Whether the EAR was skipped.
    skip: bool


class TaskMeta(TypedDict):
    """
    Information about a task.
    """

    #: The ID of the task.
    id_: int
    #: The index of the task in the workflow.
    index: int
    #: The elements in the task.
    element_IDs: list[int]


class TemplateMeta(TypedDict):  # FIXME: Incomplete, see WorkflowTemplate
    """
    Metadata about a workflow template.
    """

    #: Descriptors for loops.
    loops: list[dict]
    #: Descriptors for tasks.
    tasks: list[dict]


class Metadata(TypedDict):
    """
    Workflow metadata.
    """

    #: Information about the store's creation.
    creation_info: NotRequired[StoreCreationInfo]
    #: Elements in the workflow.
    elements: NotRequired[list[ElemMeta]]
    #: Iterations in the workflow.
    iters: NotRequired[list[IterMeta]]
    #: Loops in the workflow.
    loops: NotRequired[list[LoopDescriptor]]
    #: The name of the workflow.
    name: NotRequired[str]
    #: The number of added tasks.
    num_added_tasks: NotRequired[int]
    #: The replacement workflow, if any.
    replaced_workflow: NotRequired[str]
    #: Element Action Runs in the workflow.
    runs: NotRequired[list[RunMeta]]
    #: Tasks in the workflow.
    tasks: NotRequired[list[TaskMeta]]
    #: The template that generated the workflow.
    template: NotRequired[TemplateMeta]
    #: Custom template components used.
    template_components: NotRequired[dict[str, Any]]
    #: Format for timestamps.
    ts_fmt: NotRequired[str]
    #: Format for timestamps used in naming.
    ts_name_fmt: NotRequired[str]


class TypeLookup(TypedDict, total=False):
    """
    Information for looking up the type of a parameter.

    Note
    ----
    Not a total typed dictionary.
    """

    #: Tuples involving the parameter.
    tuples: list[list[int]]
    #: Sets involving the parameter.
    sets: list[list[int]]
    #: Arrays involving the parameter.
    arrays: list[list[list[int] | int]]
    #: Masked arrays involving the parameter.
    masked_arrays: list[list[int | list[int]]]


class EncodedStoreParameter(TypedDict):
    """
    The encoding of a :class:`StoreParameter`.
    """

    #: The parameter data.
    data: Any
    #: Information for looking up the type.
    type_lookup: TypeLookup


class PersistenceCache(
    TypedDict, Generic[AnySTask, AnySElement, AnySElementIter, AnySEAR, AnySParameter]
):
    """
    Cache used internally by the persistence engine.
    """

    #: Tasks.
    tasks: dict[int, AnySTask]
    #: Elements.
    elements: dict[int, AnySElement]
    #: Element iterations.
    element_iters: dict[int, AnySElementIter]
    #: Element action runs.
    EARs: dict[int, AnySEAR]
    #: Parameter sources.
    param_sources: dict[int, ParamSource]
    #: Number of tasks.
    num_tasks: int | None
    #: Parameters.
    parameters: dict[int, AnySParameter]
    #: Number of element action runs.
    num_EARs: int | None


class ZarrAttrsDict(TypedDict):
    """
    Zarr workflow attributes descriptor.
    """

    #: Workflow name.
    name: str
    #: Timestamp format.
    ts_fmt: str
    #: Timestamp format for names.
    ts_name_fmt: str
    #: Information about the creation of the workflow and persistent store.
    creation_info: StoreCreationInfo
    #: The template used to build the workflow.
    template: TemplateMeta
    #: Custom components used to build the workflow.
    template_components: dict[str, Any]
    #: Number of tasks added.
    num_added_tasks: int
    #: Tasks in the workflow.
    tasks: list[dict[str, Any]]
    #: Loops in the workflow.
    loops: list[dict[str, Any]]
    #: Submissions by the workflow.
    submissions: list[JSONDocument]
    #: Replacement workflow, if any.
    replaced_workflow: NotRequired[str]
