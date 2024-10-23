"""
Common type aliases.
"""
from __future__ import annotations
from dataclasses import InitVar
from typing import ClassVar, Final, TypeVar, cast, TYPE_CHECKING
from typing_extensions import NotRequired, TypeAlias, TypedDict
from pathlib import Path
import re

if TYPE_CHECKING:
    from datetime import datetime
    from .core.object_list import (
        CommandFilesList,
        EnvironmentsList,
        ParametersList,
        TaskSchemasList,
    )
    from .submission.enums import JobscriptElementState
    from .submission.submission import Submission

#: Type of a value that can be treated as a path.
PathLike: TypeAlias = "str | Path | None"


class ParamSource(TypedDict):
    """
    A parameter source descriptor.
    """

    #: Parameter type name.
    type: NotRequired[str]
    #: EAR ID.
    EAR_ID: NotRequired[int]
    #: Task insertion ID.
    task_insert_ID: NotRequired[int]
    #: Action index.
    action_idx: NotRequired[int]
    #: Element index.
    element_idx: NotRequired[int]
    #: Element set index.
    element_set_idx: NotRequired[int]
    #: Element action run index.
    run_idx: NotRequired[int]
    #: Sequence index.
    sequence_idx: NotRequired[int]
    #: Task index.
    task_idx: NotRequired[int]
    #: Name of method used to create the parameter's value(s).
    value_class_method: NotRequired[str]


class KnownSubmission(TypedDict):
    """
    Describes a known submission.
    """

    #: Local ID.
    local_id: int
    #: Workflow global ID.
    workflow_id: str
    #: Whether the submission is active.
    is_active: bool
    #: Submission index.
    sub_idx: int
    #: Submission time.
    submit_time: str
    #: Path to submission.
    path: str
    #: Start time.
    start_time: str
    #: Finish time.
    end_time: str


class KnownSubmissionItem(TypedDict):
    """
    Describes a known submission.
    """

    #: Local ID.
    local_id: int
    #: Workflow global ID.
    workflow_id: str
    #: Path to the workflow.
    workflow_path: str
    #: Time of submission.
    submit_time: str
    #: Parsed time of submission.
    submit_time_obj: NotRequired[datetime | None]
    #: Time of start.
    start_time: str
    #: Parsed time of start.
    start_time_obj: datetime | None
    #: Time of finish.
    end_time: str
    #: Parsed time of finish.
    end_time_obj: datetime | None
    #: Submission index.
    sub_idx: int
    #: Jobscripts in submission.
    jobscripts: list[int]
    #: Active jobscript state.
    active_jobscripts: dict[int, dict[int, JobscriptElementState]]
    #: Whether this is deleted.
    deleted: bool
    #: Whether this is unloadable.
    unloadable: bool
    #: Expanded submission object.
    submission: NotRequired[Submission]


class TemplateComponents(TypedDict):
    """
    Components loaded from templates.
    """

    #: Parameters loaded from templates.
    parameters: NotRequired[ParametersList]
    #: Command files loaded from templates.
    command_files: NotRequired[CommandFilesList]
    #: Execution environments loaded from templates.
    environments: NotRequired[EnvironmentsList]
    #: Task schemas loaded from templates.
    task_schemas: NotRequired[TaskSchemasList]
    #: Scripts discovered by templates.
    scripts: NotRequired[dict[str, Path]]


#: Simplification of :class:`TemplateComponents` to allow some types of
#: internal manipulations.
BasicTemplateComponents: TypeAlias = "dict[str, list[dict]]"

# EAR: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
#: Type of an element index:
#: (task_insert_ID, element_idx)
E_idx_type: TypeAlias = "tuple[int, int]"
#: Type of an element iteration index:
#: (task_insert_ID, element_idx, iteration_idx)
EI_idx_type: TypeAlias = "tuple[int, int, int]"
#: Type of an element action run index:
#: (task_insert_ID, element_idx, iteration_idx, action_idx, run_idx)
EAR_idx_type: TypeAlias = "tuple[int, int, int, int, int]"

DataIndex: TypeAlias = "dict[str, int | list[int]]"
"""
The type of indices to data. These are *normally* dictionaries of integers,
but can have leaves being lists of integers when dealing with iterations.
"""


_T = TypeVar("_T")

_CLASS_VAR_RE: Final = re.compile(r"ClassVar\[(.*)\]")
_INIT_VAR_RE: Final = re.compile(r"InitVar\[(.*)\]")


def hydrate(cls: type[_T]) -> type[_T]:
    """
    Partially hydrates the annotations on fields in a class, so that a @dataclass
    annotation can recognise that ClassVar-annotated fields are class variables.
    """
    anns = {}
    for f, a in cls.__annotations__.items():
        if isinstance(a, str):
            m = _CLASS_VAR_RE.match(a)
            if m:
                anns[f] = ClassVar[m[1]]
                continue
            m = _INIT_VAR_RE.match(a)
            if m:
                anns[f] = InitVar(cast(type, m[1]))
                continue
        anns[f] = a
    cls.__annotations__ = anns
    return cls
