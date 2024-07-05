from typing import TypeVar, TYPE_CHECKING
if TYPE_CHECKING:
    from .base import StoreTask, StoreElement, StoreElementIter, StoreEAR, StoreParameter

AnySTask = TypeVar("AnySTask", bound="StoreTask")
AnySElement = TypeVar("AnySElement", bound="StoreElement")
AnySElementIter = TypeVar("AnySElementIter", bound="StoreElementIter")
AnySEAR = TypeVar("AnySEAR", bound="StoreEAR")
AnySParameter = TypeVar("AnySParameter", bound="StoreParameter")
