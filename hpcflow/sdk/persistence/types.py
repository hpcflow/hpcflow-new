"""
Types used in type-checking the persistence subsystem.
"""
from __future__ import annotations
from typing import TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from .base import StoreTask, StoreElement, StoreElementIter, StoreEAR, StoreParameter

#: Bound type variable.
AnySTask = TypeVar("AnySTask", bound="StoreTask")
#: Bound type variable.
AnySElement = TypeVar("AnySElement", bound="StoreElement")
#: Bound type variable.
AnySElementIter = TypeVar("AnySElementIter", bound="StoreElementIter")
#: Bound type variable.
AnySEAR = TypeVar("AnySEAR", bound="StoreEAR")
#: Bound type variable.
AnySParameter = TypeVar("AnySParameter", bound="StoreParameter")
