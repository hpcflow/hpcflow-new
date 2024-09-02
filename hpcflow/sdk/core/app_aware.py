from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import ClassVar
    from hpcflow.sdk.app import BaseApp


class AppAware:
    """
    A base class that marks its subclasses as aware of the application.

    Attributes
    ----------
    _app: BaseApp
        A class attribute that holds the application instance.
    """

    __slots__: ClassVar[tuple[str, ...]] = ()
    _app: ClassVar[BaseApp]
    _app_attr: ClassVar[str] = "_app"
