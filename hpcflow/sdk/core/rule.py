from __future__ import annotations
from typing import TypedDict, TYPE_CHECKING

from valida.conditions import ConditionLike  # type: ignore
from valida import Rule as ValidaRule  # type: ignore

from hpcflow.sdk.core.json_like import JSONLike
from hpcflow.sdk.core.utils import get_in_container
from hpcflow.sdk.log import TimeIt

if TYPE_CHECKING:
    from typing import Any, ClassVar
    from typing_extensions import NotRequired
    from .actions import Action, ElementActionRun
    from .element import ElementIteration
    from ..app import BaseApp


class RuleArgs(TypedDict):
    """
    The keyword arguments that may be used to create a Rule.
    """

    check_exists: NotRequired[str]
    check_missing: NotRequired[str]
    path: NotRequired[str]
    condition: NotRequired[dict[str, Any] | ConditionLike]
    cast: NotRequired[str]
    doc: NotRequired[str]


class Rule(JSONLike):
    """Class to represent a testable condition on an element iteration or run."""

    app: ClassVar[BaseApp]

    def __init__(
        self,
        check_exists: str | None = None,
        check_missing: str | None = None,
        path: str | None = None,
        condition: dict[str, Any] | ConditionLike | None = None,
        cast: str | None = None,
        doc: str | None = None,
    ):
        if sum(i is not None for i in (check_exists, check_missing, condition)) != 1:
            raise ValueError(
                "Specify either one of `check_exists`, `check_missing` or a `condition` "
                "(and optional `path`)"
            )

        if not isinstance(condition, dict):
            self.condition = condition
        else:
            self.condition = ConditionLike.from_json_like(condition)

        self.check_exists = check_exists
        self.check_missing = check_missing
        self.path = path
        self.cast = cast
        self.doc = doc

    def __repr__(self) -> str:
        out = f"{self.__class__.__name__}("
        if self.check_exists:
            out += f"check_exists={self.check_exists!r}"
        elif self.check_missing:
            out += f"check_missing={self.check_missing!r}"
        else:
            out += f"condition={self.condition!r}"
            if self.path:
                out += f", path={self.path!r}"
            if self.cast:
                out += f", cast={self.cast!r}"

        out += ")"
        return out

    def __eq__(self, other) -> bool:
        if not isinstance(other, Rule):
            return False
        return (
            self.check_exists == other.check_exists
            and self.check_missing == other.check_missing
            and self.path == other.path
            and self.condition == other.condition
            and self.cast == other.cast
            and self.doc == other.doc
        )

    @TimeIt.decorator
    def test(
        self,
        element_like: ElementIteration | ElementActionRun,
        action: Action | None = None,
    ) -> bool:
        """Test if the rule evaluates to true or false for a given run, or element
        iteration and action combination."""

        task = element_like.task
        schema_data_idx = element_like.data_idx

        check = self.check_exists or self.check_missing
        if check:
            param_s = check.split(".")
            if len(param_s) > 2:
                # sub-parameter, so need to try to retrieve parameter data
                try:
                    task._get_merged_parameter_data(
                        schema_data_idx, raise_on_missing=True
                    )
                    return True if self.check_exists else False
                except ValueError:
                    return False if self.check_exists else True
            else:
                if self.check_exists:
                    return self.check_exists in schema_data_idx
                elif self.check_missing:
                    return self.check_missing not in schema_data_idx
        else:
            if self.path and self.path.startswith("resources."):
                if isinstance(element_like, self.app.ElementIteration):
                    assert action is not None
                    elem_res = element_like.get_resources(
                        action=action, set_defaults=True
                    )
                else:
                    # must be an `ElementActionRun`
                    assert isinstance(element_like, self.app.ElementActionRun)
                    elem_res = element_like.get_resources()

                res_path = self.path.split(".")[1:]
                element_dat = get_in_container(
                    cont=elem_res, path=res_path, cast_indices=True
                )
            else:
                element_dat = element_like.get(
                    self.path,
                    raise_on_missing=True,
                    raise_on_unset=True,
                )
            # test the rule:
            return self._valida_check(element_dat)

        # Something bizarre was specified. Don't match it!
        return False

    def _valida_check(self, value: Any) -> bool:
        """
        Check this rule against the specific object, under the assumption that we need
        to use valida for the check. Does not do path tracing to select the object to
        pass; that is the caller's responsibility.
        """
        # note: Valida can't `rule.test` scalars yet, so wrap it in a list and set
        # path to first element (see: https://github.com/hpcflow/valida/issues/9):
        rule = ValidaRule(
            path=[0],
            condition=self.condition,
            cast=self.cast,
        )
        return rule.test([value]).is_valid
