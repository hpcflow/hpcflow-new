from __future__ import annotations
from typing import Dict, Optional, Union

from valida.conditions import ConditionLike
from valida.rules import Rule as ValidaRule

from hpcflow.sdk import app
from hpcflow.sdk.core.json_like import JSONLike
from hpcflow.sdk.core.utils import get_in_container


class Rule(JSONLike):
    """Class to represent a testable condition on an element iteration or run."""

    def __init__(
        self,
        check_exists: Optional[str] = None,
        check_missing: Optional[str] = None,
        path: Optional[str] = None,
        condition: Optional[Union[Dict, ConditionLike]] = None,
        cast: Optional[str] = None,
        doc: Optional[str] = None,
    ):
        if sum(i is not None for i in (check_exists, check_missing, condition)) != 1:
            raise ValueError(
                "Specify either one of `check_exists`, `check_missing` or a `condition` "
                "(and optional `path`)"
            )

        if isinstance(condition, dict):
            condition = ConditionLike.from_json_like(condition)

        self.check_exists = check_exists
        self.check_missing = check_missing
        self.path = path
        self.condition = condition
        self.cast = cast
        self.doc = doc

    def __repr__(self):
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

    def __eq__(self, other):
        if not isinstance(other, Rule):
            return False
        elif (
            self.check_exists == other.check_exists
            and self.check_missing == other.check_missing
            and self.path == other.path
            and self.condition == other.condition
            and self.cast == other.cast
            and self.doc == other.doc
        ):
            return True
        else:
            return False

    def test(
        self,
        element_like: Union[app.ElementIteration, app.ElementActionRun],
        action: Optional[app.Action] = None,
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
            if self.path.startswith("resources."):
                try:
                    # assume an `ElementIteration`
                    elem_res = element_like.get_resources(
                        action=action, set_defaults=True
                    )
                except TypeError:
                    # must be an `ElementActionRun`
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
            # note: Valida can't `rule.test` scalars yet, so wrap it in a list and set
            # path to first element (see: https://github.com/hpcflow/valida/issues/9):
            rule = ValidaRule(path=[0], condition=self.condition, cast=self.cast)
            return rule.test([element_dat]).is_valid
