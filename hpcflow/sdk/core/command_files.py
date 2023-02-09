from __future__ import annotations
import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union

from valida.rules import Rule
from valida.conditions import Value

from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike
from hpcflow.sdk.core.environment import Environment
from hpcflow.sdk.core.utils import search_dir_files_by_regex
from hpcflow.sdk.core.zarr_io import zarr_decode


@dataclass
class FileSpec(JSONLike):

    _app_attr = "app"

    _validation_schema = "files_spec_schema.yaml"
    _child_objects = (ChildObjectSpec(name="name", class_name="FileNameSpec"),)

    label: str
    name: str
    _hash_value: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.name = (
            self.app.FileNameSpec(self.name) if isinstance(self.name, str) else self.name
        )

    def value(self, directory=None):
        return self.name.value(directory)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.label is other.label and self.name == other.name:
            return True
        return False

    @property
    def stem(self):
        return self.name.stem

    @property
    def ext(self):
        return self.name.ext


class FileNameSpec(JSONLike):

    _app_attr = "app"

    def __init__(self, name, args=None, is_regex=False):
        self.name = name
        self.args = args
        self.is_regex = is_regex

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return (
            self.name == other.name
            and self.args == other.args
            and self.is_regex == other.is_regex
        )

    @property
    def stem(self):
        return self.app.FileNameStem(self)

    @property
    def ext(self):
        return self.app.FileNameExt(self)

    def value(self, directory=None):
        format_args = [i.value(directory) for i in self.args or []]
        value = self.name.format(*format_args)
        if self.is_regex:
            value = search_dir_files_by_regex(value, group=0, directory=directory)
        return value

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"


@dataclass
class FileNameStem(JSONLike):
    file_name: FileNameSpec

    def value(self, directory=None):
        return Path(self.file_name.value(directory)).stem


@dataclass
class FileNameExt(JSONLike):
    file_name: FileNameSpec

    def value(self, directory=None):
        return Path(self.file_name.value(directory)).suffix


@dataclass
class InputFileGenerator(JSONLike):

    _app_attr = "app"

    _child_objects = (
        ChildObjectSpec(
            name="input_file",
            class_name="FileSpec",
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="inputs",
            class_name="Parameter",
            is_multiple=True,
            json_like_name="from_inputs",
            shared_data_primary_key="typ",
            shared_data_name="parameters",
        ),
    )

    input_file: FileSpec
    inputs: List[Parameter]
    script: str = None
    environment: Environment = None

    def get_action_rule(self):
        """Get the rule that allows testing if this input file generator must be
        run or not for a given element."""
        return self.app.ActionRule(check_missing=f"input_files.{self.input_file.label}")


@dataclass
class OutputFileParser(JSONLike):
    # TODO: Rename output parser

    _child_objects = (
        ChildObjectSpec(
            name="output",
            class_name="Parameter",
            shared_data_name="parameters",
            shared_data_primary_key="typ",
        ),
        ChildObjectSpec(
            name="output_files",
            json_like_name="from_files",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
    )

    output: Parameter
    output_files: List[FileSpec]
    script: str = None
    environment: Environment = None
    inputs: List[str] = None
    options: Dict = None


class _FileContentsSpecifier(JSONLike):
    """Class to represent the contents of a file, either via a file-system path or
    directly."""

    def __init__(
        self,
        path: Union[Path, str] = None,
        contents: Optional[str] = None,
        extension: Optional[str] = "",
        store_contents: Optional[bool] = True,
    ):

        if path is not None and contents is not None:
            raise ValueError("Specify exactly one of `path` and `contents`.")

        if contents is not None and not store_contents:
            raise ValueError(
                "`store_contents` cannot be set to False if `contents` was specified."
            )

        self._path = path
        self._contents = contents
        self._extension = extension
        self._store_contents = store_contents

        # assigned by `make_persistent`
        self._workflow = None
        self._value_group_idx = None

        # assigned by parent `ElementSet`
        self._element_set = None

    def __deepcopy__(self, memo):
        kwargs = self.to_dict()
        value_group_idx = kwargs.pop("value_group_idx")
        obj = self.__class__(**copy.deepcopy(kwargs, memo))
        obj._value_group_idx = value_group_idx
        obj._workflow = self._workflow
        obj._element_set = self._element_set
        return obj

    def to_dict(self):
        out = super().to_dict()
        if "_workflow" in out:
            del out["_workflow"]

        out = {k.lstrip("_"): v for k, v in out.items()}
        return out

    @classmethod
    def _json_like_constructor(cls, json_like):
        """Invoked by `JSONLike.from_json_like` instead of `__init__`."""

        _value_group_idx = json_like.pop("value_group_idx", None)
        obj = cls(**json_like)
        obj._value_group_idx = _value_group_idx

        return obj

    def _get_members(self, ensure_contents=False):
        out = self.to_dict()
        del out["value_group_idx"]

        if ensure_contents and self._store_contents and self._contents is None:
            out["contents"] = self.read_contents()

        return out

    def make_persistent(self, workflow) -> Dict:
        """Save to a persistent workflow.

        Parameters
        ----------
        workflow : Workflow

        Returns
        -------

        (str, list of int)
            String is the data path for this task input and integer list
            contains the indices of the parameter data Zarr groups where the data is
            stored.
        """
        if self._value_group_idx is not None:
            param_group_idx = self._value_group_idx
            is_new = False
            if not workflow.check_parameter_group_exists(param_group_idx):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a parameter group index "
                    f"({param_group_idx}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
        else:
            param_group_idx = workflow._add_parameter_group(
                data=self._get_members(ensure_contents=True, use_file_label=True),
                is_pending_add=workflow._in_batch_mode,
                is_set=True,
            )
            is_new = True
            self._value_group_idx = param_group_idx
            self._workflow = workflow
            self._path = None
            self._contents = None
            self._extension = None
            self._store_contents = None

        return (self.normalised_path, [param_group_idx], is_new)

    def _get_value(self, value_name=None):
        if self._value_group_idx is not None:
            grp = self.workflow.get_zarr_parameter_group(self._value_group_idx)
            val = zarr_decode(grp)
        else:
            val = self._get_members(ensure_contents=(value_name == "contents"))
        if value_name:
            val = val.get(value_name)

        return val

    def read_contents(self):
        with self.path.open("r") as fh:
            return fh.read()

    @property
    def path(self):
        path = self._get_value("path")
        return Path(path) if path else None

    @property
    def store_contents(self):
        return self._get_value("store_contents")

    @property
    def contents(self):
        if self.store_contents:
            contents = self._get_value("contents")
        else:
            contents = self.read_contents()

        return contents

    @property
    def extension(self):
        return self._get_value("extension")

    @property
    def workflow(self):
        if self._workflow:
            return self._workflow
        elif self._element_set:
            return self._element_set.task_template.workflow_template.workflow


class InputFile(_FileContentsSpecifier):

    _child_objects = (
        ChildObjectSpec(
            name="file",
            class_name="FileSpec",
            shared_data_name="command_files",
            shared_data_primary_key="label",
        ),
    )

    def __init__(
        self,
        file: Union[FileSpec, str],
        path: Optional[Union[Path, str]] = None,
        contents: Optional[str] = None,
        extension: Optional[str] = "",
        store_contents: Optional[bool] = True,
    ):
        self.file = file
        if not isinstance(self.file, FileSpec):
            self.file = self.app.command_files.get(self.file.label)

        super().__init__(path, contents, extension, store_contents)

    def to_dict(self):
        dct = super().to_dict()
        return dct

    def _get_members(self, ensure_contents=False, use_file_label=False):
        out = super()._get_members(ensure_contents)
        if use_file_label:
            out["file"] = self.file.label
        return out

    def __repr__(self):

        val_grp_idx = ""
        if self._value_group_idx is not None:
            val_grp_idx = f", value_group_idx={self._value_group_idx}"

        path_str = ""
        if self.path is not None:
            path_str = f", path={self.path!r}"

        return (
            f"{self.__class__.__name__}("
            f"file={self.file.label!r}"
            f"{path_str}"
            f"{val_grp_idx}"
            f")"
        )

    @property
    def normalised_files_path(self):
        return self.file.label

    @property
    def normalised_path(self):
        return f"input_files.{self.normalised_files_path}"


class InputFileGeneratorSource(_FileContentsSpecifier):
    def __init__(
        self,
        generator: InputFileGenerator,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        self.generator = generator
        super().__init__(path, contents, extension)


class OutputFileParserSource(_FileContentsSpecifier):
    def __init__(
        self,
        parser: OutputFileParser,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        self.parser = parser
        super().__init__(path, contents, extension)
