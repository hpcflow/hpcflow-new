from __future__ import annotations
import copy
from dataclasses import dataclass, field
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple, Union

from hpcflow.sdk import app
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

    def value(self, directory="."):
        return self.name.value(directory)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return False
        if self.label == other.label and self.name == other.name:
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

    def value(self, directory="."):
        format_args = [i.value(directory) for i in self.args or []]
        value = self.name.format(*format_args)
        if self.is_regex:
            value = search_dir_files_by_regex(value, group=0, directory=directory)
        return value

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"


@dataclass
class FileNameStem(JSONLike):
    file_name: app.FileNameSpec

    def value(self, directory=None):
        return Path(self.file_name.value(directory)).stem


@dataclass
class FileNameExt(JSONLike):
    file_name: app.FileNameSpec

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

    input_file: app.FileSpec
    inputs: List[app.Parameter]
    script: str = None
    environment: app.Environment = None
    abortable: Optional[bool] = False

    def get_action_rule(self):
        """Get the rule that allows testing if this input file generator must be
        run or not for a given element."""
        return self.app.ActionRule(check_missing=f"input_files.{self.input_file.label}")

    def compose_source(self) -> str:
        """Generate the file contents of this input file generator source."""

        script_name = self.script
        script_path = self.app.scripts.get(script_name)
        script_main_func = Path(script_name).stem

        with script_path.open("rt") as fp:
            script_str = fp.read()

        main_block = dedent(
            """\
            if __name__ == "__main__":
                import sys
                from pathlib import Path
                import {app_module} as app
                app.load_config(
                    log_file_path=Path("{app_package_name}.log").resolve(),
                    config_dir=r"{cfg_dir}",
                    config_invocation_key=r"{cfg_invoc_key}",
                )
                wk_path, EAR_ID = sys.argv[1:]
                EAR_ID = int(EAR_ID)
                wk = app.Workflow(wk_path)
                EAR = wk.get_EARs_from_IDs([EAR_ID])[0]
                {script_main_func}(path=Path({file_path!r}), **EAR.get_IFG_input_values())
        """
        )
        main_block = main_block.format(
            app_package_name=self.app.package_name,
            app_module=self.app.module,
            cfg_dir=self.app.config.config_directory,
            cfg_invoc_key=self.app.config.config_invocation_key,
            script_main_func=script_main_func,
            file_path=self.input_file.name.value(),
        )

        out = dedent(
            """\
            {script_str}
            {main_block}
        """
        )

        out = out.format(script_str=script_str, main_block=main_block)
        return out

    def write_source(self, action):
        script_path = action.get_script_path(self.script)
        with Path(script_path).open("wt", newline="\n") as fp:
            fp.write(self.compose_source())


@dataclass
class OutputFileParser(JSONLike):
    """
    Parameters
    ----------
    output
        The singular output parsed by this parser. Not to be confused with `outputs` (plural).
    outputs
        Optional multiple outputs from the upstream actions of the schema that are
        required to parametrise this parser.
    """

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
        ChildObjectSpec(
            name="save_files",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
        ChildObjectSpec(
            name="clean_up",
            class_name="FileSpec",
            is_multiple=True,
            shared_data_primary_key="label",
            shared_data_name="command_files",
        ),
    )

    output: app.Parameter
    output_files: List[app.FileSpec]
    script: str = None
    environment: Environment = None
    inputs: List[str] = None
    outputs: List[str] = None
    options: Dict = None
    abortable: Optional[bool] = False
    save_files: Union[List[str], bool] = True
    clean_up: Optional[List[str]] = None

    def __post_init__(self):
        if not self.save_files:
            # save no files
            self.save_files = []
        elif self.save_files is True:
            # save all output files
            self.save_files = [i for i in self.output_files]
        if self.clean_up is None:
            self.clean_up = []

    @classmethod
    def from_json_like(cls, json_like, shared_data=None):
        if "save_files" in json_like:
            if not json_like["save_files"]:
                json_like["save_files"] = []
            elif json_like["save_files"] is True:
                json_like["save_files"] = [i for i in json_like["output_files"]]
        return super().from_json_like(json_like, shared_data)

    def compose_source(self) -> str:
        """Generate the file contents of this output file parser source."""

        script_name = self.script
        script_path = self.app.scripts.get(script_name)
        script_main_func = Path(script_name).stem

        with script_path.open("rt") as fp:
            script_str = fp.read()

        main_block = dedent(
            """\
            if __name__ == "__main__":
                import sys
                from pathlib import Path
                import {app_module} as app
                app.load_config(
                    log_file_path=Path("{app_package_name}.log").resolve(),
                    config_dir=r"{cfg_dir}",
                    config_invocation_key=r"{cfg_invoc_key}",
                )
                wk_path, EAR_ID = sys.argv[1:]
                EAR_ID = int(EAR_ID)
                wk = app.Workflow(wk_path)
                EAR = wk.get_EARs_from_IDs([EAR_ID])[0]
                value = {script_main_func}(
                    **EAR.get_OFP_output_files(),
                    **EAR.get_OFP_inputs(),
                    **EAR.get_OFP_outputs(),
                )
                wk.save_parameter(name="{param_name}", value=value, EAR_ID=EAR_ID)

        """
        )
        main_block = main_block.format(
            app_package_name=self.app.package_name,
            app_module=self.app.module,
            cfg_dir=self.app.config.config_directory,
            cfg_invoc_key=self.app.config.config_invocation_key,
            script_main_func=script_main_func,
            param_name=f"outputs.{self.output.typ}",
        )

        out = dedent(
            """\
            {script_str}
            {main_block}
        """
        )

        out = out.format(script_str=script_str, main_block=main_block)
        return out

    def write_source(self, action):
        script_path = action.get_script_path(self.script)
        with Path(script_path).open("wt", newline="\n") as fp:
            fp.write(self.compose_source())


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

    def make_persistent(
        self,
        workflow: app.Workflow,
        source: Dict,
    ) -> Tuple[str, List[int], bool]:
        """Save to a persistent workflow.

        Returns
        -------
        String is the data path for this task input and integer list
        contains the indices of the parameter data Zarr groups where the data is
        stored.

        """

        if self._value_group_idx is not None:
            data_ref = self._value_group_idx
            is_new = False
            if not workflow.check_parameter_group_exists(data_ref):
                raise RuntimeError(
                    f"{self.__class__.__name__} has a parameter group index "
                    f"({data_ref}), but does not exist in the workflow."
                )
            # TODO: log if already persistent.
        else:
            data_ref = workflow._add_file(
                store_contents=self.store_contents,
                is_input=True,
                source=source,
                path=self.path,
                contents=self.contents,
                filename=self.file.name.name,
            )
            # data_ref = workflow._add_parameter_data(
            #     data=self._get_members(ensure_contents=True, use_file_label=True),
            #     source=source,
            # )
            is_new = True
            self._value_group_idx = data_ref
            self._workflow = workflow
            self._path = None
            self._contents = None
            self._extension = None
            self._store_contents = None

        return (self.normalised_path, [data_ref], is_new)

    def _get_value(self, value_name=None):
        # TODO: fix
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
    def workflow(self) -> app.Workflow:
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
        file: Union[app.FileSpec, str],
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
        generator: app.InputFileGenerator,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        self.generator = generator
        super().__init__(path, contents, extension)


class OutputFileParserSource(_FileContentsSpecifier):
    def __init__(
        self,
        parser: app.OutputFileParser,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        self.parser = parser
        super().__init__(path, contents, extension)
