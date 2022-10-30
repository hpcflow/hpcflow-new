from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union
from hpcflow.sdk.core.json_like import ChildObjectSpec, JSONLike


from hpcflow.sdk.core.parameters import Parameter
from hpcflow.sdk.core.environment import Environment
from hpcflow.sdk.core.utils import search_dir_files_by_regex


@dataclass
class FileSpec(JSONLike):

    app = None
    _validation_schema = "files_spec_schema.yaml"
    _child_objects = (ChildObjectSpec(name="name", class_name="FileNameSpec"),)

    label: str
    name: str
    _hash_value: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        self.name = FileNameSpec(self.name) if isinstance(self.name, str) else self.name

    def value(self, directory=None):
        return self.name.value(directory)

    @property
    def stem(self):
        return self.name.stem

    @property
    def ext(self):
        return self.name.ext


class FileNameSpec(JSONLike):
    def __init__(self, name, args=None, is_regex=False):
        self.name = name
        self.args = args
        self.is_regex = is_regex

    @property
    def stem(self):
        return FileNameStem(self)

    @property
    def ext(self):
        return FileNameExt(self)

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

    app = None
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
    environment: Environment = None

    # @classmethod
    # def from_spec(cls, label, info, parameters, cmd_files):
    #     input_file = [i for i in cmd_files if i.label == label][0]
    #     inputs = [parameters[typ] for typ in info["from_inputs"]]
    #     return cls(input_file, inputs)


@dataclass
class OutputFileParser(JSONLike):

    app = None
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
    environment: Environment = None
    inputs: List[str] = None
    options: Dict = None

    # @classmethod
    # def from_spec(cls, param_typ, info, parameters, cmd_files):
    #     output = parameters[param_typ]
    #     output_files = [
    #         [j for j in cmd_files if j.label == label][0]
    #         for label in info["from_files"]
    #     ]
    #     return cls(output, output_files)


class _FileContentsSpecifier(JSONLike):
    """Class to represent the contents of a file, either via a file-system path or directly."""

    def __init__(
        self, path: Union[Path, str] = None, contents: str = None, extension: str = ""
    ):
        self.path = Path(path) if path is not None else path
        self._contents = contents
        self.extension = extension

        if (path is not None and contents is not None) or (
            path is None and contents is None
        ):
            raise ValueError("Specify exactly one of `path` and `contents`.")

    @property
    def contents(self):
        if self.path is not None:
            with self.path.open("r") as fh:
                contents = fh.read()
        else:
            contents = self._contents
        return contents


class InputFile(_FileContentsSpecifier):
    app = None

    def __init__(
        self,
        file: Union[FileSpec, str],
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        super().__init__(path, contents, extension)
        self.file = file
        if not isinstance(self.file, FileSpec):
            self.file = self.app.command_files[self.file]


class InputFileGeneratorSource(_FileContentsSpecifier):
    def __init__(
        self,
        generator: InputFileGenerator,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        super().__init__(path, contents, extension)
        self.generator = generator


class OutputFileParserSource(_FileContentsSpecifier):
    def __init__(
        self,
        parser: OutputFileParser,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        super().__init__(path, contents, extension)
        self.parser = parser
