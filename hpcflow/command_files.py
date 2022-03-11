from dataclasses import dataclass
from pathlib import Path
from re import I
from typing import Dict, List, Union


from hpcflow.parameters import Parameter
from hpcflow.environment import Environment
from hpcflow.utils import search_dir_files_by_regex


@dataclass
class FileSpec:
    label: str
    name: str

    def __post_init__(self):
        self.name = FileNameSpec(self.name) if isinstance(self.name, str) else self.name
        self.stem = self.name.stem
        self.ext = self.name.ext

    def value(self, directory=None):
        return self.name.value(directory)

    @classmethod
    def from_spec(cls, spec):
        spec["name"] = FileNameSpec.from_spec(spec["name"])
        return cls(**spec)


class FileNameSpec:
    def __init__(self, name, args=None, is_regex=False):
        self.name = name
        self.args = args
        self.is_regex = is_regex
        self.stem = FileNameStem(self)
        self.ext = FileNameExt(self)

    def value(self, directory=None):
        format_args = [i.value(directory) for i in self.args or []]
        value = self.name.format(*format_args)
        if self.is_regex:
            value = search_dir_files_by_regex(value, group=0, directory=directory)
        return value

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name})"

    @classmethod
    def from_spec(cls, spec):
        return cls(**spec)


@dataclass
class FileNameStem:
    file_name: FileNameSpec

    def value(self, directory=None):
        return Path(self.file_name.value(directory)).stem


@dataclass
class FileNameExt:
    file_name: FileNameSpec

    def value(self, directory=None):
        return Path(self.file_name.value(directory)).suffix


@dataclass
class InputFileGenerator:
    input_file: FileSpec
    inputs: List[Parameter]
    environment: Environment = None

    @classmethod
    def from_spec(cls, label, info, parameters, cmd_files):
        input_file = [i for i in cmd_files if i.label == label][0]
        inputs = [parameters[typ] for typ in info["from_inputs"]]
        return cls(input_file, inputs)


@dataclass
class OutputFileParser:
    output: Parameter
    output_files: List[FileSpec]
    environment: Environment = None
    inputs: List[str] = None
    options: Dict = None

    @classmethod
    def from_spec(cls, param_typ, info, parameters, cmd_files):
        output = parameters[param_typ]
        output_files = [
            [j for j in cmd_files if j.label == label][0]
            for label in info["from_files"]
        ]
        return cls(output, output_files)


class _FileContentsSpecifier:
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
    def __init__(
        self,
        file: FileSpec,
        path: Union[Path, str] = None,
        contents: str = None,
        extension: str = "",
    ):
        super().__init__(path, contents, extension)
        self.file = file


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
