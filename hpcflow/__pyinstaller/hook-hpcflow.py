from PyInstaller.utils.hooks import collect_data_files

from hpcflow.sdk import sdk_classes


# most of the modules in `sdk_classes` are imported on-demand via the app object:
hiddenimports = list(sdk_classes.values())

hiddenimports += [
    "hpcflow.sdk.data",
    "hpcflow.data.demo_data_manifest",
    "hpcflow.data.scripts",
    "hpcflow.data.template_components",
    "hpcflow.data.workflows",
    "hpcflow.tests.data",
    "hpcflow.sdk.core.test_utils",
    "click.testing",
    "requests",  # for GitHub fsspec file system
    "fsspec.implementations.github",  # for GitHub fsspec file system
]

py_include_kwargs = dict(include_py_files=True, excludes=("**/__pycache__",))
datas = (
    collect_data_files("hpcflow.sdk.data")
    + collect_data_files("hpcflow.data.demo_data_manifest")
    + collect_data_files("hpcflow.data.scripts", **py_include_kwargs)
    + collect_data_files("hpcflow.data.template_components")
    + collect_data_files("hpcflow.data.workflows")
    + collect_data_files("hpcflow.tests", **py_include_kwargs)
    + collect_data_files("hpcflow.tests.data")
)
