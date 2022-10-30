from PyInstaller.utils.hooks import collect_data_files

hiddenimports = ["hpcflow.sdk.data", "click.testing"]

datas = collect_data_files("hpcflow.sdk.data")
datas += collect_data_files(
    "hpcflow.tests",
    include_py_files=True,
    excludes=("**/__pycache__",),
)
