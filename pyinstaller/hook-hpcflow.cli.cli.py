from PyInstaller.utils.hooks import collect_data_files

hiddenimports = ["hpcflow.data"]
datas = collect_data_files("hpcflow")
