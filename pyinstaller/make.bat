:: Might need to disable cloud sync. engines during this
:: Make sure we ran `poetry install --extras=pyinstaller` (or `poetry install --no-dev --extras "pyinstaller"`)
rmdir /s "build"
rmdir /s "dist"
del hpcflow.spec
poetry run pyinstaller --name=hpcflow --onefile ../hpcflow/cli.py 
