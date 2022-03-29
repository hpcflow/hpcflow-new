# Make sure we ran `poetry install --extras=pyinstaller` (or `poetry install --no-dev --extras "pyinstaller"`)
rm hpcflow.spec
poetry run pyinstaller --clean -y --onefile --name=hpcflow ../hpcflow/cli/cli.py
