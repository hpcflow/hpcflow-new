# Make sure we ran `poetry install --extras "pyinstaller"` (or `poetry install --no-dev --extras "pyinstaller"`)
# Might need to disable cloud sync. engines during this
EXE_NAME_DEFAULT="hpcflow"
EXE_NAME="${1:-$EXE_NAME_DEFAULT}"
poetry run pyinstaller --clean -y --onefile --name=$EXE_NAME ../hpcflow/cli/cli.py
