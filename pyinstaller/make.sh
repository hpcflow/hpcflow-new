# Make sure we ran `poetry install --extras "pyinstaller"` (or `poetry install --no-dev --extras "pyinstaller"`)
# Might need to disable cloud sync. engines during this
EXE_NAME_DEFAULT="hpcflow"
LOG_LEVEL_DEFAULT="INFO"
EXE_NAME="${1:-$EXE_NAME_DEFAULT}"
LOG_LEVEL="${2:-$LOG_LEVEL_DEFAULT}"
poetry run pyinstaller --log-level=$LOG_LEVEL --onefile --clean -y --name=$EXE_NAME ../hpcflow/cli/cli.py
