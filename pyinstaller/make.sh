# Make sure we ran one of these:
#   `poetry install`
#   `poetry install --without dev` (for building with pyinstaller and pytest)
#   `poetry install --without dev,test` (for building with pyinstaller)
# 
# Might need to disable desktop cloud sync. engines during this!
# 
EXE_NAME_DEFAULT="hpcflow"
LOG_LEVEL_DEFAULT="INFO"
EXE_NAME="${1:-$EXE_NAME_DEFAULT}"
LOG_LEVEL="${2:-$LOG_LEVEL_DEFAULT}"
poetry run pyinstaller --log-level=$LOG_LEVEL --distpath ./dist/onefile --onefile --clean -y --name=$EXE_NAME ../hpcflow/cli/cli.py
poetry run pyinstaller --log-level=$LOG_LEVEL --onefolder ./dist/onefolder --clean -y --name=$EXE_NAME ../hpcflow/cli/cli.py