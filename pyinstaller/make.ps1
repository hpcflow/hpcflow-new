# Make sure we ran `poetry install --extras "pyinstaller"` (or `poetry install --no-dev --extras "pyinstaller"`)
# Might need to disable cloud sync. engines during this
param($ExeName="hpcflow")
poetry run pyinstaller --clean -y --onefile --name=$ExeName ..\hpcflow\cli\cli.py
