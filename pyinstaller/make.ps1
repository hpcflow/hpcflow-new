# Make sure we ran one of these:
#   `poetry install`
#   `poetry install --without dev` (for building with pyinstaller and pytest)
#   `poetry install --without dev,test` (for building with pyinstaller)
# 
# Might need to disable desktop cloud sync. engines during this!
# 
param($ExeName = "hpcflow", $LogLevel = "INFO")
poetry run pyinstaller --log-level=$LogLevel --onefile --clean -y --name=$ExeName ..\hpcflow\cli\cli.py
poetry run pyinstaller --log-level=$LogLevel --onefolder --clean -y --name=$ExeName ..\hpcflow\cli\cli.py
