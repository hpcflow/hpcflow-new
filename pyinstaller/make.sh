# Make sure we ran `poetry install --extras=pyinstaller` (or `poetry install --no-dev --extras "pyinstaller"`)
poetry run pyinstaller --name=hpcflow --onefile ../hpcflow/cli.py 
