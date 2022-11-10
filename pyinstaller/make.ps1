# Make sure we ran one of these:
#   `poetry install`
#   `poetry install --without dev` (for building with pyinstaller and pytest)
#   `poetry install --without dev,test` (for building with pyinstaller)
# 
# Might need to disable desktop cloud sync. engines during this!
# 
param($ExeName = "hpcflow", $LogLevel = "INFO", $BuildType = 'onefile')
if ($BuildType -eq 'onefile')
{
    poetry run pyinstaller --log-level=$LogLevel --distpath ./dist/onefile --onefile --clean -y --name=$ExeName ..\hpcflow\cli\cli.py
}
elseif ($BuildType -eq 'onefolder')
{
    poetry run pyinstaller --log-level=$LogLevel --distpath ./dist/onedir --onedir --clean -y --name=$ExeName ..\hpcflow\cli\cli.py
}
else
{
    Write-Warning "Build type $BuildType unknown. Specify either onefile or onefolder"
}

