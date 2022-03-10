python3 -c "
from pathlib import Path;
for i in Path('release-artifacts').glob('*/*'):
  print(str(i));
"
