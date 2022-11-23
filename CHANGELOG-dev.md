
<a name="v0.2.0a16"></a>
## [v0.2.0a16](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a15...v0.2.0a16) - 2022.11.17

### 🐛 Bug Fixes

* Fix incorrect folder for pyinst onefolder
* Change onefolder to onefir in makefiles
* Correct typos in make script
* Correct typo in build-exes workflow
* Fix typo in build-exes template & workflow
* Fix typo in pyinstaller/make.sh
* Change onefolder to onedir in makefiles
* **gha:** Correct typo in build wf file & template
* **gha:** Fix typo in build-exes wf temp & file

### 👷 Build changes

* Update make scripts
* Add onefile build to ipyinstaller makefiles
* Update make scripts.
* Add sep folder for onefile, onefolder build
* update binary download links file [skip ci]
* **GHA:** Update build workflow template & file
* **GHA:** Update workflow templates and files
* **gha:** Update build-exes template & workflow
* **gha:** Update build-exes wf template & file
* **gha:** Update build workflow file & template
* **gha:** Update build-exes wf template and file
* **gha:** Update build-exes wf template and file
* **gha:** Update release wf with pyinst ondir


<a name="v0.2.0a15"></a>
## [v0.2.0a15](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a14...v0.2.0a15) - 2022.11.14

### ♻ Code Refactoring

* add App.assign_core_classes

### ✨ Features

* add optional _json_like_constructor method

### 👷 Build changes

* update binary download links file [skip ci]
* **GHA:** Update workflow YAML files
* **GHA:** Update workflow templates


<a name="v0.2.0a14"></a>
## [v0.2.0a14](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a13...v0.2.0a14) - 2022.10.30

### 👷 Build changes

* update binary download links file [skip ci]
* **pyi:** fix custom hook


<a name="v0.2.0a13"></a>
## [v0.2.0a13](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a12...v0.2.0a13) - 2022.10.30

### 👷 Build changes

* deps
* update binary download links file [skip ci]
* add pyi hidden import
* **GHA:** update py vers
* **GHA:** run tests on frozen app
* **GHA:** remove pytest module restriction
* **GHA:** update os vers
* **GHA:** template updates


<a name="v0.2.0a12"></a>
## [v0.2.0a12](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a11...v0.2.0a12) - 2022.10.29

### 🐛 Bug Fixes

* invoke tests from non-frozen

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a11"></a>
## [v0.2.0a11](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a10...v0.2.0a11) - 2022.10.29

### 👷 Build changes

* add empty init py in tests
* update binary download links file [skip ci]


<a name="v0.2.0a10"></a>
## [v0.2.0a10](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a9...v0.2.0a10) - 2022.10.29

### 🐛 Bug Fixes

* **build:** GHA poetry install commands

### 👷 Build changes

* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* add back sdk data hiddenimport!
* merge develop
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* add empty init file back to sdk data
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* remove unused 'include'
* add test extra
* use poetry dep groups
* update binary download links file [skip ci]
* **GHA:** don't run test on push


<a name="v0.2.0a9"></a>
## [v0.2.0a9](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a8...v0.2.0a9) - 2022.10.29

### ♻ Code Refactoring

* remove unused import

### ✨ Features

* add testing to API and CLI

### 🐛 Bug Fixes

* scoping issue when adding multiple API methods to the BaseApp
* init of InputValue with array value
* retrieval of Task inputs once workflow-bound

### 👷 Build changes

* update deps
* allow tests to run frozen
* merge develop
* update binary download links file [skip ci]
* merge from develop


<a name="v0.2.0a8"></a>
## [v0.2.0a8](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a7...v0.2.0a8) - 2022.10.25

### ♻ Code Refactoring

* Remove multiprocessing.freeze_support

### 👷 Build changes

* Fix failing test workflow on fork
* update binary download links file [skip ci]
* **workflow:** Fix failing test wokflow on fork


<a name="v0.2.0a7"></a>
## [v0.2.0a7](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a6...v0.2.0a7) - 2022.10.19

### 🐛 Bug Fixes

* GHA git perms

### 👷 Build changes

* try to fix mac problem when using SDK
* merge
* update poetry
* update binary download links file [skip ci]


<a name="v0.2.0a6"></a>
## [v0.2.0a6](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a5...v0.2.0a6) - 2022.10.19

### 👷 Build changes

* update GHA workflows
* add missing data files
* update GH Actions workflows
* update binary download links file [skip ci]


<a name="v0.2.0a5"></a>
## [v0.2.0a5](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a4...v0.2.0a5) - 2022.10.19

### ✨ Features

* initial pass of zarr integration
* support ChildObjectSpec within dict values
* JSONLike superclass
* add python_version to run time info and Sentry tag

### 🐛 Bug Fixes

* scripts
* more unit tests
* failing tests
* to_json_like round trip test with parent ref

### 👷 Build changes

* merge
* merge from develop
* fix pyinstaller build on MacOS
* more pyinstaller tweaks
* update pyinstaller hooks
* update poetry pre-commit
* updated poetry lock
* update gitignore
* merge
* update deps
* update binary download links file [skip ci]


<a name="v0.2.0a4"></a>
## [v0.2.0a4](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a3...v0.2.0a4) - 2022.03.30

### 🐛 Bug Fixes

* actions workflow
* more config fixes
* update config valida schema
* default config
* add pyinstaller hidden imports
* test config CLI

### 👷 Build changes

* workflows
* add pyinstaller log level actions input
* use pyinstaller collect_data_files
* chmod+x pyinstaller make
* use pyinstaller hook
* merge
* update binary download links file [skip ci]


<a name="v0.2.0a3"></a>
## [v0.2.0a3](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a2...v0.2.0a3) - 2022.03.29

### ✨ Features

* add sentry tracing
* improve configuration feature
* pass version to RunTimeInfo

### 🐛 Bug Fixes

* better config
* overriding log file from CLI
* show config dir metadata on ConfigValidationError
* workflow dep
* pyinstaller build scripts

### 👷 Build changes

* CI issue https://github.com/psf/black/issues/2964
* update GH workflow cache keys
* remove debug print
* test on docker image as well
* try add tkinter import 2
* try add hidden import again
* try add hidden import
* add hidden import for linux pyinstaller
* update GH workflows
* try pyinstaller fix
* move tests to unit dir
* revert "bump: 0.2.0a2 → 0.2.0a3 [skip ci]"
* fix pyinstaller scripts
* merge develop
* fix pyinstaller entry in actions
* fix entrypoint
* revert "bump: 0.2.0a2 → 0.2.0a3 [skip ci]"
* update deps
* merge
* update binary download links file [skip ci]


<a name="v0.2.0a2"></a>
## [v0.2.0a2](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a1...v0.2.0a2) - 2022.03.21

### ✨ Features

* add --run-time-info option to CLI

### 👷 Build changes

* clean before pyinstaller run
* merge
* update binary download links file [skip ci]
* remove unused ci pre-commit


<a name="v0.2.0a1"></a>
## [v0.2.0a1](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a0...v0.2.0a1) - 2022.03.21

### ✨ Features

* add logging

### 👷 Build changes

* poetry update again
* pre-commits
* add auto-gen requirements
* add scripts for sphinx-apidoc
* update poetry lock
* slightly increase black line-length
* update binary download links file [skip ci]


<a name="v0.2.0a0"></a>
## [v0.2.0a0](https://github.com/hpcflow/hpcflow-new/compare/v0.1.16...v0.2.0a0) - 2022.03.18

### ✨ Features

* test
* change package name temporarily
* revert version

### 👷 Build changes

* repeat release
* initial setup


<a name="v0.1.16"></a>
## [v0.1.16](https://github.com/hpcflow/hpcflow-new/compare/v0.1.15...v0.1.16) - 2021.06.06


<a name="v0.1.15"></a>
## [v0.1.15](https://github.com/hpcflow/hpcflow-new/compare/v0.1.14...v0.1.15) - 2021.04.10


<a name="v0.1.14"></a>
## [v0.1.14](https://github.com/hpcflow/hpcflow-new/compare/v0.1.13...v0.1.14) - 2021.02.05


<a name="v0.1.13"></a>
## [v0.1.13](https://github.com/hpcflow/hpcflow-new/compare/v0.1.12...v0.1.13) - 2021.01.18


<a name="v0.1.12"></a>
## [v0.1.12](https://github.com/hpcflow/hpcflow-new/compare/v0.1.11...v0.1.12) - 2020.12.16


<a name="v0.1.11"></a>
## [v0.1.11](https://github.com/hpcflow/hpcflow-new/compare/v0.1.10...v0.1.11) - 2020.08.25


<a name="v0.1.10"></a>
## [v0.1.10](https://github.com/hpcflow/hpcflow-new/compare/v0.1.9...v0.1.10) - 2020.07.07


<a name="v0.1.9"></a>
## [v0.1.9](https://github.com/hpcflow/hpcflow-new/compare/v0.1.8...v0.1.9) - 2020.06.09


<a name="v0.1.8"></a>
## [v0.1.8](https://github.com/hpcflow/hpcflow-new/compare/v0.1.7...v0.1.8) - 2020.06.09


<a name="v0.1.7"></a>
## [v0.1.7](https://github.com/hpcflow/hpcflow-new/compare/v0.1.6...v0.1.7) - 2020.05.12


<a name="v0.1.6"></a>
## [v0.1.6](https://github.com/hpcflow/hpcflow-new/compare/v0.1.5...v0.1.6) - 2020.05.11


<a name="v0.1.5"></a>
## [v0.1.5](https://github.com/hpcflow/hpcflow-new/compare/v0.1.4...v0.1.5) - 2020.05.07


<a name="v0.1.4"></a>
## [v0.1.4](https://github.com/hpcflow/hpcflow-new/compare/v0.1.3...v0.1.4) - 2020.05.07


<a name="v0.1.3"></a>
## [v0.1.3](https://github.com/hpcflow/hpcflow-new/compare/v0.1.2...v0.1.3) - 2020.05.07


<a name="v0.1.2"></a>
## [v0.1.2](https://github.com/hpcflow/hpcflow-new/compare/v0.1.1...v0.1.2) - 2020.05.06


<a name="v0.1.1"></a>
## v0.1.1 - 2019.06.14

