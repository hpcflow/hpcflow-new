
<a name="v0.2.0a29"></a>
## [v0.2.0a29](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a28...v0.2.0a29) - 2023.04.25

### ♻ Code Refactoring

* split add_tassk_before_and_after into unit test

### ✨ Features

* added docstrings for add_task before and after
* defined add_task_after and add_task_before, with test

### 🐛 Bug Fixes

* default task index, new task variable name and pass.


<a name="v0.2.0a28"></a>
## [v0.2.0a28](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a27...v0.2.0a28) - 2023.04.20

### ✨ Features

* Update ubuntu runners for build docs

### 🐛 Bug Fixes

* Correct typo in Configure Poetry in release

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a27"></a>
## [v0.2.0a27](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a26...v0.2.0a27) - 2023.04.19


<a name="v0.2.0a26"></a>
## [v0.2.0a26](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a25...v0.2.0a26) - 2023.03.28

### 👷 Build changes

* Update relase actions workflow.
* Update build-exes workflow
* Update build-exes workflow
* Updated actions workflows
* Updated all actions workflows
* update binary download links file [skip ci]


<a name="v0.2.0a25"></a>
## [v0.2.0a25](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a24...v0.2.0a25) - 2023.03.22

### ♻ Code Refactoring

* removed InputSourceMode


<a name="v0.2.0a24"></a>
## [v0.2.0a24](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a23...v0.2.0a24) - 2023.03.06

### ✨ Features

* store element meta-data in zarr array

### 🐛 Bug Fixes

* add missing import to pyinstsaller hook
* Task.insert_ID is now unique!
* get_events_of_type now considers unsaved events

### 👷 Build changes

* merge develop
* merge develop
* update binary download links file [skip ci]
* merge develop
* merge from remote


<a name="v0.2.0a23"></a>
## [v0.2.0a23](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a22...v0.2.0a23) - 2023.02.09

### ✨ Features

* allow passing input files (fix [#345](https://github.com/hpcflow/hpcflow-new/issues/345)); also add rules to actions
* add Element.test_rule to test a Rule on an element's data

### 👷 Build changes

* bump deps + remove helper group
* update binary download links file [skip ci]


<a name="v0.2.0a22"></a>
## [v0.2.0a22](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a21...v0.2.0a22) - 2023.01.26

### ✨ Features

* example notebook

### 🐛 Bug Fixes

* add missing class to __init__
* fix getting correct group index when from a task input

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a21"></a>
## [v0.2.0a21](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a20...v0.2.0a21) - 2023.01.23

### ♻ Code Refactoring

* Moved helper dependencies to poetry.group.helper.dependencies

### 👷 Build changes

* poetry lock
* update binary download links file [skip ci]


<a name="v0.2.0a20"></a>
## [v0.2.0a20](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a19...v0.2.0a20) - 2023.01.20

### ♻ Code Refactoring

* remove duplicate core_classes
* improve get_available_task_input_sources
* improve InputSource repr
* remove unused
* remove unused
* remove unused Task arguments for now
* remove unused
* remove unused method

### ✨ Features

* add notebook to look at element dependency viz
* propagate new elements to downstream tasks
* Adds app name to --version option to cli help.
* add Workflow.copy method
* add sourceable_elements option to ElementSet
* track sequence indices for each element
* track element set for each element
* support passing a base element to add_elements
* support passing already-bound inputs, resources and sequences to add_elements
* track input sources for each new element
* add_elements
* raise on missing inputs

### 🐛 Bug Fixes

* add hidden import to pyinstaller
* decorator order matters for staticmethods!
* combining upstream parameter with locally defined sub-parameter
* track input sources for sub-parameters
* local values take precedence over other sources
* allow passing SchemaInput to TaskSchema outputs arg
* no need to specify 'name' in schema objective YAML
* BaseApp.shared_data_from_json_like to not use app_data

### 👷 Build changes

* merge
* update binary download links file [skip ci]


<a name="v0.2.0a19"></a>
## [v0.2.0a19](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a18...v0.2.0a19) - 2022.12.22

### ♻ Code Refactoring

* remove unused imports; sort

### ✨ Features

* move clear_helper to API
* add WorkflowMonitor.on_modified event to log
* more helper functions + use PollingObserver to work on networked file systems
* add watchdog to helper
* add timeout option to helper
* add clear command to helper CLI
* add pid command to helper CLI
* initial server implementation
* expose make/submit-workflow to CLI
* add Workflow.submit for local serial execution
* add demo-software CLI
* add scripts app data and first demo scripts
* validate inputs and sequence paths
* update SchemaInput repr
* add resources property to Element

### 🐛 Bug Fixes

* another py37 f-string
* py37 fstring incompat
* helper watch-list if not running
* delete watch file in stop_helper
* missing f-string
* merge branch 'aplowman/develop' into feat/server
* App._load_scripts for frozen app
* helper invocation on frozen
* ValueSequence cls methods
* quote script paths
* add missing WorkflowTask.dir_path prop
* InputFile
* ValueSequence repr
* EnvironmentList.get when no specifiers
* element resolution of downstream tasks
* Element.get with no path arg
* set unsourced inputs
* sequence path cannot start with output!
* add _validate_parameter_path to ValueSequence
* use dot-delimited string for Element.get() path arg

### 👷 Build changes

* merge
* update binary download links file [skip ci]
* merge branch 'aplowman/develop' into feat/server
* merge branch 'develop' into aplowman/develop
* add watchdog dep


<a name="v0.2.0a18"></a>
## [v0.2.0a18](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a17...v0.2.0a18) - 2022.12.12

### Other changes

* Add wf to main so appears in API
* Add wf to main so appears in API

### 🐛 Bug Fixes

* Update wf
* Make capitalisation consistent.
* Make capitalisation consistent.
* onedir output now linked correctly

### 👷 Build changes

* update binary download links file [skip ci]


<a name="v0.2.0a17"></a>
## [v0.2.0a17](https://github.com/hpcflow/hpcflow-new/compare/v0.0.1...v0.2.0a17) - 2022.12.09

### Other changes

* tweak powershell compress
* test bash compress
* Fix filenames in release testing
* correct error in release_testing
* Update permissions on compress script
* fix filenames in release_testing
* Change to compress script (bash)
* re-add release_testing.yaml
* Update permissions on compress (bash)
* remove extra space
* Remove test workflows.
* rectify conflicting action ids
* test exisitng *win-dir.zip
* add renamed archive to release
* change compressed filename
* Test separate compress script
* tweaking if statement again
* tweak if statement
* fix if statement
* test zip win-dir right after build
* test zip on macOS
* tweak zip command
* remove extra space
* test zip win-dir before release
* test download to action root
* test macOS-dir in isolation
* Try download win-dir only
* Try artefact path beginning ./
* fix path to artifacts
* test specify full path to artefacts
* test softprops/action-gh-release[@v0](https://github.com/v0).1.13
* fix artefact names in relese step
* Add workflow to test GH releasing

### ♻ Code Refactoring

* add App.assign_core_classes
* remove unused import
* Remove multiprocessing.freeze_support

### ✨ Features

* change package name temporarily
* add optional _json_like_constructor method
* add testing to API and CLI
* initial pass of zarr integration
* support ChildObjectSpec within dict values
* JSONLike superclass
* add python_version to run time info and Sentry tag
* add sentry tracing
* improve configuration feature
* pass version to RunTimeInfo
* add --run-time-info option to CLI
* add logging
* test
* **gha:** Add pyinstaller onedir to release

### 🐛 Bug Fixes

* pyinstaller build scripts
* Correct typo in linux onedir upload name
* Correct typo in release build windows, file
* Remove .zip from onedir filenames
* Correct typo in Version check (win, folder).
* Correct typo in Version check (win, folder).
* Correct erroneous version number.
* Remove erroneously updated changelog.
* Remove erroneously updated changelog.
* Fix typo in release workflow template
* Fix incorrect folder for pyinst onefolder
* workflow dep
* Correct typos in make script
* Correct typo in build-exes workflow
* Fix typo in build-exes template & workflow
* show config dir metadata on ConfigValidationError
* Fix typo in pyinstaller/make.sh
* Change onefolder to onefir in makefiles
* Change onefolder to onedir in makefiles
* invoke tests from non-frozen
* Fix name of release_testing wf.
* scoping issue when adding multiple API methods to the BaseApp
* init of InputValue with array value
* retrieval of Task inputs once workflow-bound
* GHA git perms
* scripts
* more unit tests
* failing tests
* to_json_like round trip test with parent ref
* actions workflow
* more config fixes
* update config valida schema
* default config
* add pyinstaller hidden imports
* test config CLI
* better config
* overriding log file from CLI
* **build:** GHA poetry install commands
* **gha:** Correct typo in build wf file & template
* **gha:** Fix typo in build-exes wf temp & file

### 👷 Build changes

* repeat release
* Add compress script (bash)
* Add compress script (powershell)
* update binary download links file [skip ci]
* update binary download links file [skip ci]
* Update pyinstaller make scripts
* slightly increase black line-length
* update poetry lock
* add scripts for sphinx-apidoc
* add auto-gen requirements
* pre-commits
* poetry update again
* Update make scripts.
* remove unused ci pre-commit
* update binary download links file [skip ci]
* merge
* Update make scripts
* clean before pyinstaller run
* update binary download links file [skip ci]
* merge
* Add sep folder for onefile, onefolder build
* Add onefile build to ipyinstaller makefiles
* update binary download links file [skip ci]
* update deps
* revert "bump: 0.2.0a2 → 0.2.0a3 [skip ci]"
* update binary download links file [skip ci]
* fix entrypoint
* update binary download links file [skip ci]
* fix pyinstaller entry in actions
* merge develop
* fix pyinstaller scripts
* revert "bump: 0.2.0a2 → 0.2.0a3 [skip ci]"
* move tests to unit dir
* add pyi hidden import
* deps
* update binary download links file [skip ci]
* update binary download links file [skip ci]
* add empty init py in tests
* update binary download links file [skip ci]
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* add back sdk data hiddenimport!
* merge develop
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* add empty init file back to sdk data
* try to fix mac problem when using SDK
* revert "bump: 0.2.0a9 → 0.2.0a10 [skip ci]"
* remove unused 'include'
* add test extra
* use poetry dep groups
* update binary download links file [skip ci]
* update deps
* allow tests to run frozen
* merge develop
* update binary download links file [skip ci]
* Fix failing test workflow on fork
* try pyinstaller fix
* merge from develop
* update binary download links file [skip ci]
* try add hidden import
* merge
* update poetry
* update binary download links file [skip ci]
* update GHA workflows
* add missing data files
* update GH Actions workflows
* update binary download links file [skip ci]
* merge
* merge from develop
* fix pyinstaller build on MacOS
* more pyinstaller tweaks
* update pyinstaller hooks
* update poetry pre-commit
* update GH workflows
* update gitignore
* merge
* update deps
* update binary download links file [skip ci]
* workflows
* add pyinstaller log level actions input
* use pyinstaller collect_data_files
* chmod+x pyinstaller make
* use pyinstaller hook
* merge
* update binary download links file [skip ci]
* CI issue https://github.com/psf/black/issues/2964
* update GH workflow cache keys
* remove debug print
* test on docker image as well
* try add tkinter import 2
* try add hidden import again
* updated poetry lock
* add hidden import for linux pyinstaller
* **GHA:** Update workflow templates and files
* **GHA:** update os vers
* **GHA:** remove pytest module restriction
* **GHA:** run tests on frozen app
* **GHA:** update py vers
* **GHA:** template updates
* **GHA:** don't run test on push
* **GHA:** Update build workflow template & file
* **GHA:** Update workflow templates
* **GHA:** Update workflow YAML files
* **gha:** Update build workflow file & template
* **gha:** Update release & build-exes workflows.
* **gha:** Compress onedir output for release
* **gha:** Update build-exes template & workflow
* **gha:** Update release & build-exes workflows
* **gha:** Update build-exes wf template and file
* **gha:** Update build-exes wf template and file
* **gha:** Update release wf with pyinst ondir
* **gha:** Update pyinstaller make files
* **gha:** Update release & build-exes workflows
* **gha:** Update build-exes wf template & file
* **pyi:** fix custom hook
* **pyinstaller:** Update pyinstaller make files
* **workflow:** Fix failing test wokflow on fork


<a name="v0.0.1"></a>
## [v0.0.1](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a16...v0.0.1) - 2022.12.09

### Other changes

* add workflow to main to enable running


<a name="v0.2.0a16"></a>
## [v0.2.0a16](https://github.com/hpcflow/hpcflow-new/compare/v0.2.0a15...v0.2.0a16) - 2022.11.28

### ✨ Features

* **gha:** Add pyinstaller onedir to release

### 🐛 Bug Fixes

* Correct typo in linux onedir upload name
* Correct typo in release build windows, file
* Remove .zip from onedir filenames
* Correct typo in Version check (win, folder).
* Correct typo in Version check (win, folder).
* Correct erroneous version number.
* Remove erroneously updated changelog.
* Remove erroneously updated changelog.
* Fix typo in release workflow template
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

* Update make scripts.
* Add onefile build to ipyinstaller makefiles
* Update pyinstaller make scripts
* Add sep folder for onefile, onefolder build
* Update make scripts
* update binary download links file [skip ci]
* update binary download links file [skip ci]
* **GHA:** Update build workflow template & file
* **GHA:** Update workflow templates and files
* **gha:** Update build-exes wf template and file
* **gha:** Update release & build-exes workflows
* **gha:** Update build-exes wf template & file
* **gha:** Update build-exes template & workflow
* **gha:** Update release wf with pyinst ondir
* **gha:** Update build workflow file & template
* **gha:** Update pyinstaller make files
* **gha:** Update build-exes wf template and file
* **gha:** Update release & build-exes workflows.
* **gha:** Update release & build-exes workflows
* **pyinstaller:** Update pyinstaller make files


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

