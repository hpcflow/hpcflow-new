:orphan:

.. _contribute:

##########
Contribute
##########

.. raw:: html
    
   <table><tr><td>

   # Reccomended reads
   - Conventional commits [link](https://www.conventionalcommits.org/en/v1.0.0/)
   - poetry ([docs](https://python-poetry.org/docs/))
   - click ([docs](https://click.palletsprojects.com/en/8.1.x/))
   - xxx ([docs]())

   # Installation for development

   ## Dependencies
   sudo apt-get install python3-tk

   ## Install poetry
   Update your system and ge the latest version of poetry with
   ```
   sudo apt update
   sudo apt upgrade
   curl -sSL https://install.python-poetry.org | python3 -
   ```
   It will prompt you to add poetry to the PATH, which you can do with
   ```
   nano ~/.bashrc
   ```
   and pasting this at the end of the file:
   ```
   export PATH=${HOME}/.local/bin:$PATH
   ```
   Now you should be able to run this command:
   ```
   poetry --version
   ```
   And while you are at it, try and auto update everything with
   ```
   poetry self update
   ```


   ## Clone repo
   Clone the git repo (see ssh links below), and then make sure that you checkout to the development branch
   ```
   git checkout develop
   ```
   This branch is protected, so create a feature branch before pushing to the repo.
   ### hpcflow
   ```
   git clone git@github.com:hpcflow/hpcflow-new.git
   ```
   ### matflow
   ```
   git clone git@github.com:hpcflow/matflow-new.git
   ```

   ## Install python dependencies with poetry
   You should be able to simply run
   ```
   poetry install
   ```
   from the hpc-flow folder and poetry will take care of everything for you.

   ### Troubleshooting
   If for some reason the install failed, try the following:

   Delete the virtualenv. You first need to find the version you are using with
   ```
   poetry env info
   ```
   then use it to remove the virtual environment with
   ```
   poetry env remove 3.10.6
   ```
   Delete the powtry.lock file
   ```
   rm poetry.lock
   ```
   Clear all cache in list with
   ```
   poetry cache clear --all PyPI
   ```
   Check if there is no chache with
   ```
   poetry cache list
   ```
   Create a new virtualenv
   ```
   poetry env use 3.10.6
   ```
   Reinstall dependencies without the lockfile
   ```
   poetry install
   ```

   # Working from the source
   Open the virtual enviroment with
   ```
   poetry shell
   ```
   ## hpcflow
   ### CLI
   You can interact with the CLI by calling
   ```
   python3 hpcflow/cli/cli.py --help
   ```

   ## matflow
   ### link to local hpcflow
   To be able to work with hpcflow and immediately see the changes reflected in matflow you need to reconfigure the hpcflow dependency to point to your local copy of hpcflow. 
   To do this, run
   ```
   poetry add --editable ${HOME}/hpcflow-new/
   ```
   This will update the hpcflow-new dependency to point to your local copy.

   If this does not work, try doing it manually:
   First modify the matflow-new/pyproject.toml file replacing
   ```
   hpcflow-new2 = "^0.2.0a14"
   ```
   with
   ```
   hpcflow-new2 = {path = "${HOME}/hpcflow-new", develop = true}
   ```
   Then, you need to update your poetry environment accordingly, by first removing the lock file
   ```
   rm poetry.lock
   ```
   and then re-running the dependency installation
   ```
   poetry install
   ```
   ### CLI
   You can interact with the CLI by calling
   ```
   python3 matflow/cli.py --help
   ```

   </td></tr></table>
