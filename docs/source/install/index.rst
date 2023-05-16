
:orphan:

.. _install:

############
Installation
############

Release notes: `on GitHub <https://github.com/hpcflow/hpcflow-new/releases/tag/v0.2.0a38>`_

There are two ways of using HPCFlow:
 * HPCFlow CLI (Command Line Interface)
 * The HPCFlow Python package

Both of these options allow workflows to be designed and executed. HPCFlow CLI
is recommended for beginners and strongly recommended if you want to 
run HPCFlow on a cluster. The HPCFlow Python package allows workflows to be
designed and explored via the Python API and is recommended for users 
comfortable working with Python. If you are interested in contributing to 
the development of HPCFlow, the Python package is the place to start.

HPCFlow CLI and the HPCFlow Python package can both be used simultaneously.

*******************************
HPCFlow CLI
*******************************

Install script (v0.2.0a38)
============================

HPCFlow CLI can be installed on macOS, Linux or Windows through a terminal
or shell prompt.

**macOS:** Open a terminal, paste the command shown below and press enter.

.. code-block:: bash

    (touch tmp.sh && curl -fsSL https://raw.githubusercontent.com/hpcflow/install-scripts/main/src/install-hpcflow.sh > tmp.sh && bash tmp.sh --prerelease --path --onefile) ; rm tmp.sh

**Linux:** Open a shell prompt, paste the command shown below and press enter.

.. code-block:: bash

    (touch tmp.sh && curl -fsSL https://raw.githubusercontent.com/hpcflow/install-scripts/main/src/install-hpcflow.sh > tmp.sh && bash tmp.sh --prerelease --path --onefile) ; rm tmp.sh

Note that if you're installing on CSF3 or CSF4 using this method, the proxy
module should be loaded first. To do this, paste the command shown below
into a the shell prompt and press enter.

.. code-block:: bash

    module load tools/env/proxy2

**Windows:** Open a Powershell terminal, paste the command shown below and 
press enter.

.. code-block:: bash

    & $([scriptblock]::Create((New-Object Net.WebClient).DownloadString('https://raw.githubusercontent.com/hpcflow/install-scripts/main/src/install-hpcflow.ps1'))) -PreRelease -OneFile

Download binaries (v0.2.0a38)
===============================

Binaries are available in two formats:

  * A single executable file containing everything.
  * A folder containing an executable and supporting files.

Click below to download the HPCFlow binary for your platform (other binary releases are available by using the version switcher in the top-right corner):

.. raw:: html

   <table class="binary-downloads-table">
     <tr><td>Linux executable</td><td><a href="https://github.com/hpcflow/hpcflow-new/releases/download/v0.2.0a38/hpcflow-v0.2.0a38-linux">hpcflow-v0.2.0a38-linux</a></td></tr>
     <tr><td>Linux folder</td><td><a href="https://github.com/hpcflow/hpcflow-new/releases/download/v0.2.0a38/hpcflow-v0.2.0a38-linux-dir.zip">hpcflow-v0.2.0a38-linux-dir.zip</a></td></tr>
     <tr><td>macOS executable</td><td><a href="https://github.com/hpcflow/hpcflow-new/releases/download/v0.2.0a38/hpcflow-v0.2.0a38-macOS">hpcflow-v0.2.0a38-macOS</a></td></tr>
     <tr><td>macOS folder</td><td><a href="https://github.com/hpcflow/hpcflow-new/releases/download/v0.2.0a38/hpcflow-v0.2.0a38-macOS-dir.zip">hpcflow-v0.2.0a38-macOS-dir.zip</a></td></tr>
     <tr><td>Windows folder</td><td><a href="https://github.com/hpcflow/hpcflow-new/releases/download/v0.2.0a38/hpcflow-v0.2.0a38-win-dir.zip">hpcflow-v0.2.0a38-win-dir.zip</a></td></tr>
     <tr><td>Windows executable</td><td><a href="https://github.com/hpcflow/hpcflow-new/releases/download/v0.2.0a38/hpcflow-v0.2.0a38-win.exe">hpcflow-v0.2.0a38-win.exe</a></td></tr>
   </table>

**************************
The HPCFlow Python package
**************************

Using pip
==========================

Use pip to install the Python package from PyPI::

  pip install hpcflow==0.2.0a38
