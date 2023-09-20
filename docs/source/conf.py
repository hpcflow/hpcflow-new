# Configuration file for the Sphinx documentation builder.

# -------- app-specific content START ----------------------------------------------------

from hpcflow import __version__
from hpcflow.app import app

project = "hpcflow"
copyright = "2023, hpcflow developers"
author = "hpcflow developers"
release = __version__

github_user = "hpcflow"
github_repo = "hpcflow-new"
PyPI_project = "hpcflow-new2"

switcher_JSON_URL = "https://hpcflow.github.io/docs/switcher.json"

html_logo = "_static/images/logo-v2.png"

additional_intersphinx = {}

# -------- app-specific content END ------------------------------------------------------

from config_common import *
