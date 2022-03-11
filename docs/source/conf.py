# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))

from hpcflow._version import __version__

# -- Project information -----------------------------------------------------

project = "hpcflow"
copyright = "2022, hpcflow developers"
author = "hpcflow developers"

# The full version, including alpha/beta/rc tags
release = __version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon"]

autodoc_typehints = "description"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "logo_link": "https://hpcflow.github.io",
    "github_url": "https://github.com/hpcflow/hpcflow-new",
    "external_links": [
        {"name": "Install", "url": "https://hpcflow.github.io/install"},
        {"name": "Documentation", "url": "https://hpcflow.github.io/docs/stable"},
        {"name": "Contribute", "url": "https://hpcflow.github.io/contribute"},
    ],
    "switcher": {
        "json_url": "https://hpcflow.github.io/docs/switcher.json",
        "url_template": "https://hpcflow.github.io/docs/{version}/",
        "version_match": __version__,
    },
    "navbar_end": ["version-switcher"],
    "use_edit_page_button": True,
}

html_context = {
    "github_user": "hpcflow",
    "github_repo": "hpcflow-new",
    "github_version": "develop",
    "doc_path": "docs",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

text_newlines = "unix"
