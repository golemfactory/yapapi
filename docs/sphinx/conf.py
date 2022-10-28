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
import os
import sys

sys.path.insert(0, os.path.abspath("../../"))

from yapapi import get_version

# -- Project information -----------------------------------------------------

project = "yapapi"
copyright = "2020-2021, Golem Factory"
author = "Golem Factory"

# The full version, including alpha/beta/rc tags
release = get_version()


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named "sphinx.ext.*") or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx_autodoc_typehints",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []

#   This removes the `yapapi.log` docstrings.
#   There are two reasons:
#   *   there are some sections/subsections declared there and it messes up the docs
#   *   it's obsolete, because encourages usage of Executor
#   Solution from https://stackoverflow.com/a/18031024/15851655
def remove_module_docstring(app, what, name, obj, options, lines):
    if what == "module" and name == "yapapi.log":
        del lines[:]


def setup(app):
    app.connect("autodoc-process-docstring", remove_module_docstring)


autodoc_member_order = "bysource"
