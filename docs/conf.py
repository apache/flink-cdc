# Copyright 2022 Ververica Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Maven
# Build your Java project and run tests with Apache Maven.
# Add steps that analyze code, save build artifacts, deploy, and more:
# https://docs.microsoft.com/azure/devops/pipelines/languages/java

# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------
project = 'CDC Connectors for Apache Flink®'
copyright = '2022, Ververica GmbH; Apache Flink, Flink®, Apache®, the squirrel logo, and the Apache feather logo are either registered trademarks or trademarks of The Apache Software Foundation'
author = 'ververica'


# -- General configuration ---------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx_rtd_theme',
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.githubpages',
    'myst_parser',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
html_favicon = '_static/fig/favicon.png'

import myst_parser

source_parsers = {
    '.md': myst_parser
}
source_suffix = ['.md']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_context = {
    'css_files': [
        '_static/theme_overrides.css',  # overrides for wide tables in RTD theme
    ],
}

try:
    html_context
except NameError:
    html_context = dict()
html_context['display_lower_left'] = True

if 'REPO_NAME' in os.environ:
    REPO_NAME = os.environ['REPO_NAME']
else:
    REPO_NAME = ''

from git import Repo
repo = Repo( search_parent_directories=True )
remote_refs = repo.remote().refs

if 'current_version' in os.environ:
    current_version = os.environ['current_version']
else:
    current_version = repo.active_branch.name

html_context['current_version'] = current_version
html_context['version'] = current_version
html_context['github_version'] = current_version

html_context['versions'] = list()
branches = [branch.name for branch in remote_refs]
for branch in branches:
    if 'origin/' in branch and ('master' in branch or 'release-' in branch)\
            and 'HEAD' not in branch and 'gh-pages' not in branch \
            and 'release-1.0' not in branch and 'release-1.1' not in branch\
            and 'release-1.2' not in branch and 'release-1.3' not in branch:
        version = branch[7:]
        html_context['versions'].append( (version, '/' +REPO_NAME+ '/' +version+ '/') )

html_context['display_github'] = True
html_context['github_user'] = 'ververica'
html_context['github_repo'] = 'flink-cdc-connectors'
