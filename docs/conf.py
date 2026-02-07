"""Sphinx configuration for pyspark-pipeline-framework documentation."""

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "pyspark-pipeline-framework"
copyright = "2026, Dustin Smith"
author = "Dustin Smith"
version = "0.1.0"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",  # Copy button for code blocks
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Furo theme configuration ------------------------------------------------
html_theme = "furo"

html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": "#2962ff",  # Blue accent
        "color-brand-content": "#2962ff",
    },
    "dark_css_variables": {
        "color-brand-primary": "#82b1ff",
        "color-brand-content": "#82b1ff",
    },
    "sidebar_hide_name": False,
    "navigation_with_keys": True,
}

html_title = "PySpark Pipeline Framework"

# -- Autodoc configuration ---------------------------------------------------
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}
autodoc_member_order = "bysource"
autodoc_typehints = "description"

# -- Napoleon configuration --------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True

# -- Intersphinx configuration -----------------------------------------------
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# -- Copy button configuration -----------------------------------------------
copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True
