#!/usr/bin/env python
"""Setup script to make usage_metrics installable with pip."""

from setuptools import find_packages, setup

setup(
    name="usage_metrics",
    description="A package for processing PUDl usage metrics using Dagster.",
    # setuptools_scm lets us automagically get package version from GitHub tags
    setup_requires=["setuptools_scm"],
    # .git is in the directory above
    use_scm_version=True,
    author="Catalyst Cooperative",
    author_email="pudl@catalyst.coop",
    maintainer="Bennett Norman",
    maintainer_email="bennett.norman@catalyst.coop",
    url="",  # Can be repo or docs URL if no separate web page exists.
    project_urls={
        "Source": "https://github.com/catalyst-cooperative/business/user_metrics",
        "Issue Tracker": "https://github.com/catalyst-cooperative/business/issues",
    },
    license="MIT",
    # Fill in search keywords that users might use to find the package
    keywords=[],
    python_requires=">=3.8,<3.11",
    # In order for the dependabot to update versions, they must be listed here.
    # Use the format package_name>=x,<y", Included packages are just examples:
    install_requires=[
        "pandas>=1.4,<1.5",
        "sqlalchemy>=1.4,<2",
        "dagster==0.14.9",
        "dagit==0.14.20",
        "dagster-pandera==0.14.9",
        "pandas-gbq~=0.17.0",
        "pydata-google-auth>=1.3,<1.5",
        "jupyterlab>=3.2.8,<3.5.0",
        "psycopg2~=2.9.3",
        "ipinfo~=4.2.1",
        "joblib~=1.1.0",
        "matplotlib~=3.5.1",
        "pytest~=7.1.1",
    ],
    extras_require={
        "dev": [
            "black>=22,<23",  # A deterministic code formatter
            "isort>=5,<6",  # Standardized import sorting
            "twine>=3.3,<5.0",  # Used to make releases to PyPI
            "tox>=3.20,<4",  # Python test environment manager
        ],
        "docs": [
            "doc8>=0.9,<0.12",  # Ensures clean documentation formatting
            "sphinx>=4,<5",  # The default Python documentation redering engine
            "sphinx-autoapi>=1.8,<2",  # Generates documentation from docstrings
            "sphinx-issues>=1.2,<4.0",  # Allows references to GitHub issues
            "sphinx-rtd-dark-mode>=1.2,<2",  # Allow user to toggle light/dark mode
            "sphinx-rtd-theme>=1,<2",  # Standard Sphinx theme for Read The Docs
        ],
        "tests": [
            "bandit>=1.6,<2",  # Checks code for security issues
            "coverage>=5.3,<7",  # Lets us track what code is being tested
            "doc8>=0.9,<0.12",  # Ensures clean documentation formatting
            "flake8>=4,<5",  # A framework for linting & static analysis
            "flake8-builtins>=1.5,<2",  # Avoid shadowing Python built-in names
            "flake8-colors>=0.1,<0.2",  # Produce colorful error / warning output
            "flake8-docstrings>=1.5,<2",  # Ensure docstrings are formatted well
            "flake8-rst-docstrings>=0.2,<0.3",  # Allow use of ReST in docstrings
            "flake8-use-fstring>=1,<2",  # Highlight use of old-style string formatting
            "mccabe>=0.6,<0.8",  # Checks that code isn't overly complicated
            "pep8-naming>=0.12,<0.13",  # Require PEP8 compliant variable names
            "pre-commit>=2.9,<3",  # Allow us to run pre-commit hooks in testing
            "pydocstyle>=5.1,<7",  # Style guidelines for Python documentation
            "pytest>=6.2,<8",  # Our testing framework
            "pytest-console-scripts>=1.1,<2",  # Allow automatic testing of scripts
            "pytest-cov>=2.10,<4.0",  # Pytest plugin for working with coverage
            "tox>=3.20,<4",  # Python test environment manager
        ],
    },
    # A controlled vocabulary of tags used by the Python Package Index.
    # Make sure the license and python versions are consistent with other arguments.
    # The full list of recognized classifiers is here: https://pypi.org/classifiers/
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    # Directory to search recursively for __init__.py files defining Python packages
    packages=find_packages("src"),
    # Location of the "root" package:
    package_dir={"": "src"},
    # package_data is data that is deployed within the python package on the
    # user's system. setuptools will get whatever is listed in MANIFEST.in
    include_package_data=True,
    # entry_points defines interfaces to command line scripts we distribute.
    # Can also be used for other resource deployments, like intake catalogs.
    # entry_points={
    #     "console_scripts": [
    #         "script_name = dotted.module.path.to:main_script_function",
    #     ]
    # },
)
