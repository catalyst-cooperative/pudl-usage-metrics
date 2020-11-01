#!/usr/bin/env python
from setuptools import find_packages, setup

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

setup(
    name="bizops",
    version="0.1.0",
    author="Zane Selvans",
    author_email="zane.selvans@gmail.com",
    packages=find_packages("src/bizops"),
    package_dir={"": "src"},
    install_requires=install_requires,
    scripts=["scripts/harvest_to_gspread.py"],
)
