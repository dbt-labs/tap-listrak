#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-listrak",
    version="0.1.0",
    description="Singer.io tap for extracting data from the Jira API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_listrak"],
    install_requires=[
        "singer-python>=3.2.0",
        "requests",
        "zeep",
    ],
    entry_points="""
    [console_scripts]
    tap-listrak=tap_listrak:main
    """,
    packages=["tap_listrak"],
    package_data = {
        "schemas": ["tap_listrak/schemas/*.json"]
    },
    include_package_data=True,
)
