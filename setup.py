#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-cursor",
    version="0.1.0",
    description="Singer.io tap for extracting data from the Cursor API",
    author="llemanh",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap-cursor"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.29.0",
        "urllib3==1.26.20",
        "backoff==1.8.0",
    ],
    extras_require={"dev": ["pylint==2.6.2", "ipdb", "nose", "requests-mock==1.9.3"]},
    entry_points="""
          [console_scripts]
          tap-cursor=tap_cursor:main
      """,
    packages=["tap_cursor"],
    package_data={"tap_cursor": ["schemas/*.json"]},
    include_package_data=True,
)
