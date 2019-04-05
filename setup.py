#!/usr/bin/env python

from setuptools import setup

setup(
    setup_requires=['pbr'],
    pbr=True,
    py_modules=['dulwich_tree', ],
    include_package_data=True,
    test_suite='test_dulwich_tree',
)
