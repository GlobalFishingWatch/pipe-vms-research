#!/usr/bin/env python

"""
Setup script for pipe-vms-research
"""
from setuptools import find_packages, setup

setup(
    name='pipe-vms-research',
    version='4.2.0',
    packages=find_packages(exclude=['test*.*', 'tests']),
    include_package_data=True
)

