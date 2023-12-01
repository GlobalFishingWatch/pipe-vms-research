#!/usr/bin/env python

"""
Setup script for pipe-vms-research
"""
from setuptools import find_packages
from setuptools import setup

setup(
    name='pipe-vms-research',
    version='3.1.0',
    packages=find_packages(exclude=['test*.*', 'tests']),
    include_package_data=True
)

