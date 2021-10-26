#!/usr/bin/env python

"""
Setup script for pipe-vms-research
"""

import codecs
import os

import setuptools
from setuptools import find_packages
from setuptools import setup

import subprocess
from distutils.command.build import build as _build


package = __import__('pipe_vms_research')


with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    license="Apache 2.0",
    long_description=readme,
    name='pipe-vms-research',
    packages=find_packages(exclude=['test*.*', 'tests']),
    url=package.__source__,
    version=package.__version__,
    zip_safe=True
)

