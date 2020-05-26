# coding: utf-8

from setuptools import setup, find_packages

setup(
    name="curvefs",
    version="version-anchor",
    description="curve sdk for python",
    author="curve-dev",
    packages=find_packages(),
    package_data={
        "": ["*.so"],
    }
)
