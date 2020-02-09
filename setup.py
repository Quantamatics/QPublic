# coding: utf-8
# !/usr/bin/env python
from setuptools import setup

DISTNAME = 'QPublic'
DESCRIPTION = "Quantamatics Open Source Packages"
LONG_DESCRIPTION = """NA"""
MAINTAINER = 'Quantamatics Inc'
AUTHOR = 'Quantamatics Inc'
AUTHOR_EMAIL = 'NA'
URL = "https://github.com/Quantamatics/QPublic"
LICENSE = "MIT"

VERSION = "0.1.08b"


packages = ['QPublic','QPublic/MarketData','QPublic/MarketData/Bloomberg']
package_data = {'QPublic': ['*']}

classifiers = ['Development Status :: 4 - Beta',
               'Programming Language :: Python',
               'Programming Language :: Python :: 3',
               'Programming Language :: Python :: 3.4',
               'Programming Language :: Python :: 3.5',
               'Programming Language :: Python :: 3.6',
               'Programming Language :: Python :: 3.7',
               'Intended Audience :: Science/Research',
               'Topic :: Scientific/Engineering :: Mathematics',
               'Operating System :: OS Independent']

install_reqs = [
    'blpapi',
    'ipython>=3.2.3',
    'numpy>=1.8.0',
    'pandas >=0.18.0',
    'python-dateutil>=2.6.0',
]

if __name__ == "__main__":
    setup(
        name=DISTNAME,
        version=VERSION,
        maintainer=MAINTAINER,
        description=DESCRIPTION,
        license=LICENSE,
        url=URL,
        long_description=LONG_DESCRIPTION,
        packages=packages,
        package_data=package_data,
        classifiers=classifiers,
        install_requires=install_reqs
    )