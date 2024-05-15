#!/usr/bin/env python
# vim: set sw=4 et:

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import glob

__version__ = '1.7.4'


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        # should work with setuptools <18, 18 18.5
        self.test_suite = ' '

    def run_tests(self):
        import pytest
        import sys
        import os
        errcode = pytest.main(['--doctest-modules', './warcio', '--cov', 'warcio', '-v', 'test/'])
        sys.exit(errcode)

setup(
    name='warcio',
    version=__version__,
    author='Ilya Kreymer',
    author_email='ikreymer@gmail.com',
    license='Apache 2.0',
    packages=find_packages(exclude=['test']),
    url='https://github.com/webrecorder/warcio',
    description='Streaming WARC (and ARC) IO library',
    long_description=open('README.rst').read(),
    provides=[
        'warcio',
        ],
    install_requires=[
        'six',
        ],
    zip_safe=True,
    entry_points="""
        [console_scripts]
        warcio = warcio.cli:main
    """,
    cmdclass={'test': PyTest},
    test_suite='',
    tests_require=[
        'urllib3==1.25.11',
        'pytest',
        'pytest-cov',
        'httpbin>=0.10.2',
        'requests',
        'wsgiprox',
        'hookdns',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ]
)
