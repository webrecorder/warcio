#!/usr/bin/env python
# vim: set sw=4 et:

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import glob


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        # should work with setuptools <18, 18 18.5
        self.test_suite = ' '

    def run_tests(self):
        import pytest
        import sys
        import os
        errcode = pytest.main(['--doctest-module', './warcio', '--cov', 'warcio', '-v', 'test/'])
        sys.exit(errcode)

setup(
    name='warcio',
    version='1.0',
    author='Ilya Kreymer',
    author_email='ikreymer@gmail.com',
    license='MIT',
    packages=find_packages(),
    url='https://github.com/ikreymer/warcli',
    description='Streaming WARC (and ARC) IO library',
    long_description=open('README.md').read(),
    provides=[
        'warcio',
        ],
    install_requires=[
        ],
    data_files=[
        ('test/data', glob.glob('test/data/*')),
    ],
    zip_safe=True,
    entry_points="""
        [console_scripts]
    """,
    cmdclass={'test': PyTest},
    test_suite='',
    tests_require=[
        'pytest',
        'pytest-cov',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ]
)
