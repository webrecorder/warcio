#!/usr/bin/env python
# vim: set sw=4 et:

from setuptools import setup, find_packages

__version__ = '1.7.5'


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
    test_suite='',
    extras_require={
        'testing': [
            'urllib3>=1.26.5,<1.26.16',
            'pytest',
            'pytest-cov',
            'httpbin>=0.10.2',
            'requests',
            'wsgiprox',
            'hookdns',
        ],
        'all': [
            'brotlipy',
        ]
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Utilities',
    ]
)
