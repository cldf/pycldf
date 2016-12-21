# -*- coding: utf-8 -*-
from setuptools import setup, find_packages


def read(fname):
    with open(fname) as fp:
        return fp.read().split('\n\n\n')[0]


setup(
    name='pycldf',
    version="0.6.4",
    description='A python library to read and write CLDF datasets',
    long_description=read("README.rst"),
    author='Robert Forkel',
    author_email='forkel@shh.mpg.de',
    url='https://github.com/glottobank/pycldf',
    install_requires=[
        'six',
        'pybtex',
        'clldutils>=0.9.1',
        'uritemplate>=3.0',
        'python-dateutil',
    ],
    license="Apache 2",
    zip_safe=False,
    keywords='',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'cldf=pycldf.cli:main',
        ]
    },
    tests_require=['nose', 'coverage', 'mock'],
)
