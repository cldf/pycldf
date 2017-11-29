from setuptools import setup, find_packages


def read(fname):
    with open(fname) as fp:
        return fp.read().split('\n\n\n')[0]


setup(
    name='pycldf',
    version='1.0.7',
    description='A python library to read and write CLDF datasets',
    long_description=read('README.rst'),
    author='Robert Forkel',
    author_email='forkel@shh.mpg.de',
    url='https://github.com/cldf/pycldf',
    install_requires=[
        'clldutils>=1.13.10',
        'csvw>=0.1',
        'uritemplate>=3.0',
        'python-dateutil',
        'pybtex',
        'six',
    ],
    extras_require={
        'dev': ['flake8', 'wheel', 'twine'],
        'test': [
            'pytest>=3.1',
            'pytest-mock',
            'mock',
            'pytest-cov',
            'coverage>=4.2',
        ],
    },
    license='Apache 2',
    zip_safe=False,
    keywords='',
    platforms='any',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'
    ],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'cldf=pycldf.__main__:main',
        ]
    },
)
