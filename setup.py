"""Setup module for Robot Framework PostgreSQL Library package."""

# To use a consistent encoding
from codecs import open
from os import path

from setuptools import setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='robotframework-postgresqldb',
    version='1.0.2',
    description='Robot Framework Library For Working With PostgreSQL Database.',
    long_description=long_description,
    url='https://github.com/peterservice-rnd/robotframework-postgresqldb',
    author='Nexign',
    author_email='mf_aist_all@nexign-systems.com',
    license='Apache License 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Testing',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Framework :: Robot Framework :: Library',
    ],
    keywords='testing testautomation robotframework autotest psycopg2 postgresql database',
    package_dir={'': 'src'},
    py_modules=['PostgreSQLDB'],
    install_requires=[
        "robotframework",
        "psycopg2"
    ],
    extras_require={
        ':python_version<="2.7"': [
            'future>=0.16.0'
        ],
    }
)
