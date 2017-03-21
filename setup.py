"""
TruenoDB Python Driver Setup Module.
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='trueno_python_driver',
    version='0.1.0',

    description='TruenoDB Python Driver',
    long_description=long_description,

    url='https://github.com/TruenoDB/trueno-python-driver',

    author='Miguel Rivera',
    author_email='mrivm@users.noreply.github.com',

    license='',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',
        'License :: Free for non-commercial use',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],

    keywords='graph database connector trueno',

    packages=find_packages(),

    install_requires=['socketIO-client', 'promise'],

    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },
)
