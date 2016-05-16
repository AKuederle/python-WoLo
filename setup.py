# setup.py
import os
import sys
from distutils.core import setup

version = '0.1dev'

if sys.argv[-1] == 'publish':
    os.system("python setup.py sdist upload")
    print("You probably want to also tag the version now:")
    print("  git tag -a {} -m 'version {}'".format(version, version))
    print("  git push --tags")
    sys.exit()


setup(
    name='WoLo',
    version=version,
    author='Arne Küderle',
    author_email='a.kuederle@gmail.com',
    url='https://github.com/AKuederle/python-WoLo',
    packages=['wolo'],
    license='LICENSE.txt',
    description='A scientific workflow tool written in Python',
    long_description=open('README.rst').read(),
    extras_require={
        'export log as dataframe': ['pandas']
    }
)
