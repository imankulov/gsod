#!/usr/bin/env python
import os
from distutils.core import setup

def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname)).read()
    except:
        return ''

setup(
    name='gsod',
    version='0.1',
    description='Global Surface Summary of Day data importing library',
    author='Roman Imankulov',
    author_email='roman.imankulov@gmail.com',
    url='https://github.com/imankulov/gsod',
    long_description = read('README.rst'),
    license = 'BSD License',
    py_modules=['gsod'],
    install_requires=[
        'httplib2',
    ],
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ),
)
