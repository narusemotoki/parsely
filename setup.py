#!/usr/bin/env python3
import os
import re
import setuptools
import sys


here = os.path.abspath(os.path.dirname(__file__))


# Python 3.4 doesn't have typing module. So I cannot give typehint here.
def get_meta():
    with open(os.path.join(here, 'brokkoly/__init__.py')) as f:
        source = f.read()

    regex = r'^{}\s*=\s*[\'"]([^\'"]*)[\'"]'
    return lambda name: re.search(regex.format(name), source, re.MULTILINE).group(1)


get_meta = get_meta()

with open(os.path.join(here, 'README.rst')) as f:
    readme = f.read()

install_requires = [
    'celery',
    'falcon',
    'jinja2',
    'jinja2-highlight',
]

if sys.version_info < (3, 5):
    install_requires.append('typing')

test_requires = [
    'Pygments',
    'docutils',
    'flake8',
    'mypy',
    'pytest',
    'pytest-cov',
]

setuptools.setup(
    name='brokkoly',
    version=get_meta('__version__'),
    description="Brokkoly is a framework for enqueuing messages via HTTP request for celery.",
    long_description=readme,
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development :: Libraries"
    ],
    keywords=["celery", "queue"],
    author=get_meta('__author__'),
    author_email=get_meta('__email__'),
    url="https://github.com/narusemotoki/brokkoly",
    license=get_meta('__license__'),
    packages=['brokkoly'],
    install_requires=install_requires,
    extras_require={
        'test': test_requires,
    },
    include_package_data=True,
)
