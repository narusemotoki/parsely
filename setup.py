#!/usr/bin/env python3
import os
import re
import setuptools
import sys


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'brokkoly.py')) as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read(), re.MULTILINE).group(1)

with open(os.path.join(here, 'README.rst')) as f:
    readme = f.read()

install_requires = [
    'celery',
    'falcon',
]

if sys.version_info < (3, 5):
    install_requires.append('mypy')

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
    version=version,
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
    author="Motoki Naruse",
    author_email="motoki@naru.se",
    url="https://github.com/narusemotoki/brokkoly",
    license='MIT',
    py_modules=['brokkoly'],
    install_requires=install_requires,
    extras_require={
        'test': test_requires,
    }
)
