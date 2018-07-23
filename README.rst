========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|

.. |docs| image:: https://readthedocs.org/projects/python-pandasutils/badge/?style=flat
    :target: https://readthedocs.org/projects/python-pandasutils
    :alt: Documentation Status

.. |travis| image:: https://travis-ci.org/fernandolkf/python-pandasutils.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/fernandolkf/python-pandasutils

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/fernandolkf/python-pandasutils?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/fernandolkf/python-pandasutils

.. |requires| image:: https://requires.io/github/fernandolkf/python-pandasutils/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/fernandolkf/python-pandasutils/requirements/?branch=master

.. |codecov| image:: https://codecov.io/github/fernandolkf/python-pandasutils/coverage.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/fernandolkf/python-pandasutils

.. |version| image:: https://img.shields.io/pypi/v/pandasutils.svg
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/pandasutils

.. |commits-since| image:: https://img.shields.io/github/commits-since/fernandolkf/python-pandasutils/v0.1.0.svg
    :alt: Commits since latest release
    :target: https://github.com/fernandolkf/python-pandasutils/compare/v0.1.0...master

.. |wheel| image:: https://img.shields.io/pypi/wheel/pandasutils.svg
    :alt: PyPI Wheel
    :target: https://pypi.python.org/pypi/pandasutils

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/pandasutils.svg
    :alt: Supported versions
    :target: https://pypi.python.org/pypi/pandasutils

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/pandasutils.svg
    :alt: Supported implementations
    :target: https://pypi.python.org/pypi/pandasutils


.. end-badges

A collection of functions and tools do deal with transformations and tricks on pandas library

* Free software: BSD 2-Clause License

Installation
============

::

    pip install pandasutils

Documentation
=============

https://python-pandasutils.readthedocs.io/

Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
