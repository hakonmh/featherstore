Overview
========

FeatherStore is high performance datastore for storing Pandas DataFrames, Polars DataFrames,
and PyArrow Tables. By saving data in the form of partitioned
`Feather Files <https://arrow.apache.org/docs/python/feather.html>`_, FeatherStore enables
several operations on the stored tables, optimizing performance by selectively loading only
the necessary segments of data:

* Partial reading of data
* Append data
* Insert data
* Update data
* Drop data
* Read metadata (including column names, indices, table dimensions, etc.)
* Changing column types

For more information on using FeatherStore, please refer to the
`user guide <Quickstart.html>`_.

Installation
++++++++++++
| The project is hosted on PyPI at:
| https://pypi.org/project/FeatherStore/

| To install FeatherStore, simply use pip

.. code-block::

    pip install featherstore

| or

.. code-block::

    pip install git+https://github.com/hakonmh/featherstore.git

| to install the latest version from GitHub.

Python version support
----------------------

Officially Python 3.8 and up is supported.

Source Code
+++++++++++

| The source code is currently hosted on GitHub at:
| https://github.com/Hakonmh/featherstore

LICENSE
+++++++

`MIT <https://github.com/hakonmh/featherstore/blob/master/LICENSE>`_

Contributions
+++++++++++++

All contributions, bug reports, bug fixes, documentation improvements, enhancements and ideas are welcome.

| Issues are posted on:
| https://github.com/hakonmh/featherstore/issues
