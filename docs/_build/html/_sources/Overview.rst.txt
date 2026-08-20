Overview
========

FeatherStore is a high-performance datastore for Pandas DataFrames, Polars DataFrames,
and PyArrow Tables. Data is stored as partitioned
`Feather files <https://arrow.apache.org/docs/python/feather.html>`_, so FeatherStore can
run several operations on stored tables while loading only the data each operation needs:

* Partial reads
* Appends
* Row and column inserts (Pandas, Polars, or PyArrow)
* Updates (Pandas, Polars, or PyArrow)
* Drops
* Metadata reads (column names, index, table dimensions, and more)
* Column type changes

For more information, see the
`user guide <Quickstart.html>`_.

Installation
++++++++++++
| The project is hosted on PyPI at:
| https://pypi.org/project/FeatherStore/

| To install FeatherStore, use pip:

.. code-block::

    pip install featherstore

| or

.. code-block::

    pip install git+https://github.com/hakonmh/featherstore.git

| to install the latest version from GitHub.

Python version support
----------------------

Python 3.11 and later are officially supported.

Dependency requirements
-----------------------

FeatherStore 0.3.0 requires:

* pandas >= 2.2.0
* polars[timezone] >= 1.21.0
* pyarrow >= 14.0.0

These are installed automatically with ``pip install featherstore``.

Source code
+++++++++++

| The source code is hosted on GitHub at:
| https://github.com/hakonmh/featherstore

License
+++++++

`MIT <https://github.com/hakonmh/featherstore/blob/master/LICENSE>`_

Contributions
+++++++++++++

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.

| Issues are posted on:
| https://github.com/hakonmh/featherstore/issues
