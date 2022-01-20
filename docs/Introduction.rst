Introduction
============
FeatherStore is a fast datastore for storing Pandas DataFrames, Pandas Series, Polars
DataFrames and PyArrow Tables as partitioned `Feather Files <https://arrow.apache.org/docs/python/feather.html>`_.

FeatherStore supports several operations on stored tables that can be done without loading
in the full data:

* Partial reading of data
* Append data
* Insert data
* Update data
* Drop data
* Read metadata (column names, index, table shape, etc)
* Changing column types

To learn more, go to the `Quickstart <Quickstart.html>`_ page.


Installation
++++++++++++
| The project is hosted on PyPI at:
| https://pypi.org/project/FeatherStore/

| To install FeatherStore, simply use pip

.. code-block::

    pip install featherstore

Python version support
----------------------
Officially Python 3.8, 3.9, and 3.10.

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
