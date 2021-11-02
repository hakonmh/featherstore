Introduction
============
FeatherStore is a fast datastore for storing Pandas DataFrames, Pandas Series, Polars
DataFrames and PyArrow Tables as partitioned `Feather Files <https://arrow.apache.org/docs/python/feather.html>`_.

FeatherStore supports several operations on stored tables that can be done without loading
in the full data:

* Predicate filtering when reading
* Appending data
* Read metadata (column names, index, table size, etc)
* (planned) Insert data
* (planned) Update data
* (planned) Remove columns and rows
* (planned) Changing types

To learn more, go to the `Quickstart <Quickstart.html>`_ page.

Source Code
+++++++++++
| The source code is currently hosted on GitHub at:
| https://github.com/Hakonmh/featherstore

LICENSE
+++++++
`MIT <https://github.com/Hakonmh/featherstore/blob/master/LICENSE>`_

Contributions
+++++++++++++
All contributions, bug reports, bug fixes, documentation improvements, enhancements and ideas are welcome.

| Issues are posted on:
| https://github.com/Hakonmh/featherstore/issues