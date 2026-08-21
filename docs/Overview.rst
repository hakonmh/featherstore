Overview
========

FeatherStore is a datastore for Pandas DataFrames, Polars DataFrames, and
PyArrow Tables. It is built for the machine you already have: a standard
consumer PC or laptop, not a cluster.

The usual way to persist a DataFrame is to write the whole object to a file
(Feather, Parquet, Pickle) and read the whole object back. That is simple, and
it is fast when the table is small. It gets expensive when the table grows and
you only need yesterday's rows, two columns, or to append today's batch.

FeatherStore's goal is to be the fastest local datastore for that workload:
repeated reads, range queries, appends, and row edits on tables that live on
disk and may be larger than you want to load at once.

The core idea
-------------

Feather already gives you a strong on-disk layout. Files are **columnar**, so
you can read a subset of columns. They support **memory mapping** and
**zero-copy** access into Arrow, so you avoid serialize/deserialize round-trips
when you stay in Polars or PyArrow.

FeatherStore keeps that layout and adds two things:

* **Partitions** — a table is split into several Feather files of a chosen size
  (128 MB by default).
* **A sorted unique index** — rows are stored in index order, so FeatherStore
  knows which partitions overlap a query or a row edit.

Together, those are what make partial work cheap. A range query opens only the
partitions that overlap the requested index values, and only the columns you
ask for. An append rewrites the last partition, not the whole table. A row
update, insert, or drop rewrites only the partitions whose index range is
touched. Metadata (column names, shape, index) can be read without scanning
the data files.

Column inserts and drops still rewrite every partition, because a column lives
in all of them.

You keep working with the DataFrames you already use. FeatherStore is a store
for those objects, not a query engine and not a database server.

How data is organized
---------------------

A **database** is a directory on disk. Inside it, **stores** group related
**tables**. Each table is a folder of partitioned Feather files plus metadata:

.. code-block:: text

    path/to/db/          # database
        weather/         # store
            bergen/      # table
                ...      # Feather partitions and metadata

There is no daemon to start. Point FeatherStore at a folder and read or write.

What this makes cheap
---------------------

Because tables are partitioned Feather files with a sorted index, FeatherStore
can run several operations while loading only the partitions each operation
needs:

* Partial reads (rows by label or range, columns by name or pattern)
* Appends (last partition only)
* Row inserts, updates, and drops (only partitions that overlap the affected
  index range)
* Metadata reads (column names, index, table dimensions, and more)

These still rewrite the full table, because they change every partition:

* Column inserts and drops
* Column type changes and column renames

Also supported:

* Table and store snapshots

On small tables, a single Pickle or Feather file can still be faster to read or
write in full. FeatherStore is aimed at the case where the table is large
enough, or queried often enough, that loading everything is the wrong default.
See the :doc:`Benchmarks` for full-table read and write numbers, and the
:doc:`Quickstart` for the operations that skip most of the file.

When to use it
--------------

FeatherStore fits local, DataFrame-shaped work: time series that grow every
day, keyed tables you slice by index, notebooks and apps that should not stand
up a database. It is not a warehouse, not SQL, and not a replacement for
DuckDB when you want ad-hoc queries over many files.

To start using it, follow the :doc:`Quickstart`.

Installation
------------

The project is hosted on PyPI at https://pypi.org/project/FeatherStore/.

Install with pip or uv:

.. code-block:: bash

    pip install featherstore

.. code-block:: bash

    uv add featherstore

Or install the latest version from GitHub:

.. code-block:: bash

    pip install git+https://github.com/hakonmh/featherstore.git

.. code-block:: bash

    uv add git+https://github.com/hakonmh/featherstore.git

Python version support
^^^^^^^^^^^^^^^^^^^^^^

Python 3.11 and later are officially supported.

Dependency requirements
^^^^^^^^^^^^^^^^^^^^^^^

FeatherStore 0.3.0 requires:

* pandas >= 2.2.0
* polars[timezone] >= 1.21.0
* pyarrow >= 14.0.0

These are installed automatically with ``pip install featherstore`` or
``uv add featherstore``.

Source code
-----------

The source code is hosted on GitHub at https://github.com/hakonmh/featherstore.

License
-------

`MIT <https://github.com/hakonmh/featherstore/blob/master/LICENSE>`_

Contributions
-------------

All contributions, bug reports, bug fixes, documentation improvements,
enhancements, and ideas are welcome.

Issues are posted on https://github.com/hakonmh/featherstore/issues.
