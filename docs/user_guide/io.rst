===================
Writing and reading
===================

FeatherStore stores Pandas DataFrames and Series, Polars DataFrames and Series,
and PyArrow Tables as partitioned Feather files. This page covers full-table
writes and reads. For row and column filters, see :doc:`indexing`. For backend
differences, see :doc:`backends`.

Supported inputs
----------------

Write and append accept:

* ``pandas.DataFrame`` and ``pandas.Series``
* ``polars.DataFrame`` and ``polars.Series``
* ``pyarrow.Table``

Edit methods such as :meth:`~featherstore.table.Table.update` accept the same
types except Polars Series; see :doc:`backends`.

Writing a table
---------------

Use :meth:`~featherstore.store.Store.write_table` or
:meth:`~featherstore.table.Table.write`:

.. code-block:: python

    import pandas as pd
    import featherstore as fs

    fs.create_database("path/to/db")
    store = fs.create_store("weather")

    df = pd.DataFrame(
        {
            "temperature": [2.1, 1.4, 0.8, 3.2],
            "humidity": [88, 91, 79, 74],
            "rainfall": [4.2, 0.0, 0.0, 0.3],
        },
        index=pd.DatetimeIndex(
            ["2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"],
            name="date",
        ),
    )
    store.write_table("bergen", df, partition_size=128)

Important parameters:

* ``index`` — column name to use as the stored index. For pandas, the DataFrame
  index is used when ``index`` is omitted. For Polars and Arrow, pass the column
  name (or you get a default integer index). See :doc:`table_index`.
* ``partition_size`` — target partition size in **bytes**. Default is 128 MB.
  Pass ``-1`` to store the table as a single partition. See :doc:`partitioning`.
* ``errors='raise'`` (default) refuses to overwrite an existing table.
  ``errors='ignore'`` overwrites.
* ``warnings='warn'`` (default) warns when an unsorted index will be sorted.
  ``warnings='ignore'`` stays quiet.

FeatherStore sorts rows by the index before writing. Duplicate column names,
duplicate index values, unsupported index types, and multi-type columns are
rejected at write time.

Reading a table
---------------

Read back as pandas, Polars, or Arrow:

.. code-block:: python

    store.read_pandas("bergen")
    store.read_polars("bergen")
    store.read_arrow("bergen")

The same methods exist on :class:`~featherstore.table.Table`. Optional keyword
arguments:

* ``cols`` / ``rows`` — see :doc:`indexing`
* ``mmap`` — use memory mapping when opening Feather files. Default is ``False``
  on Windows and ``True`` on other systems.

Pandas restores a named index as the DataFrame index. Polars and Arrow return a
named index as a column. A default integer index is omitted from Polars and Arrow
results. When the result has a single data column, pandas and Polars may return a
Series; see :doc:`backends`.

Full table vs partial reads
---------------------------

A full read loads every partition. Passing ``rows`` and/or ``cols`` lets
FeatherStore open only the partitions and columns that overlap the query, which
saves time and memory on large tables. See :doc:`indexing` and
:doc:`/Benchmarks`.

See also
--------

* :doc:`append` — adding rows after the stored index range
* :doc:`partitioning` — choosing ``partition_size``
* :doc:`/API/Table` — :meth:`~featherstore.table.Table.write` and read methods
