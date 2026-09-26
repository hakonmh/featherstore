===========================
Pandas, Polars, and PyArrow
===========================

FeatherStore stores one on-disk layout and can return data as a pandas object, a
Polars object, or a PyArrow Table. This page covers how indexes and Series behave
across those backends.

Index as Index vs column
------------------------

Pandas DataFrames have an index. When you write a pandas object with a named
index, FeatherStore stores that index and restores it on
:meth:`~featherstore.table.Table.read_pandas`.

Polars and PyArrow have no separate index. A named stored index is returned as an
ordinary column. A **default integer index** is omitted from Polars and Arrow
results so you do not get an extra ``__index_level_0__``-style column unless you
need it.

.. code-block:: python

    # pandas: named index stays the DataFrame index
    store.read_pandas("bergen")

    # polars / arrow: named index appears as a column named "date"
    store.read_polars("bergen")
    store.read_arrow("bergen")

Writing from Polars or Arrow
----------------------------

Pass ``index=`` with the column that should become the stored index:

.. code-block:: python

    import datetime as dt
    import polars as pl

    trondheim = pl.DataFrame(
        {
            "date": [dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2)],
            "temperature": [1.8, 0.6],
            "humidity": [92, 87],
            "rainfall": [6.4, 0.2],
        }
    )
    store.write_table("trondheim", trondheim, index="date")

If you omit ``index`` for Polars or Arrow, FeatherStore uses a default integer
index. For pandas, omitting ``index`` uses the DataFrame index when present.

Series on read
--------------

When a read returns a single **data** column (not counting a restored pandas
index), :meth:`~featherstore.table.Table.read_pandas` and
:meth:`~featherstore.table.Table.read_polars` return a Series. Arrow always
returns a Table.

Edit APIs and Polars Series
---------------------------

:meth:`~featherstore.table.Table.update`,
:meth:`~featherstore.table.Table.insert`,
:meth:`~featherstore.table.Table.insert_rows`, and
:meth:`~featherstore.table.Table.insert_columns` accept:

* pandas DataFrame or Series
* Polars DataFrame
* PyArrow Table

Polars Series is **not** supported for those edit methods. Convert to a DataFrame
(or use pandas / Arrow) first. Write and append still accept Polars Series.

Performance note
----------------

Polars and Arrow use the Arrow columnar format in memory, so FeatherStore can
avoid a pandas serialize/deserialize round-trip. See the “Pandas vs Polars and
Arrow” section of :doc:`/Benchmarks`.

See also
--------

* :doc:`io` — write and read parameters
* :doc:`table_index` — supported index types and default indexes
* :doc:`gotchas` — Series surprises and other traps
