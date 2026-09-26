==========================
Inserting rows and columns
==========================

:class:`~featherstore.table.Table` can insert rows at sorted index positions or
insert new columns. Use the explicit methods when you know which operation you
want, or :meth:`~featherstore.table.Table.insert` for convenience dispatch.

``insert_rows``, ``insert_columns``, and ``insert``
---------------------------------------------------

* :meth:`~featherstore.table.Table.insert_rows` — insert rows; column names must
  match the stored table.
* :meth:`~featherstore.table.Table.insert_columns` — insert columns; new names
  must not already exist.
* :meth:`~featherstore.table.Table.insert` — if ``df`` column names match the
  stored table, rows are inserted; otherwise columns are inserted.

Accepted types: pandas DataFrame or Series, Polars DataFrame, PyArrow Table.
Polars Series is not supported for these methods (see :doc:`backends`).

Inserting rows
--------------

.. code-block:: python

    delayed = pd.DataFrame(
        {"temperature": [-0.3], "humidity": [85], "rainfall": [1.1]},
        index=pd.DatetimeIndex(["2024-01-03"], name="date"),
    )
    table.insert(delayed)  # matching columns -> insert_rows
    # or: table.insert_rows(delayed)

Rows are placed at their sorted index positions. Overlapping partitions are
rewritten. Existing index labels raise
:exc:`~featherstore.exceptions.RowAlreadyExistsError`. Schema rules match append
(column names/dtypes and index name/type must align).

Pass ``warnings='ignore'`` to suppress sorting warnings when the input index is
unsorted (default ``warnings='warn'``).

Inserting columns
-----------------

Pass data whose column names are **not** already in the table. FeatherStore
rewrites every partition.

``idx`` controls placement among **data** columns (the index is not counted):

* a single integer inserts the new columns as a block at that position
* a sequence of integers places each new column individually (one position per
  new column)
* default ``idx=-1`` appends columns at the end
* ``0`` inserts as the first data column

.. code-block:: python

    index = table.read_pandas().index
    wind = pd.DataFrame(
        {"wind_speed": [4.5, 6.1, 3.2, 5.8, 2.0, 7.4]},
        index=index,
    )
    table.insert(wind, idx=2)
    # or: table.insert_columns(wind, idx=2)

``idx`` is only valid when inserting columns. Passing ``idx`` while inserting
rows raises ``TypeError``.

Common errors
-------------

* :exc:`~featherstore.exceptions.ColumnAlreadyExistsError` — new column name
  collides with an existing one
* :exc:`~featherstore.exceptions.ColumnLengthMismatchError` — new column length
  does not match the row count
* :exc:`~featherstore.exceptions.IndexMismatchError` — indices do not match the
  stored table (column insert)
* :exc:`~featherstore.exceptions.IndexNameInColumnsError` — a new column uses the
  index name

See also
--------

* :doc:`append` — adding rows only after the last index value
* :doc:`update` — overwriting existing cells
* :doc:`drop` — removing rows or columns
* :doc:`gotchas` — ``insert()`` dispatch surprises
