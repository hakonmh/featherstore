==============
Appending data
==============

:meth:`~featherstore.store.Store.append_table` and
:meth:`~featherstore.table.Table.append` add rows whose index values fall
**strictly after** the stored data. Only the last partition is loaded and
rewritten.

When to append
--------------

Use append for growing time series and other tables that receive new keys at the
end of the index range. To insert rows into a hole in the middle of the index,
use :doc:`insert` instead.

Basic example
-------------

.. code-block:: python

    import pandas as pd

    new_day = pd.DataFrame(
        {"temperature": [1.9], "humidity": [83], "rainfall": [2.6]},
        index=pd.DatetimeIndex(["2024-01-06"], name="date"),
    )
    store.append_table("bergen", new_day)
    print(store.read_pandas("bergen"))

.. code-block:: text

                temperature  humidity  rainfall
    date
    2024-01-01          2.1        88       4.2
    2024-01-02          1.4        91       0.0
    2024-01-04          0.8        79       0.0
    2024-01-05          3.2        74       0.3
    2024-01-06          1.9        83       2.6

Requirements
------------

* New index values must be strictly after the last stored index value. Otherwise
  FeatherStore raises :exc:`~featherstore.exceptions.AppendIndexError`.
* Column names must match the stored table
  (:exc:`~featherstore.exceptions.ColumnMismatchError`).
* Column dtypes must be compatible
  (:exc:`~featherstore.exceptions.ColumnDtypeMismatchError`).
* Index name and type must match
  (:exc:`~featherstore.exceptions.IndexNameMismatchError`,
  :exc:`~featherstore.exceptions.IndexTypeMismatchError`).
* Index values in the appended batch must be unique
  (:exc:`~featherstore.exceptions.DuplicateIndexValuesError`).

``warnings='warn'`` (default) warns if the appended index will be sorted;
``warnings='ignore'`` suppresses that warning.

Default integer indexes
-----------------------

If the table uses a default integer index, append continues that sequence when
the new data also looks like a default index. If you supply an explicit index
instead, FeatherStore stops treating the table as having a default index. See
:doc:`table_index`.

Append is not insert
--------------------

Append never places rows between existing keys. For mid-range inserts, call
:meth:`~featherstore.table.Table.insert_rows` (or :meth:`~featherstore.table.Table.insert`
when column names match). See :doc:`insert`.

See also
--------

* :doc:`io` — writing the initial table
* :doc:`partitioning` — why only the last partition is rewritten
* :doc:`/API/Table` — :meth:`~featherstore.table.Table.append`
