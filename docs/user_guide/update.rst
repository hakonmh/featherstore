=============
Updating data
=============

:meth:`~featherstore.table.Table.update` overwrites stored values for the given
index labels and columns. Only partitions whose index range overlaps the update
are rewritten.

Basic usage
-----------

The index of the input selects which rows to update. The columns of the input
are the values to write. You can update a subset of columns; other columns are
left unchanged.

.. code-block:: python

    correction = pd.DataFrame(
        {"temperature": [2.4], "rainfall": [3.8]},
        index=pd.DatetimeIndex(["2024-01-01"], name="date"),
    )
    table.update(correction)
    print(table.read_pandas())

.. code-block:: text

                temperature  humidity  wind_speed  rainfall
    date
    2024-01-01          2.4        88         4.5       3.8
    2024-01-02          1.4        91         6.1       0.0
    ...

Accepted inputs: pandas DataFrame or Series, Polars DataFrame, PyArrow Table
(not Polars Series).

Index values cannot be updated
------------------------------

``update`` changes cell values, not index labels. To rekey a row, drop the old
label and insert a new one:

.. code-block:: python

    table.drop(rows=["2024-01-01"])
    table.insert_rows(new_row_with_new_index)

See :doc:`drop` and :doc:`insert`.

Requirements
------------

* Every requested row must already exist
  (:exc:`~featherstore.exceptions.RowNotFoundError`).
* Columns must exist in the table
  (:exc:`~featherstore.exceptions.ColumnNotFoundError`).
* Column dtypes must be compatible
  (:exc:`~featherstore.exceptions.ColumnDtypeMismatchError`).
* Index name and type must match the stored table.
* Index values in the update batch must be unique.

See also
--------

* :doc:`insert` — adding new rows or columns
* :doc:`append` — adding rows after the stored range
* :doc:`/API/Table` — :meth:`~featherstore.table.Table.update`
