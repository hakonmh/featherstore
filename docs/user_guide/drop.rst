=========================
Dropping rows and columns
=========================

:meth:`~featherstore.table.Table.drop` removes rows and/or columns. You can also
call :meth:`~featherstore.table.Table.drop_rows` and
:meth:`~featherstore.table.Table.drop_columns` directly.

Selectors
---------

``rows`` and ``cols`` use the same forms as reads (see :doc:`indexing`):

* collections of labels / column names
* row predicates ``before`` / ``after`` / ``between``
* column predicate ``{'like': pattern}``

Both may be provided in one call. At least one of ``rows`` or ``cols`` is
required (otherwise ``AttributeError``).

.. code-block:: python

    table.drop(rows=["2024-01-06"])
    table.drop(cols=["humidity"])
    table.drop(rows={"before": "2024-01-02"}, cols={"like": "rain%"})

Cost model
----------

* **Row drops** rewrite only partitions that overlap the dropped index range.
* **Column drops** rewrite every partition, because each partition holds every
  column.

Constraints
-----------

* You cannot drop all rows
  (:exc:`~featherstore.exceptions.CannotDropAllRowsError`).
* You cannot drop all columns
  (:exc:`~featherstore.exceptions.CannotDropAllColumnsError`).
* You cannot drop the index column
  (:exc:`~featherstore.exceptions.IndexNameInColumnsError`).
* Missing labels or columns raise
  :exc:`~featherstore.exceptions.RowNotFoundError` or
  :exc:`~featherstore.exceptions.ColumnNotFoundError`.

Example
-------

.. code-block:: python

    table.drop(rows=["2024-01-06"])
    print(table.read_pandas())

.. code-block:: text

                temperature  humidity  wind_speed  rainfall
    date
    2024-01-01          2.4        88         4.5       3.8
    2024-01-02          1.4        91         6.1       0.0
    2024-01-03         -0.3        85         3.2       1.1
    2024-01-04          0.8        79         5.8       0.0
    2024-01-05          3.2        74         2.0       0.3

See also
--------

* :doc:`indexing` — row and column predicate syntax
* :doc:`update` — changing values without removing rows
* :doc:`tables` — dropping an entire table with ``drop_table``
