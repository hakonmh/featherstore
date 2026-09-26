===============
The table index
===============

Every FeatherStore table has an index. Rows are stored sorted by that index, and
index values must be unique. The index is what makes range queries and targeted
row edits cheap.

Supported types
---------------

The index must be one of:

* integer or unsigned integer
* float
* decimal
* string
* binary
* duration
* temporal (date, time, or timestamp)

Unsupported types raise
:exc:`~featherstore.exceptions.UnsupportedIndexTypeError` at write or when
casting with :meth:`~featherstore.table.Table.astype`.

Uniqueness and sorting
----------------------

Duplicate index values raise
:exc:`~featherstore.exceptions.DuplicateIndexValuesError`. Before writing or
appending, FeatherStore sorts the input by the index. If the input was unsorted,
you get a warning unless you pass ``warnings='ignore'``.

Named vs default index
----------------------

* **Pandas**: if the DataFrame has an index, FeatherStore uses it (unless you
  pass ``index=`` to select a column instead).
* **Polars / Arrow**: pass ``index=`` with the column name. If you omit it,
  FeatherStore assigns a default integer index.

A named index is a real stored column. It appears in
:attr:`~featherstore.table.Table.columns` and counts toward
:attr:`~featherstore.table.Table.shape`. A default integer index is omitted from
Polars and Arrow reads; see :doc:`backends`.

Some operations track whether the table still has a default index (for example
append, row insert, drop, and astype) so later appends can continue the integer
sequence correctly.

Rules that often surprise people
--------------------------------

* You cannot drop the index column.
* You cannot insert a data column whose name is the index name
  (:exc:`~featherstore.exceptions.IndexNameInColumnsError`).
* :meth:`~featherstore.table.Table.update` cannot change index **values**. To
  rekey rows, drop the old labels and :doc:`insert` new ones.
* :meth:`~featherstore.table.Table.astype` can cast the index by including its
  name in the mapping, still subject to supported types.
* :meth:`~featherstore.table.Table.reorder_columns` takes data column names only;
  do not include the index name.

See also
--------

* :doc:`indexing` — selecting by labels and ranges
* :doc:`io` — ``index=`` on write
* :doc:`gotchas` — common index-related traps
