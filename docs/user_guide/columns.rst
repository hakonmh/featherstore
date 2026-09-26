=========================================
Renaming, reordering, and changing dtypes
=========================================

Schema-oriented column operations rewrite the full table. Use them when you need
to rename, reorder, or cast columns; use :doc:`metadata` to inspect names without
loading data.

Renaming columns
----------------

:meth:`~featherstore.table.Table.rename_columns` supports two call styles:

.. code-block:: python

    table.rename_columns({"humidity": "rh", "rainfall": "precip"})
    table.rename_columns(["humidity", "rainfall"], to=["rh", "precip"])

You cannot rename a column to the index name
(:exc:`~featherstore.exceptions.IndexNameInColumnsError`). Renamed names must
stay unique.

Reordering columns
------------------

:meth:`~featherstore.table.Table.reorder_columns` (and the
:attr:`~featherstore.table.Table.columns` **setter**) change data-column order
only. Pass the data column names in the desired order **without** the index
name:

.. code-block:: python

    table.reorder_columns(["rainfall", "temperature", "humidity"])
    # equivalent:
    table.columns = ["rainfall", "temperature", "humidity"]

This does **not** rename columns. To rename, use ``rename_columns``. Including
the index name raises
:exc:`~featherstore.exceptions.IndexNameInColumnsError`.

Changing dtypes
---------------

:meth:`~featherstore.table.Table.astype` casts one or more columns. It accepts a
dict mapping, or parallel sequences with ``to=``. Dtypes may be PyArrow types or
NumPy dtypes.

.. code-block:: python

    import pyarrow as pa

    table.astype({"humidity": pa.int32()})
    table.astype(["humidity", "rainfall"], to=[pa.int32(), pa.float32()])

Include the index name in the mapping to cast the index. The new index type must
still be supported (see :doc:`table_index`).

Cost
----

Rename, reorder, and astype rewrite every partition. Prefer them for infrequent
schema fixes rather than hot paths. Contrast with reading
:attr:`~featherstore.table.Table.columns`, which only touches metadata.

See also
--------

* :doc:`metadata` — inspecting columns and shape
* :doc:`insert` — adding columns
* :doc:`drop` — removing columns
* :doc:`/API/Table` — method reference
