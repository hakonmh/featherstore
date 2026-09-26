===========================
Indexing and selecting data
===========================

Partitioned Feather files let FeatherStore load only the partitions and columns a
query needs. Pass ``rows`` and ``cols`` to the read methods on
:class:`~featherstore.store.Store` or :class:`~featherstore.table.Table`. The same
row and column selectors are used by :meth:`~featherstore.table.Table.drop`.

Selecting rows
--------------

``rows`` can be:

* a collection of index labels, or
* a filter predicate ``{keyword: value}`` where ``keyword`` is ``before``,
  ``after``, or ``between``.

Range bounds are **inclusive**. For ``between``, ``value`` is a two-element
sequence ``[start, end]``.

.. code-block:: python

    store.read_pandas("bergen", rows={"after": "2024-01-02"})
    store.read_pandas("bergen", rows={"before": "2024-01-04"})
    store.read_pandas(
        "bergen",
        rows={"between": ["2024-01-02", "2024-01-04"]},
    )
    store.read_pandas("bergen", rows=["2024-01-01", "2024-01-05"])

If any requested label is missing, FeatherStore raises
:exc:`~featherstore.exceptions.RowNotFoundError`. If label dtypes do not match the
stored index, it raises
:exc:`~featherstore.exceptions.IndexTypeMismatchError`.

Selecting columns
-----------------

``cols`` can be:

* a collection of column names, or
* a filter predicate ``{'like': pattern}``.

``like`` uses SQL wildcards: ``%`` matches any number of characters, ``?``
matches a single character. Matching is case-insensitive. The same ``like``
language is used by :func:`~featherstore.store.list_stores` and
:meth:`~featherstore.store.Store.list_tables`.

.. code-block:: python

    store.read_pandas("bergen", cols=["rainfall", "temperature"])
    store.read_pandas("bergen", cols={"like": "temp%"})

Missing column names raise
:exc:`~featherstore.exceptions.ColumnNotFoundError`.

Combining filters
-----------------

You can pass ``rows`` and ``cols`` together. FeatherStore opens only partitions
that overlap the row filter, and reads only the requested columns from those
files.

.. code-block:: python

    print(
        store.read_pandas(
            "bergen",
            rows={"after": "2024-01-02"},
            cols=["rainfall", "temperature"],
        )
    )

.. code-block:: text

                rainfall  temperature
    date
    2024-01-02       0.0          1.4
    2024-01-04       0.0          0.8
    2024-01-05       0.3          3.2

Partition pruning
-----------------

Because rows are stored in sorted index order, FeatherStore can skip partitions
whose index range does not overlap the query. Smaller partitions can skip more
data on selective row filters, at the cost of slower full-table IO. See
:doc:`partitioning` and :doc:`/Benchmarks`.

Using the same selectors with drop
----------------------------------

:meth:`~featherstore.table.Table.drop` accepts the same ``rows`` and ``cols``
forms. See :doc:`drop`.

See also
--------

* :doc:`table_index` — index types, uniqueness, and sorting
* :doc:`io` — ``mmap`` and full-table reads
* :doc:`/API/Table` — :meth:`~featherstore.table.Table.read_arrow` parameter docs
