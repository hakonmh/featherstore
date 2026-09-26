==============
Table metadata
==============

Several attributes and methods inspect a table without loading every partition.
Use them for discovery, validation, and lightweight bookkeeping.

What you can inspect
--------------------

On an existing :class:`~featherstore.table.Table`:

.. code-block:: python

    print(table.columns)
    print(table.shape)
    print(table.partition_size)
    print(table.name)
    print(table.exists())
    print(table.index)

.. code-block:: text

    ['date', 'temperature', 'humidity', 'wind_speed', 'rainfall']
    (5, 5)
    128
    bergen
    True
    DatetimeIndex(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04',
                   '2024-01-05'],
                  dtype='datetime64[us]', name='date', freq=None)

Details:

* :attr:`~featherstore.table.Table.columns` — column names **including** the
  index name. Cheap (metadata only). The **setter** reorders data columns; see
  :doc:`columns`.
* :attr:`~featherstore.table.Table.shape` — ``(rows, columns)`` where the column
  count **includes** the index column. Cheap.
* :attr:`~featherstore.table.Table.partition_size` — partition size in bytes.
  Cheap.
* :attr:`~featherstore.table.Table.name` — table folder name.
* :meth:`~featherstore.table.Table.exists` — whether the table folder is on disk.
* :attr:`~featherstore.table.Table.index` — returns a pandas ``Index``. This
  reads the index column from disk (not a pure metadata hit).

Store-level listing
-------------------

To list tables without opening each one, use
:meth:`~featherstore.store.Store.list_tables` and
:meth:`~featherstore.store.Store.table_exists`. See :doc:`tables`.

Related schema methods
----------------------

Metadata inspection is read-only (except the ``columns`` setter). To change
names, order, or dtypes, see :doc:`columns`. To change partition size, see
:doc:`partitioning`.

See also
--------

* :doc:`data_model` — what lives in ``.metadata``
* :doc:`table_index` — why the index appears in ``columns`` and ``shape``
* :doc:`/API/Table` — attribute and method docs
