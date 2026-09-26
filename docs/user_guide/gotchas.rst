=======
Gotchas
=======

This page collects behaviors that surprise people coming from pandas files,
databases, or earlier FeatherStore versions. It is not a second API dump; see the
topic pages and :doc:`/API Reference` for details.

Process-global connection
-------------------------

There is one active database per process. A new
:func:`~featherstore.connection.connect` replaces the previous connection. If you
juggle multiple databases, disconnect explicitly and reconnect as needed. See
:doc:`database`.

``create_store`` vs ``write``
-----------------------------

:func:`~featherstore.store.create_store` does **not** fail when the store already
exists; it warns and returns the existing store. :meth:`~featherstore.table.Table.write`
**does** fail when the table already exists, unless you pass ``errors='ignore'``.

``insert()`` dispatch
---------------------

:meth:`~featherstore.table.Table.insert` chooses rows vs columns by comparing
column names. Matching names mean row insert; anything else means column insert.
If that is ambiguous for your data, call ``insert_rows`` or ``insert_columns``
explicitly. See :doc:`insert`.

``shape`` and ``columns`` include the index
-------------------------------------------

:attr:`~featherstore.table.Table.shape` is ``(rows, columns)`` **including** the
index column. :attr:`~featherstore.table.Table.columns` lists the index name
first (or among the stored columns). :meth:`~featherstore.table.Table.reorder_columns`
takes data column names only — do not pass the index name. See :doc:`metadata`
and :doc:`table_index`.

Series on read, not always on edit
----------------------------------

A single-column pandas or Polars read may return a Series. Edit APIs accept
pandas Series but **not** Polars Series. See :doc:`backends`.

Append is not insert
--------------------

Append requires new index values strictly after the stored range. Mid-range rows
need ``insert_rows``. See :doc:`append` and :doc:`insert`.

Update cannot change index labels
---------------------------------

To rekey rows, drop and insert. See :doc:`update`.

You cannot drop everything
--------------------------

Dropping all rows or all columns is rejected. Dropping the index column is also
forbidden. See :doc:`drop`.

Windows memory mapping
----------------------

``mmap`` defaults to ``False`` on Windows and ``True`` elsewhere. Pass
``mmap=True`` or ``mmap=False`` explicitly when you care. See :doc:`io`.

Empty-only deletes
------------------

You cannot :func:`~featherstore.store.drop_store` a store that still has tables,
or :func:`~featherstore.connection.drop_database` a database that still has
stores. Clear children first.

Forbidden names
---------------

``.featherstore`` is reserved as a store name (database marker). ``.metadata`` is
reserved as a table name (per-table metadata folder). Empty names, ``"."``,
``".."``, and names with ``/`` or ``\\`` are also forbidden. See :doc:`stores`
and :doc:`tables`.

Coming from FeatherStore 0.2
----------------------------

Highlights from 0.3.0 (see ``CHANGELOG.md`` for the full list):

* Python 3.11+; higher pandas / Polars / PyArrow floors
* ``Table.insert_rows`` / ``insert_columns`` replace older insert/add_columns
  naming; ``insert()`` is a convenience dispatcher
* Domain errors raise types from ``featherstore.exceptions`` instead of many
  built-ins

See also
--------

* :doc:`errors` — exception hierarchy and ``errors`` / ``warnings``
* :doc:`10min` — guided tour without the edge cases
* :doc:`data_model` — cost model behind several of these rules
