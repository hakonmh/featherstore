======
Tables
======

A table is a named folder of partitioned Feather files inside a store. You can
write, read, and append through :class:`~featherstore.store.Store`, or select a
:class:`~featherstore.table.Table` for inserts, updates, drops, and schema
changes.

Store vs Table
--------------

Common IO lives on the store:

* :meth:`~featherstore.store.Store.write_table`
* :meth:`~featherstore.store.Store.read_pandas` /
  :meth:`~featherstore.store.Store.read_polars` /
  :meth:`~featherstore.store.Store.read_arrow`
* :meth:`~featherstore.store.Store.append_table`

:meth:`~featherstore.store.Store.select_table` returns a
:class:`~featherstore.table.Table` with the same IO methods plus editing and
schema APIs. You can also construct ``Table(table_name, store_name)`` directly.
The table does **not** need to exist on disk until you call
:meth:`~featherstore.table.Table.write`; use
:meth:`~featherstore.table.Table.exists` to check.

.. code-block:: python

    import featherstore as fs

    fs.create_database("path/to/db")
    store = fs.create_store("weather")
    table = store.select_table("bergen")
    print(table.exists())
    print(table.name)

.. code-block:: text

    False
    bergen

Listing and existence
---------------------

:meth:`~featherstore.store.Store.list_tables` lists table names in the store.
Pass ``like=`` with SQL wildcards (same rules as :doc:`stores`).
:meth:`~featherstore.store.Store.table_exists` and
:meth:`~featherstore.table.Table.exists` return whether the table folder is
present.

Renaming and dropping
---------------------

Rename with :meth:`~featherstore.store.Store.rename_table` or
:meth:`~featherstore.table.Table.rename_table`:

.. code-block:: python

    store.rename_table("bergen", to="bergen_daily")
    # or: table.rename_table(to="bergen_daily")

Drop with :meth:`~featherstore.store.Store.drop_table` or
:meth:`~featherstore.table.Table.drop_table`. ``warnings='warn'`` (default) warns
if the table is missing; ``warnings='ignore'`` stays quiet.

Forbidden names
---------------

Table names must be valid single path segments. Reserved and invalid names raise
:exc:`~featherstore.exceptions.ForbiddenTableNameError`. That includes
``.metadata``, ``""``, ``"."``, ``".."``, and names containing ``/`` or ``\\``.

Where to go next
----------------

* :doc:`io` — writing and reading data
* :doc:`indexing` — selecting rows and columns
* :doc:`insert`, :doc:`update`, :doc:`drop` — editing stored tables
* :doc:`metadata` — inspecting columns, shape, and index
* :doc:`/API/Table` — full method reference
