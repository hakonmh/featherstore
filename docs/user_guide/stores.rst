======
Stores
======

A store is a named folder inside the database that groups related tables. Stores
are created and listed at the package level, and opened as
:class:`~featherstore.store.Store` objects for table operations.

Creating and opening
--------------------

:func:`~featherstore.store.create_store` creates the store directory and returns
a :class:`~featherstore.store.Store`. If a store with that name already exists,
FeatherStore issues a warning (unless ``warnings='ignore'``) and returns the
existing store. It does **not** raise
:exc:`~featherstore.exceptions.StoreAlreadyExistsError`.

.. code-block:: python

    import featherstore as fs

    fs.create_database("path/to/db")
    store = fs.create_store("weather")
    print(store.name)
    print(fs.list_stores())

.. code-block:: text

    weather
    ['weather']

To open an existing store without creating it, construct
:class:`~featherstore.store.Store` directly:

.. code-block:: python

    store = fs.Store("weather")

:meth:`~featherstore.store.Store.__init__` raises
:exc:`~featherstore.exceptions.StoreNotFoundError` if the store is missing.

Renaming and dropping
---------------------

Rename with :func:`~featherstore.store.rename_store` or
:meth:`~featherstore.store.Store.rename`:

.. code-block:: python

    fs.rename_store("weather", to="obs")
    # or: store.rename(to="obs")

Drop an empty store with :func:`~featherstore.store.drop_store` or
:meth:`~featherstore.store.Store.drop`. A store that still contains tables raises
:exc:`~featherstore.exceptions.StoreNotEmptyError`. Use ``warnings='ignore'`` to
suppress the warning when the store does not exist.

Listing and existence
---------------------

:func:`~featherstore.store.list_stores` returns store names. Pass ``like=`` with
SQL wildcards (``%`` for any number of characters, ``?`` for one character;
case-insensitive) to filter. :func:`~featherstore.store.store_exists` returns a
bool.

.. code-block:: python

    fs.list_stores(like="wea%")
    fs.store_exists("weather")

Forbidden names
---------------

Store names must be valid single path segments. Reserved and invalid names raise
:exc:`~featherstore.exceptions.ForbiddenStoreNameError`. That includes
``.featherstore``, ``""``, ``"."``, ``".."``, and names containing ``/`` or
``\\``.

Table operations on a store
---------------------------

:class:`~featherstore.store.Store` provides write, read, append, list, rename, and
drop for tables, plus :meth:`~featherstore.store.Store.select_table` for a
:class:`~featherstore.table.Table`. See :doc:`tables`, :doc:`io`, and
:doc:`append`.

See also
--------

* :doc:`database` — connecting before you create stores
* :doc:`tables` — table lifecycle inside a store
* :doc:`/API/Store` — full API reference
