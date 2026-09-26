=========
Snapshots
=========

Snapshots are compressed ``.tar.xz`` archives of a table or an entire store. Use
them for backups and for copying data between databases.

Creating a snapshot
-------------------

Call :meth:`~featherstore.table.Table.create_snapshot` or
:meth:`~featherstore.store.Store.create_snapshot` with a path. FeatherStore
appends ``.tar.xz`` unless the path already ends with that suffix. An existing
file at the resulting path is overwritten.

.. code-block:: python

    from featherstore import snapshot

    table.create_snapshot("path/to/bergen_backup")
    store.create_snapshot("path/to/weather_backup")

Restoring
---------

Restore into the **currently connected** database:

* :func:`~featherstore.snapshot.restore_table` — restore a table snapshot into an
  existing store
* :func:`~featherstore.snapshot.restore_store` — restore a store snapshot

The restored **name comes from the archive**, not from a rename argument.
``store_name`` on ``restore_table`` selects only the destination store.

.. code-block:: python

    snapshot.restore_table("weather", "path/to/bergen_backup")
    snapshot.restore_store("path/to/weather_backup")

``errors='raise'`` (default) refuses to overwrite an existing table or store.
``errors='ignore'`` overwrites.

Constraints and errors
----------------------

* The target store must exist for ``restore_table``
  (:exc:`~featherstore.exceptions.StoreNotFoundError`).
* A missing archive raises
  :exc:`~featherstore.exceptions.SnapshotNotFoundError`.
* Restoring a store archive with ``restore_table`` (or the reverse) raises
  :exc:`~featherstore.exceptions.InvalidSnapshotError`.
* Archived names still obey forbidden-name rules for stores and tables.

See also
--------

* :doc:`10min` — short snapshot example in the tour
* :doc:`/API/Snapshot` — full function reference
* :doc:`errors` — snapshot-related exceptions
