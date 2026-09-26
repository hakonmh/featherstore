===================
Errors and warnings
===================

FeatherStore uses a dedicated exception hierarchy for domain failures, plus the
usual built-in exceptions for bad argument shapes. Many APIs also take
``errors=`` or ``warnings=`` to control overwrite and soft-failure behavior.

Domain exceptions
-----------------

Import exception types from ``featherstore.exceptions``. They are **not**
re-exported from the top-level ``featherstore`` package:

.. code-block:: python

    from featherstore.exceptions import (
        FeatherStoreError,
        ColumnError,
        TableNotFoundError,
    )

All domain errors inherit from
:class:`~featherstore.exceptions.FeatherStoreError`. Category bases let you catch
related failures together:

* :class:`~featherstore.exceptions.TableError`
* :class:`~featherstore.exceptions.StoreError`
* :class:`~featherstore.exceptions.ColumnError`
* :class:`~featherstore.exceptions.RowError`
* :class:`~featherstore.exceptions.IndexSchemaError`
* :class:`~featherstore.exceptions.DatabaseConnectionError`
* :class:`~featherstore.exceptions.SnapshotError`
* :class:`~featherstore.exceptions.PathError`

Example:

.. code-block:: python

    try:
        store.read_pandas("missing")
    except TableNotFoundError:
        print("create the table first")
    except FeatherStoreError as exc:
        print(f"other FeatherStore error: {exc}")

The full class list lives in :doc:`/API/Exceptions`.

Built-in exceptions for argument shape
--------------------------------------

Invalid types, missing required arguments, and similar programming mistakes still
raise ``TypeError``, ``ValueError``, or ``AttributeError``. For example,
:meth:`~featherstore.table.Table.drop` without ``rows`` or ``cols`` raises
``AttributeError``.

``errors=`` and ``warnings=``
-----------------------------

Many methods accept control flags:

+------------------+---------------------------+----------------------------------+
| Parameter        | Values                    | Typical meaning                  |
+==================+===========================+==================================+
| ``errors``       | ``'raise'``, ``'ignore'`` | Raise or overwrite / continue    |
+------------------+---------------------------+----------------------------------+
| ``warnings``     | ``'warn'``, ``'ignore'``  | Emit or suppress Python warnings |
+------------------+---------------------------+----------------------------------+

Examples of ``errors``:

* :meth:`~featherstore.table.Table.write` — existing table
* :func:`~featherstore.connection.create_database` — populated directory
* :func:`~featherstore.snapshot.restore_table` / ``restore_store`` — name clash

Examples of ``warnings``:

* unsorted index about to be sorted (write, append, insert)
* ``create_store`` when the store already exists
* drop helpers when the target does not exist

See also
--------

* :doc:`gotchas` — behaviors that look like errors until you know the rules
* :doc:`/API/Exceptions` — hierarchy and per-class docs
* :doc:`database`, :doc:`stores`, :doc:`snapshots` — where these flags appear
