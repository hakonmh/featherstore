Exceptions
----------

All domain errors inherit from :class:`~featherstore.exceptions.FeatherStoreException`.
Category bases allow catching related errors together (for example,
:class:`~featherstore.exceptions.ColumnError` for any column-related failure).

Argument validation (invalid types, missing required arguments) still raises
built-in exceptions such as :class:`TypeError`, :class:`ValueError`, and
:class:`AttributeError`.

Hierarchy
^^^^^^^^^

* :class:`~featherstore.exceptions.FeatherStoreException`

  * :class:`~featherstore.exceptions.TableError`

    * :class:`~featherstore.exceptions.TableNotFoundError`
    * :class:`~featherstore.exceptions.TableAlreadyExistsError`
    * :class:`~featherstore.exceptions.ForbiddenTableNameError`
    * :class:`~featherstore.exceptions.CannotDropAllRowsError`
    * :class:`~featherstore.exceptions.CannotDropAllColumnsError`
    * :class:`~featherstore.exceptions.PartitionCountMismatchError`

  * :class:`~featherstore.exceptions.StoreError`

    * :class:`~featherstore.exceptions.StoreNotFoundError`
    * :class:`~featherstore.exceptions.StoreAlreadyExistsError`
    * :class:`~featherstore.exceptions.ForbiddenStoreNameError`
    * :class:`~featherstore.exceptions.StoreNotEmptyError`

  * :class:`~featherstore.exceptions.ColumnError`

    * :class:`~featherstore.exceptions.ColumnNotFoundError`
    * :class:`~featherstore.exceptions.ColumnAlreadyExistsError`
    * :class:`~featherstore.exceptions.ColumnMismatchError`
    * :class:`~featherstore.exceptions.ColumnDtypeMismatchError`
    * :class:`~featherstore.exceptions.DuplicateColumnNamesError`
    * :class:`~featherstore.exceptions.ColumnLengthMismatchError`
    * :class:`~featherstore.exceptions.MultiTypeColumnError`

  * :class:`~featherstore.exceptions.RowError`

    * :class:`~featherstore.exceptions.RowNotFoundError`
    * :class:`~featherstore.exceptions.RowAlreadyExistsError`
    * :class:`~featherstore.exceptions.AppendIndexError`

  * :class:`~featherstore.exceptions.IndexSchemaError`

    * :class:`~featherstore.exceptions.IndexTypeMismatchError`
    * :class:`~featherstore.exceptions.IndexNameMismatchError`
    * :class:`~featherstore.exceptions.IndexNameInColumnsError`
    * :class:`~featherstore.exceptions.IndexNotInColumnsError`
    * :class:`~featherstore.exceptions.DuplicateIndexValuesError`
    * :class:`~featherstore.exceptions.UnsupportedIndexTypeError`
    * :class:`~featherstore.exceptions.IndexMismatchError`
    * :class:`~featherstore.exceptions.MissingIndexError`

  * :class:`~featherstore.exceptions.DatabaseConnectionError`

    * :class:`~featherstore.exceptions.NotConnectedError`
    * :class:`~featherstore.exceptions.NotADatabaseError`
    * :class:`~featherstore.exceptions.PopulatedDirectoryError`
    * :class:`~featherstore.exceptions.DatabaseNotEmptyError`
    * :class:`~featherstore.exceptions.IncompatibleDatabaseVersionError`

  * :class:`~featherstore.exceptions.SnapshotError`

    * :class:`~featherstore.exceptions.SnapshotNotFoundError`
    * :class:`~featherstore.exceptions.InvalidSnapshotError`
    * :class:`~featherstore.exceptions.SnapshotTargetNotFoundError`

  * :class:`~featherstore.exceptions.PathError`

    * :class:`~featherstore.exceptions.UnsafeDeletePathError`

Exception reference
^^^^^^^^^^^^^^^^^^^

.. automodule:: featherstore.exceptions
   :members:
   :undoc-members:
   :show-inheritance:
