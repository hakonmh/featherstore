"""FeatherStore exception hierarchy.

See the :doc:`API reference </API/Exceptions>` for the full hierarchy and
per-class documentation.
"""


class FeatherStoreError(Exception):
    """Base class for all FeatherStore domain errors."""


class TableError(FeatherStoreError):
    """Base class for table-related errors."""


class TableNotFoundError(TableError):
    """Raised when a requested table does not exist."""


class TableAlreadyExistsError(TableError):
    """Raised when creating or restoring a table that already exists."""


class ForbiddenTableNameError(TableError):
    """Raised when a table name is reserved or not a valid path name.

    Reserved and invalid names include ``.metadata``, ``""``, ``"."``,
    ``".."``, and names containing ``/`` or ``\\``.
    """


class CannotDropAllRowsError(TableError):
    """Raised when a drop operation would remove every row."""


class CannotDropAllColumnsError(TableError):
    """Raised when a drop operation would remove every column."""


class PartitionCountMismatchError(TableError):
    """Raised when partition count does not match partition names."""


class StoreError(FeatherStoreError):
    """Base class for store-related errors."""


class StoreNotFoundError(StoreError):
    """Raised when a requested store does not exist."""


class StoreAlreadyExistsError(StoreError):
    """Raised when creating or restoring a store that already exists."""


class ForbiddenStoreNameError(StoreError):
    """Raised when a store name is reserved or not a valid path name.

    Reserved and invalid names include ``.featherstore``, ``""``, ``"."``,
    ``".."``, and names containing ``/`` or ``\\``.
    """


class StoreNotEmptyError(StoreError):
    """Raised when deleting a store that still contains tables."""


class ColumnError(FeatherStoreError):
    """Base class for column-related errors."""


class ColumnNotFoundError(ColumnError):
    """Raised when requested columns are not in the table."""


class ColumnAlreadyExistsError(ColumnError):
    """Raised when inserting columns whose names already exist."""


class ColumnMismatchError(ColumnError):
    """Raised when column names do not match the stored table."""


class ColumnDtypeMismatchError(ColumnError):
    """Raised when column dtypes are incompatible."""


class DuplicateColumnNamesError(ColumnError):
    """Raised when column names are not unique."""


class ColumnLengthMismatchError(ColumnError):
    """Raised when new column length does not match stored row count."""


class MultiTypeColumnError(ColumnError):
    """Raised when a column contains multiple dtypes."""


class RowError(FeatherStoreError):
    """Base class for row-related errors."""


class RowNotFoundError(RowError):
    """Raised when requested rows are not in the table."""


class RowAlreadyExistsError(RowError):
    """Raised when inserting rows that already exist."""


class AppendIndexError(RowError):
    """Raised when append index is not strictly after stored data."""


class IndexSchemaError(FeatherStoreError):
    """Base class for index schema errors."""


class IndexTypeMismatchError(IndexSchemaError):
    """Raised when index or row types do not match the stored index."""


class IndexNameMismatchError(IndexSchemaError):
    """Raised when index names do not match the stored table."""


class IndexNameInColumnsError(IndexSchemaError):
    """Raised when the index name appears among column names."""


class IndexNotInColumnsError(IndexSchemaError):
    """Raised when a specified index column is not in the data."""


class DuplicateIndexValuesError(IndexSchemaError):
    """Raised when index values are not unique."""


class UnsupportedIndexTypeError(IndexSchemaError):
    """Raised when the index type is not supported."""


class IndexMismatchError(IndexSchemaError):
    """Raised when indices do not match between old and new data."""


class MissingIndexError(IndexSchemaError):
    """Raised when an index is required but not provided."""


class DatabaseConnectionError(FeatherStoreError):
    """Base class for database connection errors."""


class NotConnectedError(DatabaseConnectionError):
    """Raised when FeatherStore is not connected to a database."""


class NotADatabaseError(DatabaseConnectionError):
    """Raised when connecting to a path that is not a database."""


class PopulatedDirectoryError(DatabaseConnectionError):
    """Raised when creating a database in a non-empty directory."""


class DatabaseNotEmptyError(DatabaseConnectionError):
    """Raised when deleting a database that still contains stores."""


class IncompatibleDatabaseVersionError(DatabaseConnectionError):
    """Raised when the database format versions are incompatible with this FeatherStore install."""


class SnapshotError(FeatherStoreError):
    """Base class for snapshot errors."""


class SnapshotNotFoundError(SnapshotError):
    """Raised when a snapshot file does not exist."""


class InvalidSnapshotError(SnapshotError):
    """Raised when a file is not a valid snapshot of the expected type."""


class SnapshotTargetNotFoundError(SnapshotError):
    """Raised when the snapshot source path does not exist."""


class PathError(FeatherStoreError):
    """Base class for path safety errors."""


class UnsafeDeletePathError(PathError):
    """Raised when attempting to delete files outside the database."""
