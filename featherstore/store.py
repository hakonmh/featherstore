import os
import warnings as _warnings

from featherstore import _utils
from featherstore.connection import Connection, current_db
from featherstore.exceptions import (
    StoreAlreadyExistsError,
    StoreNotEmptyError,
    StoreNotFoundError,
)
from featherstore.snapshot import _create_snapshot
from featherstore.table import DEFAULT_PARTITION_SIZE, Table


def create_store(store_name, *, warnings="warn"):
    """Creates a new store.

    Parameters
    ----------
    store_name : str
        The name of the store to be created
    warnings : str, optional
        Whether to warn if the store already exists. Can be either
        `warn` or `ignore`; `ignore` passes silently if the store already
        exists. Default is `warn`.

    Returns
    -------
    Store

    Raises
    ------
    NotConnectedError
        If FeatherStore is not connected to a database.
    ForbiddenStoreNameError
        If ``store_name`` is reserved or not a valid path name.
    TypeError
        If ``store_name`` is not a str.
    ValueError
        If ``warnings`` is not ``'warn'`` or ``'ignore'``.
    """
    _can_create_store(store_name, warnings)

    store_path = os.path.join(current_db(), store_name)
    if not os.path.exists(store_path):
        os.mkdir(store_path)
    return Store(store_name)


def rename_store(store_name, *, to):
    """Renames a store

    Parameters
    ----------
    store_name : str
        The name of the store to be renamed.
    to : str
        The new name of the store.

    Raises
    ------
    NotConnectedError
        If FeatherStore is not connected to a database.
    StoreNotFoundError
        If the store does not exist.
    StoreAlreadyExistsError
        If the new store name already exists.
    ForbiddenStoreNameError
        If the new store name is reserved or not a valid path name.
    TypeError
        If ``store_name`` or ``to`` is not a str.
    """
    Store(store_name).rename(to=to)


def drop_store(store_name, *, warnings="warn"):
    """Deletes a store

    *Warning*: You cannot delete a store containing tables. All tables must
    be deleted first.

    Parameters
    ----------
    store_name : str
        The name of the store to be deleted
    warnings : str, optional
        Whether or not to warn if the store doesn't exist. Can be either
        `warn` or `ignore`, by default `warn`

    Raises
    ------
    NotConnectedError
        If FeatherStore is not connected to a database.
    StoreNotEmptyError
        If the store still contains tables.
    ForbiddenStoreNameError
        If ``store_name`` is reserved or not a valid path name.
    TypeError
        If ``store_name`` is not a str.
    ValueError
        If ``warnings`` is not ``'warn'`` or ``'ignore'``.
    """
    _can_drop_store(store_name, warnings)
    store_path = os.path.join(current_db(), store_name)
    if os.path.exists(store_path):
        _utils.delete_folder_tree(store_path, current_db())


def list_stores(*, like=None):
    """Lists stores in database

    Parameters
    ----------
    like : str, optional
        Filters out stores not matching string pattern, by default `None`.

        There are two wildcards that can be used in conjunction with `like`:

        - Question mark (`?`) matches any single character
        - The percent sign (`%`) matches any number of any characters

        Matching is case-insensitive.

    Returns
    -------
    List
        A list of the stores in the database
    """
    _can_list(like)
    stores = _utils.list_stores(current_db, like=like)
    return stores


def store_exists(store_name):
    Connection._raise_if_not_connected()
    _utils.raise_if_store_name_is_invalid(store_name)
    store_path = os.path.join(current_db(), store_name)
    return os.path.exists(store_path)


class Store:
    def __init__(self, store_name):
        """A class for common table operations within a store.

        Stores are directories for organizing data in logical groups
        within your FeatherStore database.

        Parameters
        ----------
        store_name : str
            The name of the store to be selected

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        StoreNotFoundError
            If the store does not exist.
        ForbiddenStoreNameError
            If ``store_name`` is reserved or not a valid path name.
        TypeError
            If ``store_name`` is not a str.
        """
        _can_init_store(store_name)

        self.name = store_name
        self._store_path = os.path.join(current_db(), store_name)

    def rename(self, *, to):
        """Renames the current store

        Parameters
        ----------
        to : str
            The new name of the store.

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        StoreAlreadyExistsError
            If the new store name already exists.
        ForbiddenStoreNameError
            If the new store name is reserved or not a valid path name.
        TypeError
            If ``to`` is not a str.
        """
        new_store_name = to
        _can_rename_store(new_store_name)

        new_path = os.path.join(current_db(), new_store_name)
        os.rename(self._store_path, new_path)
        self.name = new_store_name
        self._store_path = new_path

    def drop(self, *, warnings="warn"):
        """Deletes the current store

        *Warning*: You cannot delete a store containing tables. All tables must
        be deleted first.

        Parameters
        ----------
        warnings : str, optional
            Whether or not to warn if the store doesn't exist. Can be either
            `warn` or `ignore`, by default `warn`
        """
        drop_store(self.name, warnings=warnings)

    def list_tables(self, *, like=None):
        """Lists tables in store

        Parameters
        ----------
        like : str, optional
            Filters out tables not matching string pattern, by default None.

            There are two wildcards that can be used in conjunction with `like`:

            - Question mark (`?`) matches any single character
            - The percent sign (`%`) matches any number of any characters

            Matching is case-insensitive.

        Returns
        -------
        List
            A list of the tables in the store
        """
        _can_list(like)

        tables = os.listdir(self._store_path)
        if like:
            pattern = like
            tables = _utils.filter_items_like_pattern(tables, like=pattern)
        tables.sort()
        return tables

    def table_exists(self, table_name):
        return Table(table_name, self.name).exists()

    def read_arrow(self, table_name, *, cols=None, rows=None, mmap=None):
        """Reads PyArrow Table from store

        Parameters
        ----------
        cols : Collection, optional
            List of column names or filter predicates in the form of
            `{'like': pattern}`. If not provided, all columns are read.
        rows : Collection, optional
            List of index values or filter-predicates in the form of
            `{keyword: value}`, where keyword can be either `before`, `after`,
            or `between`. If not provided, all rows are read.
        mmap: bool, optional
            Use memory mapping when opening table on disk, by default `False` on
            Windows and `True` on other systems.

        Returns
        -------
        pyarrow.Table

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        StoreNotFoundError
            If the store does not exist.
        ForbiddenTableNameError
            If ``table_name`` is reserved or not a valid path name.
        TableNotFoundError
            If the table does not exist.
        ColumnNotFoundError
            If any requested column is not in the table.
        RowNotFoundError
            If any requested row is not in the table.
        IndexTypeMismatchError
            If row values do not match the table index dtype.
        TypeError
            If ``table_name``, ``cols``, or ``rows`` has an invalid type.
        ValueError
            If ``mmap`` is not a bool or ``None``.
        """
        return Table(table_name, self.name).read_arrow(cols=cols, rows=rows, mmap=mmap)

    def read_pandas(self, table_name, *, cols=None, rows=None, mmap=None):
        """Reads Pandas DataFrame or Series from store

        Parameters
        ----------
        cols : Collection, optional
            List of column names or filter predicates in the form of
            `{'like': pattern}`. If not provided, all columns are read.
        rows : Collection, optional
            List of index values or filter-predicates in the form of
            `{keyword: value}`, where keyword can be either `before`, `after`,
            or `between`. If not provided, all rows are read.
        mmap: bool, optional
            Use memory mapping when opening table on disk, by default `False` on
            Windows and `True` on other systems.

        Returns
        -------
        pandas.DataFrame or pandas.Series

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        StoreNotFoundError
            If the store does not exist.
        ForbiddenTableNameError
            If ``table_name`` is reserved or not a valid path name.
        TableNotFoundError
            If the table does not exist.
        ColumnNotFoundError
            If any requested column is not in the table.
        RowNotFoundError
            If any requested row is not in the table.
        IndexTypeMismatchError
            If row values do not match the table index dtype.
        TypeError
            If ``table_name``, ``cols``, or ``rows`` has an invalid type.
        ValueError
            If ``mmap`` is not a bool or ``None``.
        """
        return Table(table_name, self.name).read_pandas(cols=cols, rows=rows, mmap=mmap)

    def read_polars(self, table_name, *, cols=None, rows=None, mmap=None):
        """Reads Polars DataFrame or Series from store

        Parameters
        ----------
        cols : Collection, optional
            List of column names or filter predicates in the form of
            `{'like': pattern}`. If not provided, all columns are read.
        rows : Collection, optional
            List of index values or filter-predicates in the form of
            `{keyword: value}`, where keyword can be either `before`, `after`,
            or `between`. If not provided, all rows are read.
        mmap: bool, optional
            Use memory mapping when opening table on disk, by default `False` on
            Windows and `True` on other systems.

        Returns
        -------
        polars.DataFrame or polars.Series

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        StoreNotFoundError
            If the store does not exist.
        ForbiddenTableNameError
            If ``table_name`` is reserved or not a valid path name.
        TableNotFoundError
            If the table does not exist.
        ColumnNotFoundError
            If any requested column is not in the table.
        RowNotFoundError
            If any requested row is not in the table.
        IndexTypeMismatchError
            If row values do not match the table index dtype.
        TypeError
            If ``table_name``, ``cols``, or ``rows`` has an invalid type.
        ValueError
            If ``mmap`` is not a bool or ``None``.
        """
        return Table(table_name, self.name).read_polars(cols=cols, rows=rows, mmap=mmap)

    def write_table(
        self,
        table_name,
        df,
        /,
        index=None,
        *,
        partition_size=DEFAULT_PARTITION_SIZE,
        errors="raise",
        warnings="warn",
    ):
        """Writes a DataFrame to the current store as a partitioned table

        The DataFrame index, if provided, must be a supported type: integer,
        unsigned integer, float, decimal, string, binary, duration, or temporal
        (date, time, or timestamp). FeatherStore sorts the DataFrame by the
        index before storage.

        Parameters
        ----------
        table_name : str
            The name of the table the DataFrame will be stored as
        df : pandas DataFrame or Series, polars DataFrame or Series, or pyarrow Table
            The DataFrame to be stored
        index : str, optional
            The name of the column to be used as index. Uses current index for
            Pandas or a standard integer index for Arrow and Polars if `index` not
            provided, by default `None`
        partition_size : int, optional
            The size of each partition in bytes. A `partition_size` value of `-1`
            disables partitioning, by default 128 MB
        errors : str, optional
            Whether to raise an error if the table already exists. Can be either
            `raise` or `ignore`; `ignore` overwrites the existing table. Default is `raise`
        warnings : str, optional
            Whether or not to warn if an unsorted index is about to get sorted.
            Can be either `warn` or `ignore`, by default `warn`

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        StoreNotFoundError
            If the store does not exist.
        ForbiddenTableNameError
            If ``table_name`` is reserved or not a valid path name.
        TableAlreadyExistsError
            If ``errors='raise'`` and the table already exists.
        DuplicateColumnNamesError
            If column names are not unique.
        DuplicateIndexValuesError
            If index values are not unique.
        IndexNotInColumnsError
            If ``index`` is not among the table columns.
        UnsupportedIndexTypeError
            If the index type is not supported.
        MultiTypeColumnError
            If a column contains multiple dtypes.
        TypeError
            If arguments have invalid types.
        ValueError
            If ``errors`` or ``warnings`` is invalid.
        """
        Table(table_name, self.name).write(
            df,
            index=index,
            errors=errors,
            warnings=warnings,
            partition_size=partition_size,
        )

    def append_table(self, table_name, df, *, warnings="warn"):
        """Appends data to a table

        Parameters
        ----------
        table_name : str
            The name of the table you want to append to
        df : Pandas DataFrame or Series, Polars DataFrame or Series, or PyArrow Table
            The data to be appended
        warnings : str, optional
            Whether or not to warn if an unsorted index is about to get sorted.
            Can be either `warn` or `ignore`, by default `warn`

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        StoreNotFoundError
            If the store does not exist.
        ForbiddenTableNameError
            If ``table_name`` is reserved or not a valid path name.
        TableNotFoundError
            If the table does not exist.
        AppendIndexError
            If append index is not strictly after stored data.
        ColumnDtypeMismatchError
            If column dtypes are incompatible.
        ColumnMismatchError
            If column names do not match the stored table.
        DuplicateColumnNamesError
            If column names are not unique.
        DuplicateIndexValuesError
            If index values are not unique.
        IndexNameMismatchError
            If the index name does not match the stored table.
        IndexTypeMismatchError
            If the index type does not match the stored table.
        MissingIndexError
            If an index is required but not provided.
        TypeError
            If arguments have invalid types.
        ValueError
            If ``warnings`` is invalid.
        """
        Table(table_name, self.name).append(df, warnings=warnings)

    def rename_table(self, table_name, *, to):
        """Renames a table

        Parameters
        ----------
        table_name : str
            The name of the table to be renamed
        to : str
            The new name of the table.

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        StoreNotFoundError
            If the store does not exist.
        ForbiddenTableNameError
            If ``table_name`` or ``to`` is reserved or not a valid path name.
        TableAlreadyExistsError
            If the new table name already exists.
        TypeError
            If ``table_name`` or ``to`` is not a str.
        """
        Table(table_name, self.name).rename_table(to=to)

    def drop_table(self, table_name, *, warnings="warn"):
        """Deletes a table

        Parameters
        ----------
        table_name : str
            The name of the table to be deleted
        warnings : str, optional
            Whether or not to warn if the table doesn't exist. Can be either
            `warn` or `ignore`, by default `warn`
        """
        Table(table_name, self.name).drop_table(warnings=warnings)

    def select_table(self, table_name):
        """Selects a single table.

        Table objects have more features for editing stored tables.

        Parameters
        ----------
        table_name : str
            The name of the table to be returned

        Returns
        -------
        Table
        """
        return Table(table_name, self.name)

    def create_snapshot(self, path):
        """Creates a compressed backup of the store.

        The store can later be restored by using `snapshot.restore_store()`.

        Parameters
        ----------
        path : str
            The path to the snapshot archive.

        Raises
        ------
        NotConnectedError
            If FeatherStore is not connected to a database.
        SnapshotTargetNotFoundError
            If the store path does not exist.
        TypeError
            If ``path`` is not a str.
        """
        _create_snapshot(path, self._store_path, "store")


def _can_create_store(store_name, warnings):
    Connection._raise_if_not_connected()
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _utils.raise_if_store_name_is_invalid(store_name)
    store_path = os.path.join(current_db(), store_name)
    if os.path.exists(store_path) and warnings == "warn":
        _warnings.warn(f"A store with name {store_name} already exists")


def _can_drop_store(store_name, warnings):
    Connection._raise_if_not_connected()
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _utils.raise_if_store_name_is_invalid(store_name)
    _raise_if_store_contains_tables(store_name)
    store_path = os.path.join(current_db(), store_name)
    if not os.path.exists(store_path) and warnings == "warn":
        _warnings.warn(f"Store doesn't exist: '{store_name}'")


def _raise_if_store_contains_tables(store_name):
    store_path = os.path.join(current_db(), store_name)
    store_exists = os.path.exists(store_path)
    if store_exists:
        store_content = os.listdir(store_path)
        store_is_empty = len(store_content) == 0
        if not store_is_empty:
            raise StoreNotEmptyError("Can't delete a store that contains tables")


def _can_init_store(store_name):
    Connection._raise_if_not_connected()
    _utils.raise_if_store_name_is_invalid(store_name)
    _raise_if_store_not_exists(store_name)


def _can_rename_store(new_store_name):
    Connection._raise_if_not_connected()
    _utils.raise_if_store_name_is_invalid(new_store_name)
    _raise_if_store_already_exists(new_store_name)


def _raise_if_store_not_exists(store_name):
    store_path = os.path.join(current_db(), store_name)
    if not os.path.exists(store_path):
        raise StoreNotFoundError(f"Store doesn't exists: '{store_name}'")


def _raise_if_store_already_exists(store_name):
    store_path = os.path.join(current_db(), store_name)
    if os.path.exists(store_path):
        raise StoreAlreadyExistsError(f"A store with name {store_name} already exists")


def _can_list(like):
    Connection._raise_if_not_connected()
    _raise_if_like_is_not_str(like)


def _raise_if_like_is_not_str(like):
    if not isinstance(like, (str, type(None))):
        raise TypeError(f"'like' must be either of type str or None, is {type(like)}")
