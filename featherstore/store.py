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

    If a store with that name already exists, a warning is issued unless
    ``warnings='ignore'``, and the existing store is returned. This method
    does not raise :exc:`~featherstore.exceptions.StoreAlreadyExistsError`.

    Parameters
    ----------
    store_name : str
        The name of the store to be created.
    warnings : str, optional
        Whether to warn if the store already exists. Can be ``'warn'`` or
        ``'ignore'``; ``'ignore'`` passes silently if the store already
        exists. Default is ``'warn'``.

    Returns
    -------
    Store
        The new or existing store.

    Raises
    ------
    :exc:`~featherstore.exceptions.NotConnectedError`
        If FeatherStore is not connected to a database.
    :exc:`~featherstore.exceptions.ForbiddenStoreNameError`
        If ``store_name`` is reserved or not a valid path name.
    :exc:`TypeError`
        If ``store_name`` is not a str.
    :exc:`ValueError`
        If ``warnings`` is not ``'warn'`` or ``'ignore'``.
    """
    _can_create_store(store_name, warnings)

    store_path = os.path.join(current_db(), store_name)
    if not os.path.exists(store_path):
        os.mkdir(store_path)
    return Store(store_name)


def rename_store(store_name, *, to):
    """Renames a store.

    Parameters
    ----------
    store_name : str
        The name of the store to be renamed.
    to : str
        The new name of the store.

    Raises
    ------
    :exc:`~featherstore.exceptions.NotConnectedError`
        If FeatherStore is not connected to a database.
    :exc:`~featherstore.exceptions.StoreNotFoundError`
        If the store does not exist.
    :exc:`~featherstore.exceptions.StoreAlreadyExistsError`
        If the new store name already exists.
    :exc:`~featherstore.exceptions.ForbiddenStoreNameError`
        If the new store name is reserved or not a valid path name.
    :exc:`TypeError`
        If ``store_name`` or ``to`` is not a str.
    """
    Store(store_name).rename(to=to)


def drop_store(store_name, *, warnings="warn"):
    """Deletes a store.

    *Warning*: You cannot delete a store containing tables. All tables must
    be deleted first.

    Parameters
    ----------
    store_name : str
        The name of the store to be deleted.
    warnings : str, optional
        Whether to warn if the store does not exist. Can be ``'warn'`` or
        ``'ignore'``. Default is ``'warn'``.

    Raises
    ------
    :exc:`~featherstore.exceptions.NotConnectedError`
        If FeatherStore is not connected to a database.
    :exc:`~featherstore.exceptions.StoreNotEmptyError`
        If the store still contains tables.
    :exc:`~featherstore.exceptions.ForbiddenStoreNameError`
        If ``store_name`` is reserved or not a valid path name.
    :exc:`TypeError`
        If ``store_name`` is not a str.
    :exc:`ValueError`
        If ``warnings`` is not ``'warn'`` or ``'ignore'``.
    """
    _can_drop_store(store_name, warnings)
    store_path = os.path.join(current_db(), store_name)
    if os.path.exists(store_path):
        _utils.delete_folder_tree(store_path, current_db())


def list_stores(*, like=None):
    """Lists stores in the database.

    Parameters
    ----------
    like : str, optional
        Filters store names with SQL wildcards. ``?`` matches one character
        and ``%`` matches any number of characters. Matching is
        case-insensitive. Default is ``None``.

    Returns
    -------
    list
        A list of the stores in the database.

    Raises
    ------
    :exc:`~featherstore.exceptions.NotConnectedError`
        If FeatherStore is not connected to a database.
    :exc:`TypeError`
        If ``like`` is not a str or ``None``.
    """
    _can_list(like)
    stores = _utils.list_stores(current_db, like=like)
    return stores


def store_exists(store_name):
    """Returns whether a store exists in the current database.

    Parameters
    ----------
    store_name : str
        The name of the store.

    Returns
    -------
    bool

    Raises
    ------
    :exc:`~featherstore.exceptions.NotConnectedError`
        If FeatherStore is not connected to a database.
    :exc:`~featherstore.exceptions.ForbiddenStoreNameError`
        If ``store_name`` is reserved or not a valid path name.
    :exc:`TypeError`
        If ``store_name`` is not a str.
    """
    Connection._raise_if_not_connected()
    _utils.raise_if_store_name_is_invalid(store_name)
    store_path = os.path.join(current_db(), store_name)
    return os.path.exists(store_path)


class Store:
    """Provides common table operations within a store.

    Stores are directories for organizing data in logical groups
    within your FeatherStore database.

    Attributes
    ----------
    name : str
        The name of the store. Updated when the store is renamed.
    """

    def __init__(self, store_name):
        """Selects an existing store.

        Parameters
        ----------
        store_name : str
            The name of the store to be selected.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenStoreNameError`
            If ``store_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``store_name`` is not a str.
        """
        _can_init_store(store_name)

        self.name = store_name
        self._store_path = os.path.join(current_db(), store_name)

    def rename(self, *, to):
        """Renames the current store.

        Parameters
        ----------
        to : str
            The new name of the store.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.StoreAlreadyExistsError`
            If the new store name already exists.
        :exc:`~featherstore.exceptions.ForbiddenStoreNameError`
            If the new store name is reserved or not a valid path name.
        :exc:`TypeError`
            If ``to`` is not a str.
        """
        new_store_name = to
        _can_rename_store(new_store_name)

        new_path = os.path.join(current_db(), new_store_name)
        os.rename(self._store_path, new_path)
        self.name = new_store_name
        self._store_path = new_path

    def drop(self, *, warnings="warn"):
        """Deletes the current store.

        *Warning*: You cannot delete a store containing tables. All tables must
        be deleted first.

        Parameters
        ----------
        warnings : str, optional
            Whether to warn if the store does not exist. Can be ``'warn'`` or
            ``'ignore'``. Default is ``'warn'``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.StoreNotEmptyError`
            If the store still contains tables.
        :exc:`~featherstore.exceptions.ForbiddenStoreNameError`
            If the store name is reserved or not a valid path name.
        :exc:`TypeError`
            If the store name is not a str.
        :exc:`ValueError`
            If ``warnings`` is not ``'warn'`` or ``'ignore'``.
        """
        drop_store(self.name, warnings=warnings)

    def list_tables(self, *, like=None):
        """Lists tables in the store.

        Parameters
        ----------
        like : str, optional
            Filters table names with SQL wildcards. ``?`` matches one character
            and ``%`` matches any number of characters. Matching is
            case-insensitive. Default is ``None``.

        Returns
        -------
        list
            A list of the tables in the store.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`TypeError`
            If ``like`` is not a str or ``None``.
        """
        _can_list(like)

        tables = os.listdir(self._store_path)
        if like:
            pattern = like
            tables = _utils.filter_items_like_pattern(tables, like=pattern)
        tables.sort()
        return tables

    def table_exists(self, table_name):
        """Returns whether a table exists in this store.

        Parameters
        ----------
        table_name : str
            The name of the table.

        Returns
        -------
        bool

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` is not a str.

        See Also
        --------
        :meth:`~featherstore.table.Table.exists`
        """
        return Table(table_name, self.name).exists()

    def read_arrow(self, table_name, *, cols=None, rows=None, mmap=None):
        """Reads a table as a PyArrow Table.

        Parameters
        ----------
        table_name : str
            The name of the table to read.
        cols : Collection, optional
            See :meth:`~featherstore.table.Table.read_arrow`.
        rows : Collection, optional
            See :meth:`~featherstore.table.Table.read_arrow`.
        mmap : bool, optional
            See :meth:`~featherstore.table.Table.read_arrow`.

        Returns
        -------
        pyarrow.Table

        Raises
        ------
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` is not a str.

        See Also
        --------
        :meth:`~featherstore.table.Table.read_arrow`
        """
        return Table(table_name, self.name).read_arrow(cols=cols, rows=rows, mmap=mmap)

    def read_pandas(self, table_name, *, cols=None, rows=None, mmap=None):
        """Reads a table as a pandas DataFrame or Series.

        Parameters
        ----------
        table_name : str
            The name of the table to read.
        cols : Collection, optional
            See :meth:`~featherstore.table.Table.read_pandas`.
        rows : Collection, optional
            See :meth:`~featherstore.table.Table.read_pandas`.
        mmap : bool, optional
            See :meth:`~featherstore.table.Table.read_pandas`.

        Returns
        -------
        pandas.DataFrame or pandas.Series

        Raises
        ------
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` is not a str.

        See Also
        --------
        :meth:`~featherstore.table.Table.read_pandas`
        """
        return Table(table_name, self.name).read_pandas(cols=cols, rows=rows, mmap=mmap)

    def read_polars(self, table_name, *, cols=None, rows=None, mmap=None):
        """Reads a table as a polars DataFrame or Series.

        Parameters
        ----------
        table_name : str
            The name of the table to read.
        cols : Collection, optional
            See :meth:`~featherstore.table.Table.read_polars`.
        rows : Collection, optional
            See :meth:`~featherstore.table.Table.read_polars`.
        mmap : bool, optional
            See :meth:`~featherstore.table.Table.read_polars`.

        Returns
        -------
        polars.DataFrame or polars.Series

        Raises
        ------
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` is not a str.

        See Also
        --------
        :meth:`~featherstore.table.Table.read_polars`
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
        """Writes a DataFrame to the current store as a partitioned table.

        Parameters
        ----------
        table_name : str
            The name of the table the DataFrame will be stored as.
        df : pandas.DataFrame or pandas.Series, polars.DataFrame or polars.Series, or pyarrow.Table
            See :meth:`~featherstore.table.Table.write`.
        index : str, optional
            See :meth:`~featherstore.table.Table.write`.
        partition_size : int, optional
            See :meth:`~featherstore.table.Table.write`.
        errors : str, optional
            See :meth:`~featherstore.table.Table.write`.
        warnings : str, optional
            See :meth:`~featherstore.table.Table.write`.

        Raises
        ------
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` is not a str.

        See Also
        --------
        :meth:`~featherstore.table.Table.write`
        """
        Table(table_name, self.name).write(
            df,
            index=index,
            errors=errors,
            warnings=warnings,
            partition_size=partition_size,
        )

    def append_table(self, table_name, df, *, warnings="warn"):
        """Appends data to a table.

        Parameters
        ----------
        table_name : str
            The name of the table to append to.
        df : pandas.DataFrame or pandas.Series, polars.DataFrame or polars.Series, or pyarrow.Table
            See :meth:`~featherstore.table.Table.append`.
        warnings : str, optional
            See :meth:`~featherstore.table.Table.append`.

        Raises
        ------
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` is not a str.

        See Also
        --------
        :meth:`~featherstore.table.Table.append`
        """
        Table(table_name, self.name).append(df, warnings=warnings)

    def rename_table(self, table_name, *, to):
        """Renames a table.

        Parameters
        ----------
        table_name : str
            The name of the table to be renamed.
        to : str
            The new name of the table.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` or ``to`` is reserved or not a valid path name.
        :exc:`~featherstore.exceptions.TableAlreadyExistsError`
            If the new table name already exists.
        :exc:`TypeError`
            If ``table_name`` or ``to`` is not a str.

        See Also
        --------
        :meth:`~featherstore.table.Table.rename_table`
        """
        Table(table_name, self.name).rename_table(to=to)

    def drop_table(self, table_name, *, warnings="warn"):
        """Deletes a table.

        Parameters
        ----------
        table_name : str
            The name of the table to be deleted.
        warnings : str, optional
            Whether to warn if the table does not exist. Can be ``'warn'`` or
            ``'ignore'``. Default is ``'warn'``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` is not a str.
        :exc:`ValueError`
            If ``warnings`` is not ``'warn'`` or ``'ignore'``.

        See Also
        --------
        :meth:`~featherstore.table.Table.drop_table`
        """
        Table(table_name, self.name).drop_table(warnings=warnings)

    def select_table(self, table_name):
        """Selects a single table.

        Table objects have more features for editing stored tables.

        Parameters
        ----------
        table_name : str
            The name of the table to return.

        Returns
        -------
        Table

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` is not a str.

        See Also
        --------
        :class:`~featherstore.table.Table`
        """
        return Table(table_name, self.name)

    def create_snapshot(self, path):
        """Creates a compressed backup of the store.

        The store can later be restored with
        :func:`~featherstore.snapshot.restore_store`.

        Parameters
        ----------
        path : str
            Path to the snapshot archive. ``.tar.xz`` is appended unless
            ``path`` already ends with that suffix. An existing file at the
            resulting path is overwritten.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.SnapshotTargetNotFoundError`
            If the store path does not exist.
        :exc:`TypeError`
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
