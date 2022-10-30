import os

from featherstore import _utils
from featherstore.table import Table, DEFAULT_PARTITION_SIZE
from featherstore.connection import current_db, DB_MARKER_NAME, Connection
from featherstore.snapshot import _create_snapshot


def create_store(store_name, *, errors="raise"):
    """Creates a new store.

    Parameters
    ----------

    store_name : str
        The name of the store to be created
    errors : str, optional
        Whether or not to raise an error if the store already exist. Can be either
        `raise` or `ignore`, `ignore` passes if the store already exists, by
        default `raise`

    Returns
    -------
    Store
    """
    _can_create_store(store_name, errors)

    store_path = os.path.join(current_db(), store_name)
    store_already_exists = os.path.exists(store_path)
    if not store_already_exists:
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
    """
    Store(store_name).rename(to=to)


def drop_store(store_name, *, errors="raise"):
    """Deletes a store

    *Warning*: You can not delete a store containing tables. All tables must
    be deleted first.

    Parameters
    ----------
    store_name : str
        The name of the store to be deleted
    errors : str, optional
        Whether or not to raise an error if the store doesn't exist. Can be either
        `raise` or `ignore`, by default `raise`
    """
    _can_drop_store(store_name, errors)
    store_path = os.path.join(current_db(), store_name)
    _utils.delete_folder_tree(store_path)


def list_stores(*, like=None):
    """Lists stores in database

    Parameters
    ----------
    like : str, optional
        Filters out stores not matching string pattern, by default `None`.

        There are two wildcards that can be used in conjunction with `like`:

        - Question mark (`?`) matches any single character
        - The percent sign (`%`) matches any number of any characters

    Returns
    -------
    List
        A list of the tables in the store
    """
    _can_list(like)

    database_content = os.listdir(current_db())
    if like:
        pattern = like
        database_content = _utils.filter_items_like_pattern(database_content, like=pattern)
    stores = []
    for item in database_content:
        path = os.path.join(current_db(), item)
        if os.path.isdir(path):
            stores.append(item)
    stores.sort()
    return stores


def store_exists(store_name):
    store_path = os.path.join(current_db(), store_name)
    return os.path.exists(store_path)


class Store:
    def __init__(self, store_name):
        """A class for doing basic tasks with tables within a store.

        Stores are directories for organizing data in logical groups
        within your FeatherStore database.

        Parameters
        ----------
        store_name : str
            The name of the store to be selected
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
        """
        new_store_name = to
        _can_rename_store(new_store_name)

        new_path = os.path.join(current_db(), new_store_name)
        os.rename(self._store_path, new_path)
        self.name = new_store_name
        self._store_path = new_path

    def drop(self, *, errors="raise"):
        """Deletes the current store

        *Warning*: You can not delete a store containing tables. All tables must
        be deleted first.

        Parameters
        ----------
        errors : str, optional
            Whether or not to raise an error if the store doesn't exist. Can be either
            `raise` or `ignore`, by default `raise`
        """
        drop_store(self.name, errors=errors)

    def list_tables(self, *, like=None):
        """Lists tables in store

        Parameters
        ----------
        like : str, optional
            Filters out tables not matching string pattern, by default None.

            There are two wildcards that can be used in conjunction with `like`:

            - Question mark (`?`) matches any single character
            - The percent sign (`%`) matches any number of any characters

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

    def read_arrow(self, table_name, *, cols=None, rows=None):
        """Reads PyArrow Table from store

        Parameters
        ----------
        table_name : str
            The name of the table
        cols : list, optional
            list of column names or, filter-predicates in the form of
            `[like, pattern]`, by default `None`
        rows : list, optional
            list of index values or, filter-predicates in the form of
            `[keyword, value]`, where keyword can be either `before`, `after`,
            or `between`, by default `None`

        Returns
        -------
        pyarrow.Table
        """
        return Table(table_name, self.name).read_arrow(cols=cols, rows=rows)

    def read_pandas(self, table_name, *, cols=None, rows=None):
        """Reads Pandas DataFrame from store

        Parameters
        ----------
        table_name : str
            The name of the table
        cols : list, optional
            list of column names or, filter-predicates in the form of
            `[like, pattern]`, by default `None`
        rows : list, optional
            list of index values or, filter-predicates in the form of
            `[keyword, value]`, where keyword can be either `before`, `after`,
            or `between`, by default `None`

        Returns
        -------
        pandas.DataFrame or pandas.Series
        """
        return Table(table_name, self.name).read_pandas(cols=cols, rows=rows)

    def read_polars(self, table_name, *, cols=None, rows=None):
        """Reads Polars DataFrame from store

        Parameters
        ----------
        table_name : str
            The name of the table
        cols : list, optional
            list of column names or, filter-predicates in the form of
            `[like, pattern]`, by default `None`
        rows : list, optional
            list of index values or, filter-predicates in the form of
            `[keyword, value]`, where keyword can be either `before`, `after`,
            or `between`, by default `None`

        Returns
        -------
        polars.DataFrame
        """
        return Table(table_name, self.name).read_polars(cols=cols, rows=rows)

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

        The DataFrame index column, if provided, must be either of type int, str,
        or datetime. FeatherStore sorts the DataFrame by the index before storage.

        Parameters
        ----------
        table_name : str
            The name of the table the DataFrame will be stored as
        df : Pandas DataFrame or Series, Pyarrow Table, or Polars DataFrame
            The DataFrame to be stored
        index : str, optional
            The name of the column to be used as index. Uses current index for
            Pandas or a standard integer index for Arrow and Polars if `index` not
            provided, by default `None`
        partition_size : int, optional
            The size of each partition in bytes, by default 128 MB
        errors : str, optional
            Whether or not to raise an error if the table already exist. Can be either
            `raise` or `ignore`, `ignore` overwrites existing table, by default `raise`
        warnings : str, optional
            Whether or not to warn if a unsorted index is about to get sorted.
            Can be either `warn` or `ignore`, by default `warn`
        """
        Table(table_name, self.name).write(
            df,
            index=index,
            errors=errors,
            warnings=warnings,
            partition_size=partition_size,
        )

    def append_table(self, table_name, df, warnings="warn"):
        """Appends data to a table

        Parameters
        ----------
        table_name : str
            The name of the table you want to append to
        df : Pandas DataFrame or Series, Pyarrow Table, or Polars DataFrame
            The data to be appended
        warnings : str, optional
            Whether or not to warn if a unsorted index is about to get sorted.
            Can be either `warn` or `ignore`, by default `warn`
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
        """
        Table(table_name, self.name).rename_table(to=to)

    def drop_table(self, table_name):
        """Deletes a table

        Parameters
        ----------
        table_name : str
            The name of the table to be deleted
        """
        Table(table_name, self.name).drop_table()

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
        """
        _create_snapshot(path, self._store_path, 'store')


def _can_create_store(store_name, errors):
    Connection._raise_if_not_connected()
    _utils.raise_if_errors_argument_is_not_valid(errors)
    if errors == 'raise':
        _raise_if_store_already_exists(store_name)
    _raise_if_store_name_is_forbidden(store_name)


def _can_drop_store(store_name, errors):
    Connection._raise_if_not_connected()
    _utils.raise_if_errors_argument_is_not_valid(errors)
    if errors == "raise":
        _raise_if_store_not_exists(store_name)
    _raise_if_store_contains_tables(store_name)


def _raise_if_store_contains_tables(store_name):
    store_path = os.path.join(current_db(), store_name)
    store_exists = os.path.exists(store_path)
    if store_exists:
        store_content = os.listdir(store_path)
        store_is_empty = len(store_content) == 0
        if not store_is_empty:
            raise PermissionError("Can't delete a store that contains tables")


def _can_init_store(store_name):
    Connection._raise_if_not_connected()
    _raise_if_store_name_is_str(store_name)
    _raise_if_store_not_exists(store_name)
    _raise_if_store_name_is_forbidden(store_name)


def _can_rename_store(new_store_name):
    Connection._raise_if_not_connected()
    _raise_if_store_name_is_str(new_store_name)
    _raise_if_store_already_exists(new_store_name)
    _raise_if_store_name_is_forbidden(new_store_name)


def _raise_if_store_name_is_str(store_name):
    if not isinstance(store_name, str):
        raise TypeError(f"'store_name' must be a str, is type {type(store_name)}")


def _raise_if_store_name_is_forbidden(store_name):
    if store_name == DB_MARKER_NAME:
        raise ValueError(f"Store name {DB_MARKER_NAME} is forbidden")


def _raise_if_store_not_exists(store_name):
    store_path = os.path.join(current_db(), store_name)
    if not os.path.exists(store_path):
        raise FileNotFoundError(f"Store doesn't exists: '{store_name}'")


def _raise_if_store_already_exists(store_name):
    store_path = os.path.join(current_db(), store_name)
    if os.path.exists(store_path):
        raise OSError(f"A store with name {store_name} already exists")


def _can_list(like):
    Connection._raise_if_not_connected()
    _raise_if_like_is_not_str(like)


def _raise_if_like_is_not_str(like):
    if not isinstance(like, (str, type(None))):
        raise TypeError(
            f"'like' must be either of type str or None, is {type(like)}")
