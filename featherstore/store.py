import os

from featherstore import _utils

from .table import Table
from .trash_bin import TrashBin
from .connection import connect, disconnect, current_db, DB_MARKER_NAME
from ._metadata import Metadata, METADATA_FOLDER_NAME
from ._utils import like_pattern_matching


def create_store(store_name, *, errors="raise"):
    """Creates a new store.

    Parameters
    ----------
    store_name : str
        The name of the store to be created
    errors : str, optional
        Whether or not to raise an error if the store already exist. Can be either
        'raise' or 'ignore', 'ignore' overwrites the existing store and its tables,
        by default "raise"
    """
    _can_create_store(store_name, errors)

    store_path = f"{current_db()}/{store_name}"
    store_already_exists = os.path.exists(store_path)
    if not store_already_exists:
        os.mkdir(store_path)
        Metadata(store_path).create()


def rename_store(store_name, new_store_name):
    """Renames a store

    Parameters
    ----------
    store_name : str
        The name of the store to be renamed
    new_store_name : str
        The new name of the store.
    """
    Store(store_name).rename(new_store_name)


def drop_store(store_name, *, errors="raise"):
    """Deletes a store

    Warning: You can not delete a store containing tables. All tables must
    be deleted first.

    Parameters
    ----------
    store_name : str
        The name of the store to be deleted
    errors : str, optional
        Whether or not to raise an error if the store doesn't exist. Can be either
        'raise' or 'ignore', by default "raise"
    """
    _can_drop_store(store_name, errors)
    store_path = f"{current_db()}/{store_name}"
    _utils.delete_folder_tree(store_path)


def list_stores(*, like=None):
    """Lists stores in database

    Parameters
    ----------
    like : str, optional
        Filters out stores not matching string pattern, by default None.
        There are two wildcards that can be used in conjunction with 'like':

        - The percent sign (%) represents zero, one, or multiple characters
        - The underscore sign (_) represents one, single character
    """
    _can_list_stores(like)

    database_content = os.listdir(current_db())
    if like:
        database_content = like_pattern_matching(like, database_content)
    stores = []
    for item in database_content:
        path = f"{current_db()}/{item}"
        if os.path.isdir(path):
            item_is_store = METADATA_FOLDER_NAME in os.listdir(path)
            if item_is_store:
                stores.append(item)
    return stores


class Store:
    def __init__(self, store_name):
        """The basic unit for organization in FeatherStore for reading and writing tables.

        Parameters
        ----------
        store_name : str
            The name of the store to be selected
        """
        _can_init_store(store_name)

        self.store_name = store_name
        self.store_path = f"{current_db()}/{store_name}"

    def rename(self, new_store_name):
        """Renames the current store

        Parameters
        ----------
        new_store_name : str
            The new name of the store.
        """
        _can_rename_store(new_store_name)

        new_path = f"{current_db()}/{new_store_name}"
        os.rename(self.store_path, new_path)
        self.store_name = new_store_name
        self.store_path = new_path

    def list_tables(self, *, like=None):
        """Lists tables in store

        Parameters
        ----------
        like : str, optional
            Filters out tables not matching string pattern, by default None.
            There are two wildcards that can be used in conjunction with 'like':

            - The percent sign (%) represents zero, one, or multiple characters
            - The underscore sign (_) represents one, single character
        """
        _can_list_tables(like)

        tables = os.listdir(self.store_path)
        tables.remove(METADATA_FOLDER_NAME)
        if like:
            tables = like_pattern_matching(like, tables)
        return tables

    def read_table(self, table_name, *, cols=None, rows=None):
        """Reads PyArrow Table from store

        Parameters
        ----------
        table_name : str
            The name of the table
        cols : list, optional
            list of column names or, filter-predicates in the form of
            [like, pattern], by default None
        rows : list, optional
            list of index values or, filter-predicates in the form of
            [keyword, value], where keyword can be either 'before', 'after',
            or 'between', by default None
        """
        return Table(table_name, self.store_name).read(cols=cols, rows=rows)

    def read_pandas(self, table_name, *, cols=None, rows=None):
        """Reads Pandas DataFrame from store

        Parameters
        ----------
        table_name : str
            The name of the table
        cols : list, optional
            list of column names or, filter-predicates in the form of
            [like, pattern], by default None
        rows : list, optional
            list of index values or, filter-predicates in the form of
            [keyword, value], where keyword can be either 'before', 'after',
            or 'between', by default None
        """
        return Table(table_name, self.store_name).read_pandas(cols=cols, rows=rows)

    def read_polars(self, table_name, *, cols=None, rows=None):
        """Reads Polars DataFrame from store

        Parameters
        ----------
        table_name : str
            The name of the table
        cols : list, optional
            list of column names or, filter-predicates in the form of
            [like, pattern], by default None
        rows : list, optional
            list of index values or, filter-predicates in the form of
            [keyword, value], where keyword can be either 'before', 'after',
            or 'between', by default None
        """
        return Table(table_name, self.store_name).read_polars(cols=cols, rows=rows)

    def write_table(
        self,
        table_name,
        df,
        /,
        index=None,
        *,
        partition_size=None,
        errors="raise",
        warnings="warn",
    ):
        """Writes a DataFrame to the current store as a partitioned table.

        The DataFrame index column, if provided, must be either of type int, str,
        or datetime. FeatherStore sorts the DataFrame by the index before storage.

        Parameters
        ----------
        df : Pandas DataFrame or Series, Pyarrow Table, or Polars DataFrame
            The DataFrame to be stored
        table_name : str
            The name of the table the DataFrame will be stored as
        index : str, optional
            The name of the column to be used as index. Uses current index for
            Pandas or a standard integer index for Arrow and Polars if 'index' not
            provided, by default None
        errors : str, optional
            Whether or not to raise an error if the table already exist. Can be either
            'raise' or 'ignore', 'ignore' overwrites existing table, by default 'raise'
        warnings : str, optional
            Whether or not to warn if a unsorted index is about to get sorted.
            Can be either 'warn' or 'ignore', by default 'warn'
        partition_size : int, optional
            The size of each partition in bytes, by default 128 MB
        """
        Table(table_name, self.store_name).write(
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
            Can be either 'warn' or 'ignore', by default 'warn'
        """
        Table(table_name, self.store_name).append(df, warnings=warnings)

    def rename_table(self, table_name, new_table_name):
        """Renames a table

        Parameters
        ----------
        table_name : str
            The name of the table to be renamed
        new_table_name : str
            The new name of the table.
        """
        Table(table_name, self.store_name).rename_table(new_table_name)

    def drop_table(self, table_name):
        """Deletes a table

        Parameters
        ----------
        table_name : str
            The name of the table to be deleted
        """
        Table(table_name, self.store_name).drop_table(table_name)

    def table(self, table_name):
        """Selects a Table object

        Table is a class for saving and loading DataFrames as partitioned Feather
        files. Tables supports several operations that can be done without loading
        in the full data:

        - Predicate Filtering when reading
        - Appends
        - Fetching Columns
        - Fetching Index

        It will also support the following operations down the line:

        - Inserts
        - Updates
        - Remove columns and rows
        - Changing types
        - Changing index

        Parameters
        ----------
        table_name : str
            The name of the table to be returned

        Returns
        -------
        Table
        """
        return Table(table_name, self.store_name)

    def _trash_bin(self):
        return TrashBin(self._connection_string)

    def _list_items_in_bin(self, *, like=None):
        raise NotImplementedError

    def _restore_from_bin(self, item_name):
        raise NotImplementedError

    def _delete_from_bin(self, item_name):
        raise NotImplementedError

    def _empty_bin(self):
        raise NotImplementedError


def _can_list_stores(like):
    current_db()
    if not isinstance(like, (str, type(None))):
        raise TypeError("'like' must be a str or None")


def _can_create_store(store_name, errors):
    current_db()
    _utils.check_if_arg_errors_is_valid(errors)

    store_path = f"{current_db()}/{store_name}"
    store_exists = os.path.exists(store_path)
    if store_exists and errors == "raise":
        raise FileExistsError("Store already exists")

    if store_name == DB_MARKER_NAME:
        raise ValueError("")


def _can_drop_store(store_name, errors):
    current_db()
    _utils.check_if_arg_errors_is_valid(errors)

    store_path = f"{current_db()}/{store_name}"
    store_exists = os.path.exists(store_path)
    if not store_exists and errors == "raise":
        raise FileExistsError("Store doesn't exists")

    if store_exists:
        store_content = os.listdir(store_path)
        store_is_empty = store_content == [".metadata"] or store_content == []
        if not store_is_empty:
            raise OSError("Can't delete a store that contains tables")


def _can_init_store(store_name):
    current_db()

    if not isinstance(store_name, str):
        TypeError(f"Store must be type int, is {type(store_name)}")

    store_path = f"{current_db()}/{store_name}"
    store_exists = os.path.exists(store_path)
    if not store_exists:
        raise FileNotFoundError(f"Store doesn't exists: '{store_name}'")

    folder_content = os.listdir(store_path)
    if METADATA_FOLDER_NAME not in folder_content:
        raise OSError(f"{store_name} is not a store folder")


def _can_rename_store(new_store_name):
    if not isinstance(new_store_name, str):
        raise TypeError(
            f"New store name must be of type str, is {type(new_store_name)}"
        )

    if new_store_name == DB_MARKER_NAME:
        raise ValueError(f"Table name {DB_MARKER_NAME} is forbidden")


def _can_list_tables(like):
    if not isinstance(like, (str, type(None))):
        raise TypeError(f"'like' must be either of type str or None, is {type(like)}")
