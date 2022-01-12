from featherstore import store
from featherstore.connection import Connection
from featherstore._metadata import Metadata
from featherstore._table import _raise_if


def can_init_table(table_name, store_name):
    Connection.is_connected()
    store._raise_if_store_name_is_str(store_name)
    store._raise_if_store_not_exists(store_name)

    _raise_if.table_name_is_not_str(table_name)
    _raise_if.table_name_is_forbidden(table_name)


def can_rename_table(new_table_name, new_table_path):
    Connection.is_connected()

    _raise_if.table_name_is_not_str(new_table_name)
    _raise_if.table_name_is_forbidden(new_table_path)
    _raise_if.table_already_exists(new_table_path)


def can_reorganize_columns(cols, table_path):
    Connection.is_connected()
    _raise_if.table_not_exists(table_path)

    _raise_if_new_cols_is_not_list(cols)
    _raise_if.cols_argument_items_is_not_str(cols)
    _raise_if_index_in_cols(cols, table_path)
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if_cols_doesnt_match(cols, table_path)


def _raise_if_new_cols_is_not_list(cols):
    is_valid_col_format = isinstance(cols, list)
    if not is_valid_col_format:
        raise TypeError("'cols' must be of type list")


def _raise_if_index_in_cols(cols, table_path):
    index_name = Metadata(table_path, 'table')["index_name"]
    if index_name in cols:
        raise ValueError("Index name provided when trying to reorganize columns")


def _raise_if_cols_doesnt_match(cols, table_path):
    metadata = Metadata(table_path, 'table')
    stored_cols = metadata["columns"]
    index_name = metadata["index_name"]
    stored_cols.remove(index_name)

    cols_doesnt_match = set(stored_cols) != set(cols)
    if cols_doesnt_match:
        raise ValueError("The columns provided doesn't match the columns "
                         "stored")
