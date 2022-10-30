from featherstore import store
from featherstore.connection import Connection
from featherstore._table import _raise_if


def can_init_table(table_name, store_name):
    Connection._raise_if_not_connected()
    store._raise_if_store_name_is_str(store_name)
    store._raise_if_store_not_exists(store_name)

    _raise_if.table_name_is_not_str(table_name)
    _raise_if.table_name_is_forbidden(table_name)


def can_rename_table(new_table_name, new_table_path):
    Connection._raise_if_not_connected()

    _raise_if.table_name_is_not_str(new_table_name)
    _raise_if.table_name_is_forbidden(new_table_path)
    _raise_if.table_already_exists(new_table_path)


def can_reorder_columns(table, cols):
    Connection._raise_if_not_connected()
    _raise_if.table_not_exists(table)

    _raise_if.cols_argument_is_not_list_like(cols)
    _raise_if.cols_argument_items_is_not_str(cols)
    _raise_if.index_in_cols(cols, table._table_data)
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if_cols_doesnt_match(cols, table._table_data)


def _raise_if_cols_doesnt_match(cols, table_data):
    stored_cols = table_data["columns"]
    index_name = table_data["index_name"]
    stored_cols.remove(index_name)

    cols_doesnt_match = set(stored_cols) != set(cols)
    if cols_doesnt_match:
        raise ValueError("The columns provided doesn't match the columns stored")
