import warnings as _warnings

from featherstore import _utils, store
from featherstore._table import _raise_if


def can_init_table(table_name, store_name):
    _raise_if.not_connected()
    _utils.raise_if_store_name_is_invalid(store_name)
    store._raise_if_store_not_exists(store_name)

    _raise_if.table_name_is_not_str(table_name)
    _raise_if.table_name_is_forbidden(table_name)


def can_rename_table(new_table_name, new_table_path):
    _raise_if.not_connected()

    _raise_if.table_name_is_not_str(new_table_name)
    _raise_if.table_name_is_forbidden(new_table_name)
    _raise_if.table_already_exists(new_table_path)


def can_drop_table(table, warnings):
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    if not table.exists() and warnings == "warn":
        _warnings.warn(f"Table '{table.name}' not found")


def can_reorder_columns(table, cols):
    _raise_if.not_connected_or_table_not_exists(table)

    _raise_if.cols_argument_is_not_list_like(cols)
    _raise_if.cols_argument_items_is_not_str_or_none(cols)
    _raise_if.index_in_cols(cols, table._table_data)
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if.provided_cols_do_not_match_stored(cols, table._table_data)
