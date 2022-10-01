from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._metadata import Metadata
from featherstore._table import common


def can_rename_columns(cols, new_col_names, table_path):
    Connection._raise_if_not_connected()
    _raise_if.table_not_exists(table_path)

    _raise_if.cols_argument_is_not_collection(cols)
    _raise_if.to_is_provided_twice(cols, new_col_names)
    _raise_if.to_not_provided(cols, new_col_names)

    if not isinstance(cols, dict):
        _raise_if.to_argument_is_not_sequence(new_col_names)
        _raise_if.length_of_cols_and_to_doesnt_match(cols, new_col_names)
    cols = common.format_cols_and_to_args(cols, new_col_names)

    _raise_if.cols_argument_items_is_not_str(cols.keys())
    _raise_if_new_cols_items_is_not_str(cols.values())

    _raise_if.col_names_contains_duplicates(cols.keys())
    _raise_if.cols_not_in_table(cols.keys(), table_path)
    _raise_if.index_in_cols(cols, table_path)
    _raise_if_renaming_causes_duplicates(cols, table_path)


def _raise_if_new_cols_items_is_not_str(new_cols):
    try:
        _raise_if.cols_argument_items_is_not_str(new_cols)
    except TypeError:
        raise TypeError("Elements in 'to' must be of type str")


def _raise_if_renaming_causes_duplicates(cols, table_path):
    stored_cols = Metadata(table_path, 'table')['columns']
    renamed_cols = _replace_col_names(stored_cols, cols)
    _raise_if.col_names_contains_duplicates(renamed_cols)


def rename_columns(df, cols):
    stored_cols = df.column_names
    new_cols = _replace_col_names(stored_cols, cols)
    df = df.rename_columns(new_cols)
    return df


def _replace_col_names(stored_cols, cols):
    renamed_cols = stored_cols.copy()
    for old_col, new_col in cols.items():
        idx = stored_cols.index(old_col)
        renamed_cols[idx] = new_col
    return renamed_cols


def write_metadata(df, table_path):
    first_partition = tuple(df.values())[0]
    col_names = first_partition.schema.names

    table_data = Metadata(table_path, 'table')
    table_data['columns'] = col_names
