from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._metadata import Metadata


def can_rename_columns(cols, new_col_names, table_path):
    Connection.is_connected()
    _raise_if.table_not_exists(table_path)

    _raise_if_cols_is_not_supported_dtype(cols)
    _raise_if_cols_items_is_not_str(cols)
    _raise_if_new_cols_provided_twice(cols, new_col_names)
    _raise_if_new_cols_is_not_provided(cols, new_col_names)

    if isinstance(cols, dict):
        new_col_names = list(cols.values())
        cols = list(cols.keys())
    else:
        _raise_if_new_cols_is_not_list(new_col_names)
        _raise_if.cols_argument_items_is_not_str(new_col_names)

    _raise_if.col_names_contains_duplicates(cols)
    _raise_if.cols_not_in_table(cols, table_path)

    _raise_if.col_names_are_forbidden(new_col_names)
    _raise_if_length_of_cols_and_new_cols_doesnt_match(cols, new_col_names)
    _raise_if_renaming_causes_duplicates(cols, new_col_names, table_path)


def _raise_if_cols_is_not_supported_dtype(cols):
    is_valid_col_format = isinstance(cols, (list, dict))
    if not is_valid_col_format:
        raise TypeError("'cols' must be either list or dict")


def _raise_if_cols_items_is_not_str(cols):
    if isinstance(cols, dict):
        old_cols = list(cols.keys())
        new_cols = list(cols.values())
        _raise_if.cols_argument_items_is_not_str(old_cols)
        _raise_if.cols_argument_items_is_not_str(new_cols)
    else:
        _raise_if.cols_argument_items_is_not_str(cols)


def _raise_if_new_cols_provided_twice(cols, new_cols):
    cols_is_dict = isinstance(cols, dict)
    new_cols_is_provided = new_cols is not None
    if cols_is_dict and new_cols_is_provided:
        raise AttributeError("New column names provided twice, check function "
                             "calling signature")


def _raise_if_new_cols_is_not_provided(cols, new_cols):
    cols_is_not_dict = not isinstance(cols, dict)
    if cols_is_not_dict and new_cols is None:
        raise AttributeError("New column names is not provided")


def _raise_if_new_cols_is_not_list(cols):
    is_valid_col_format = isinstance(cols, list)
    if not is_valid_col_format:
        raise TypeError("'to' must be of type list")


def _raise_if_length_of_cols_and_new_cols_doesnt_match(cols, new_cols):
    if len(cols) != len(new_cols):
        raise ValueError("Number of column names is not the same as the "
                         "number of new column names")


def _raise_if_renaming_causes_duplicates(cols, new_cols, table_path):
    cols = _format_col_args(cols, new_cols)
    stored_cols = Metadata(table_path, 'table')['columns']
    renamed_cols = _replace_col_names(stored_cols, cols)
    _raise_if.col_names_contains_duplicates(renamed_cols)


def rename_columns(df, cols, to):
    cols = _format_col_args(cols, to)
    stored_cols = df.column_names
    new_cols = _replace_col_names(stored_cols, cols)
    df = df.rename_columns(new_cols)
    return df


def _format_col_args(cols, to):
    if isinstance(cols, list):
        cols = dict(zip(cols, to))
    return cols


def _replace_col_names(stored_cols, new_cols):
    renamed_cols = stored_cols.copy()
    for old_col, new_col in new_cols.items():
        idx = stored_cols.index(old_col)
        renamed_cols[idx] = new_col
    return renamed_cols


def write_metadata(df, table_path):
    first_partition = tuple(df.values())[0]
    col_names = first_partition.schema.names

    table_data = Metadata(table_path, 'table')
    table_data['columns'] = col_names
