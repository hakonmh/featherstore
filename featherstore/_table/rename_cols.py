from featherstore._table import _raise_if


def can_rename_columns(table, cols, new_col_names):
    _raise_if.not_connected_or_table_not_exists(table)

    cols = _raise_if.cols_and_to_arguments_are_not_valid(cols, new_col_names)

    _raise_if.cols_argument_items_is_not_str_or_none(cols.keys())
    _raise_if_new_cols_items_is_not_str(cols.values())

    _raise_if.col_names_contains_duplicates(cols.keys())
    _raise_if.cols_not_in_table(cols.keys(), table._table_data)
    _raise_if.index_in_cols(cols, table._table_data)
    _raise_if_renaming_causes_duplicates(cols, table._table_data)


def _raise_if_new_cols_items_is_not_str(new_cols):
    try:
        _raise_if.cols_argument_items_is_not_str_or_none(new_cols)
    except TypeError:
        raise TypeError("Elements in 'to' must be of type str")


def _raise_if_renaming_causes_duplicates(cols, table_data):
    stored_cols = table_data["columns"]
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


def write_metadata(table, df):
    first_partition = next(iter(df.values()))
    col_names = first_partition.schema.names
    table._table_data["columns"] = col_names
