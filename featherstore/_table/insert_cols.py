import pyarrow.compute as pc

from featherstore.connection import Connection
from featherstore import _utils
from featherstore._table import _raise_if
from featherstore._table import _table_utils
from featherstore._table import common


def can_insert_columns(table, df, warnings):
    Connection._raise_if_not_connected()
    _utils.raise_if_warnings_argument_is_not_valid(warnings)

    _raise_if.table_not_exists(table)
    _raise_if.df_is_not_table_type(df, _table_utils.EDIT_TABLE_TYPES)

    table_data = table._table_data
    index_name = table_data["index_name"]
    cols = _table_utils.get_data_col_names(df, index_name=index_name)
    common.validate_incoming_table_schema(df, table_data, cols)

    _raise_if.index_in_cols(cols, table_data)
    _raise_if.cols_already_in_table(cols, table_data)
    _raise_if_num_rows_does_not_match(df, table_data)

    index = _table_utils.get_index_if_exists(df, index_name)
    _raise_if.index_values_contains_duplicates(index)


def _raise_if_num_rows_does_not_match(df, table_data):
    stored_table_length = table_data["num_rows"]
    new_cols_length = len(df)
    if new_cols_length != stored_table_length:
        raise IndexError(f"Length of new cols ({new_cols_length}) doesn't match "
                         f"length of stored data ({stored_table_length})")


def insert_columns(old_df, df, index):
    """Insert columns into an Arrow table without converting to Pandas."""
    index_name = _table_utils.get_index_name(old_df)
    _raise_if_indices_do_not_match(old_df, df, index_name)
    return _insert_cols(old_df, df, index, index_name)


def _raise_if_indices_do_not_match(old_df, df, index_name):
    old_index = old_df[index_name]
    new_index = df[index_name]
    if len(old_index) != len(new_index):
        raise ValueError("New and old indices doesn't match")
    indices_equal = pc.all(pc.equal(old_index, new_index))
    if not indices_equal.as_py():
        raise ValueError("New and old indices doesn't match")


def _insert_cols(old_df, df, index, index_name):
    new_cols = [c for c in df.column_names if c != index_name]
    result = old_df
    if index == -1:
        for col in new_cols:
            result = result.append_column(col, df[col])
    else:
        # Index column is stored at position 0; idx is among data columns.
        insert_at = index + 1
        for col in new_cols:
            result = result.add_column(insert_at, col, df[col])
            insert_at += 1
    return result
