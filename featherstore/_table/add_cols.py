import pandas as pd

from featherstore._metadata import Metadata
from featherstore._table import common
from featherstore.connection import Connection
from featherstore._table import _raise_if


def can_add_columns(df, table_path):
    Connection.is_connected()

    _raise_if.table_not_exists(table_path)
    _raise_if.df_is_not_pandas_table(df)

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns.tolist()

    _raise_if.col_names_are_forbidden(cols)
    _raise_if_col_already_in_table(cols, table_path)
    _raise_if.index_dtype_not_same_as_index(df, table_path)
    _raise_if_num_rows_does_not_match(df, table_path)


def _raise_if_col_already_in_table(cols, table_path):
    table_metadata = Metadata(table_path, 'table')
    stored_cols = table_metadata["columns"]

    cols = common.filter_cols_if_like_provided(cols, stored_cols)
    some_cols_in_stored_cols = set(stored_cols) - (set(stored_cols) - set(cols))
    if some_cols_in_stored_cols:
        raise IndexError("Column already exists")


def _raise_if_num_rows_does_not_match(df, table_path):
    table_metadata = Metadata(table_path, 'table')
    stored_table_length = table_metadata["num_rows"]

    new_cols_length = len(df)

    if new_cols_length != stored_table_length:
        raise IndexError("Length of new cols doesnt match length of stored data")
