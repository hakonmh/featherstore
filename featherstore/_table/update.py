import pandas as pd

from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._table import _table_utils


def can_update_table(df, table_path):
    Connection.is_connected()

    _raise_if.table_not_exists(table_path)
    _raise_if.df_is_not_pandas_table((df))

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns.tolist()

    _raise_if.index_is_not_supported_dtype(df.index)
    _raise_if.index_values_contains_duplicates(df.index)
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if.col_names_are_forbidden(cols)
    _raise_if.index_dtype_not_same_as_stored_index(df, table_path)
    _raise_if.cols_not_in_table(cols, table_path)


def update_data(old_df, *, to):
    if isinstance(to, pd.Series):
        new_data = to.to_frame()
    else:
        new_data = to
    old_df = old_df.to_pandas()
    _raise_if_rows_is_not_in_old_data(old_df, new_data)

    new_data = _table_utils.coerce_col_dtypes(new_data, to=old_df)
    old_df.loc[new_data.index, new_data.columns] = new_data
    df = old_df
    return df


def _raise_if_rows_is_not_in_old_data(old_df, df):
    index = df.index
    old_index = old_df.index
    rows_not_in_old_df = not all(index.isin(old_index))
    if rows_not_in_old_df:
        raise ValueError(f"Some rows not in stored table")
