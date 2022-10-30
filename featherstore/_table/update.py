import pandas as pd

from featherstore.connection import Connection
from featherstore._table import _raise_if


def can_update_table(table, df):
    Connection._raise_if_not_connected()

    _raise_if.table_not_exists(table)
    _raise_if.df_is_not_pandas_table(df)

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns.tolist()

    _raise_if.index_name_not_same_as_stored_index(df, table._table_data)
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if.index_type_not_same_as_stored_index(df, table._table_data)
    _raise_if.index_values_contains_duplicates(df.index)
    _raise_if.cols_not_in_table(cols, table._table_data)


def update_data(old_df, *, to):
    if isinstance(to, pd.Series):
        new_data = to.to_frame()
    else:
        new_data = to
    old_df = old_df.to_pandas()
    _raise_if_rows_is_not_in_old_data(old_df, new_data)

    new_data = _coerce_pd_col_dtypes(new_data, to=old_df)
    old_df.loc[new_data.index, new_data.columns] = new_data
    df = old_df
    return df


def _raise_if_rows_is_not_in_old_data(old_df, df):
    index = df.index
    old_index = old_df.index
    rows_not_in_old_df = not all(index.isin(old_index))
    if rows_not_in_old_df:
        raise ValueError("Some rows not in stored table")


def _coerce_pd_col_dtypes(df, *, to):
    cols = df.columns
    dtypes = to[cols].dtypes
    try:
        df = df.astype(dtypes)
    except ValueError:
        raise TypeError("New and old column dtypes do not match")
    return df
