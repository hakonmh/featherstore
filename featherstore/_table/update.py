import pandas as pd

from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._table.common import (_check_index_constraints,
                                        _check_column_constraints,
                                        _coerce_column_dtypes)


def can_update_table(df, table_path):
    Connection.is_connected()

    _raise_if.table_not_exists(table_path)
    _raise_if.df_is_not_pandas_table((df))

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns.tolist()

    _check_index_constraints(df.index)
    _check_column_constraints(cols)
    _raise_if.index_dtype_not_same_as_index(df, table_path)
    _raise_if.cols_not_in_table(cols, table_path)


def update_data(old_df, *, to):
    if isinstance(to, pd.Series):
        new_data = to.to_frame()
    else:
        new_data = to
    old_df = old_df.to_pandas()
    _check_if_all_rows_is_in_old_data(old_df, new_data)

    new_data = _coerce_column_dtypes(new_data, to=old_df)
    old_df.loc[new_data.index, new_data.columns] = new_data
    df = old_df
    return df


def _check_if_all_rows_is_in_old_data(old_df, df):
    index = df.index
    old_index = old_df.index
    rows_not_in_old_df = not all(index.isin(old_index))
    if rows_not_in_old_df:
        raise ValueError(f"Some rows not in stored table")
