import warnings

import pandas as pd

from featherstore._table import _raise_if, _table_utils
from featherstore.exceptions import ColumnDtypeMismatchError, RowNotFoundError


def can_update_table(table, df):
    _raise_if.not_connected_or_table_not_exists(table)
    _raise_if.df_is_not_pandas_table(df)

    cols = _table_utils.get_pandas_column_names(df)
    _raise_if.df_index_or_column_names_incompatible_with_stored(
        df, table._table_data, cols
    )
    _raise_if.cols_not_in_table(cols, table._table_data)


def update_data(old_df, *, to):
    if isinstance(to, pd.Series):
        new_data = to.to_frame()
    else:
        new_data = to
    df = old_df.to_pandas()
    _raise_if_rows_is_not_in_old_data(df, new_data)

    new_data = _coerce_pd_col_dtypes(new_data, to=df)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df.loc[new_data.index, new_data.columns] = new_data
    return df


def _raise_if_rows_is_not_in_old_data(old_df, df):
    index = df.index
    old_index = old_df.index
    rows_not_in_old_df = not all(index.isin(old_index))
    if rows_not_in_old_df:
        missing = index[~index.isin(old_index)].tolist()
        raise RowNotFoundError(f"Some rows not in stored table ({missing})")


def _coerce_pd_col_dtypes(df, *, to):
    cols = df.columns
    dtypes = to[cols].dtypes
    try:
        df = df.astype(dtypes)
    except ValueError:
        raise ColumnDtypeMismatchError("New and old column dtypes do not match")
    return df
