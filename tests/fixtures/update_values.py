import copy

import pandas as pd
import polars as pl
import pyarrow as pa

from .make_table import get_col_dtypes
from . import _utils


def update_values(df, index_name=None):
    if index_name is None:
        index_name = _utils.get_index_name(df)

    df = copy.copy(df)

    col_names = _utils.get_col_names(df, index=False)
    for col_name in col_names:
        if col_name == index_name:
            continue
        if isinstance(df, pd.Series):
            col = df.to_numpy()
        else:
            col = df[col_name].to_numpy()
        col = _update_col(col)

        if isinstance(df, pa.Table):
            col_idx = col_names.index(col_name)
            df = df.set_column(col_idx, col_name, pa.array(col))
        elif isinstance(df, pl.DataFrame):
            col = pl.Series(col_name, col)
            df = df.with_column(col)
        elif isinstance(df, pd.DataFrame):
            df[col_name] = col
        elif isinstance(df, pd.Series):
            df[:] = col
    if isinstance(df, pd.DataFrame):
        df = _utils.convert_object_cols_to_string(df)
    return df


def _update_col(df):
    dtype = __get_dtype(df)
    col_dtypes = get_col_dtypes()
    make_col = col_dtypes[dtype]
    rows = df.shape[0]
    return make_col(rows)


def __get_dtype(df):
    dtype = df.dtype.kind
    DTYPES = {'i': 'int', 'f': 'float', 'b': 'bool',
              'O': 'string', 'M': 'datetime'}
    dtype = DTYPES[dtype]
    if dtype == 'int':
        if df.min() >= 0:
            dtype = 'uint'
    return dtype
