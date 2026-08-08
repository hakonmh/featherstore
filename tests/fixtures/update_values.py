import copy

import pandas as pd
import polars as pl
import pyarrow as pa

from . import _utils
from .make_table import get_col_dtypes


def replace_values(df, index_name=None):
    """Replace data-column values with new samples of the same dtype.

    The index column is left unchanged.
    """
    if index_name is None:
        index_name = _utils.get_index_name(df)

    df = copy.copy(df)

    if isinstance(df, pd.Series):
        return _update_pandas_series(df)
    if isinstance(df, pd.DataFrame):
        return _update_pandas_dataframe(df, index_name)
    if isinstance(df, pl.DataFrame):
        return _update_polars_dataframe(df, index_name)
    if isinstance(df, pa.Table):
        return _update_arrow_table(df, index_name)
    raise TypeError(f"Unsupported type: {type(df)!r}")


def _update_pandas_series(series):
    series[:] = _regenerate_values(series.to_numpy())
    return series


def _update_pandas_dataframe(df, index_name):
    for col_name in _data_column_names(df, index_name):
        df[col_name] = _regenerate_values(df[col_name].to_numpy())
    return _utils.convert_object_cols_to_string(df)


def _update_polars_dataframe(df, index_name):
    for col_name in _data_column_names(df, index_name):
        updated = _regenerate_values(df[col_name].to_numpy())
        df = df.with_columns(pl.Series(col_name, updated))
    return df


def _update_arrow_table(table, index_name):
    for col_name in _data_column_names(table, index_name):
        updated = _regenerate_values(table[col_name].to_numpy())
        col_idx = table.column_names.index(col_name)
        table = table.set_column(col_idx, col_name, pa.array(updated))
    return table


def _data_column_names(df, index_name):
    return [
        name for name in _utils.get_col_names(df, index=False) if name != index_name
    ]


def _regenerate_values(values):
    dtype = _numpy_dtype_name(values)
    make_col = get_col_dtypes()[dtype]
    return make_col(values.shape[0])


def _numpy_dtype_name(values):
    kind_to_name = {
        "i": "int",
        "f": "float",
        "b": "bool",
        "O": "string",
        "M": "datetime",
    }
    name = kind_to_name[values.dtype.kind]
    if name == "int" and values.min() >= 0:
        return "uint"
    return name
