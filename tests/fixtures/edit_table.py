"""Mutate tables for test inputs and expected results.

Put helpers that edit table contents or structure in place—regenerating values,
merging updates, shuffling/sorting columns, renaming columns—here.
"""

import copy
import random

import pandas as pd
import polars as pl
import pyarrow as pa

from . import _utils
from .make_table import get_col_dtypes


def regenerate_values(df):
    df = copy.copy(df)
    index_name = _utils.get_index_name(df)

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
    for col_name in _utils.get_col_names(df, index_name=index_name):
        df[col_name] = _regenerate_values(df[col_name].to_numpy())
    return _utils.convert_object_cols_to_string(df)


def _update_polars_dataframe(df, index_name):
    for col_name in _utils.get_col_names(df, index_name=index_name):
        updated = _regenerate_values(df[col_name].to_numpy())
        df = df.with_columns(pl.Series(col_name, updated))
    return df


def _update_arrow_table(table, index_name):
    for col_name in _utils.get_col_names(table, index_name=index_name):
        updated = _regenerate_values(table[col_name].to_numpy())
        col_idx = table.column_names.index(col_name)
        table = table.set_column(col_idx, col_name, pa.array(updated))
    return table


def _regenerate_values(values):
    dtype = _numpy_dtype_name(values)
    return get_col_dtypes()[dtype](values.shape[0])


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


def update_table(df, update_df):
    expected = df.copy()
    expected.update(update_df)
    return expected


def shuffle_cols(df):
    data_cols = _utils.get_col_names(df)
    shuffled_cols = random.sample(tuple(data_cols), len(data_cols))
    index_name = _utils.get_index_name(df)

    if isinstance(df, pa.Table):
        cols = _index_then_cols(index_name, shuffled_cols, df.column_names)
        return df.select(cols)
    if isinstance(df, pl.DataFrame):
        cols = _index_then_cols(index_name, shuffled_cols, df.columns)
        return df.select(cols)
    return df[shuffled_cols]


def _index_then_cols(index_name, data_cols, col_names):
    if index_name and index_name in col_names:
        return [index_name, *data_cols]
    return data_cols


def sort_table(df, *, by=None):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        return df.sort_index()
    if isinstance(df, pa.Table) and by:
        sorted_index = pa.compute.sort_indices(df[by])
        return df.take(sorted_index)
    if isinstance(df, (pl.DataFrame, pl.Series)) and by:
        if isinstance(df, pl.Series):
            return df.sort()
        return df.sort(by=by)
    return df


def insert_column_names_at(df, col_names, insert_idx):
    columns = df.columns.tolist()
    start, end = _column_slice_for_insert(insert_idx, len(col_names))
    columns[start:end] = col_names
    df.columns = columns
    return df


def _column_slice_for_insert(insert_idx, num_cols):
    end = insert_idx + num_cols
    if insert_idx < 0:
        insert_idx = -num_cols
        end = None
    return insert_idx, end
