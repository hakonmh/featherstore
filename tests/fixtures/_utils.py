"""Shared low-level helpers used across fixture modules.

Put cross-cutting table utilities here (column/index names, dtype parsing,
Arrow formatting) rather than in the public test API.
"""

import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME


def parse_astype(astype):
    """Split a ``make_table``-style astype into ``(backend, as_series)``."""
    backend = astype.split("[", 1)[0]
    as_series = "[series]" in astype
    return backend, as_series


def squeeze_df(df):
    if isinstance(df, pl.DataFrame) and df.shape[1] == 1:
        return df.to_series()
    if isinstance(df, pd.DataFrame) and df.shape[1] == 1:
        return df.squeeze(axis=1)
    return df


def format_arrow_table(df):
    if _index_in_columns(df):
        df = _make_index_first_column(df)
    return df


def _make_index_first_column(df):
    index_name = df.schema.pandas_metadata["index_columns"][0]
    cols = df.column_names
    cols.remove(index_name)
    cols.insert(0, index_name)
    df = df.select(cols)
    return df


def _index_in_columns(df):
    metadata = df.schema.pandas_metadata
    if not metadata:
        return False
    index_columns = metadata.get("index_columns") or []
    if not index_columns:
        return False
    index_name = index_columns[0]
    return isinstance(index_name, str) and index_name in df.column_names


def is_rangeindex(index):
    rangeindex = pa.array(pd.RangeIndex(len(index)))
    TYPES_NOT_MATCHING = pa.lib.ArrowNotImplementedError
    try:
        is_rangeindex = pa.compute.equal(index, rangeindex)
        is_rangeindex = pa.compute.all(is_rangeindex).as_py()
    except TYPES_NOT_MATCHING:
        is_rangeindex = False

    return is_rangeindex


def get_col_names(df, index_name=None):
    if index_name is None:
        index_name = get_index_name(df)
    if index_name is None:
        index_name = DEFAULT_ARROW_INDEX_NAME
    cols = _raw_col_names(df)
    if isinstance(df, (pd.Series, pd.DataFrame)):
        return cols
    return [c for c in cols if c != index_name]


def get_index_name(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        index_name = None
    else:
        cols = _raw_col_names(df)
        if "Date" in cols:
            index_name = "Date"
        elif "index" in cols or "Index" in cols:
            index_name = "index"
        elif DEFAULT_ARROW_INDEX_NAME in cols:
            index_name = DEFAULT_ARROW_INDEX_NAME
        else:
            index_name = None
    return index_name


def _raw_col_names(df):
    if isinstance(df, pd.Series):
        return [df.name]
    if isinstance(df, pd.DataFrame):
        return df.columns.tolist()
    if isinstance(df, pl.DataFrame):
        return list(df.columns)
    if isinstance(df, pa.Table):
        return df.column_names
    return [df.name]


def convert_object_cols_to_string(df):
    for col in df.columns:
        if df[col].dtype.name == "object":
            try:
                if isinstance(df[col].iloc[0], str):
                    df[col] = df[col].astype("string")
            except (KeyError, IndexError):
                df[col] = df[col].astype("string")
    return df
