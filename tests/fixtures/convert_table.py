import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

from . import _utils


def convert_table(df, *, to, index_name=None, as_series=None, keep_index=False):
    backend, series_from_to = _utils.parse_astype(to)
    if as_series is None:
        as_series = series_from_to

    if backend == "pandas":
        return _convert_to_pandas(df, index_name=index_name, as_series=as_series)
    if backend == "arrow":
        return _convert_to_arrow(df, keep_index=keep_index)
    if backend == "polars":
        return _convert_to_polars(df, as_series=as_series, keep_index=keep_index)
    raise ValueError(f"Unsupported conversion target {to!r}")


def _convert_to_pandas(df, index_name=None, as_series=False):
    if isinstance(df, pd.Series):
        return df
    if isinstance(df, pd.DataFrame):
        return _utils.squeeze_df(df) if as_series else df

    if isinstance(df, pl.Series):
        df = df.to_frame()
    df = df.to_pandas(date_as_object=False)

    df = __convert_object_cols_to_string(df)
    if isinstance(df.index, pd.DatetimeIndex):
        df.index.freq = df.index.inferred_freq

    if index_name and index_name in df.columns:
        df = df.set_index(index_name)
    elif DEFAULT_ARROW_INDEX_NAME in df.columns:
        df = df.set_index(DEFAULT_ARROW_INDEX_NAME)
    if df.index.name == DEFAULT_ARROW_INDEX_NAME:
        df.index.name = None

    if as_series:
        df = df.squeeze(axis=1)
    return df


def __convert_object_cols_to_string(df):
    for col in df.columns:
        if df[col].dtype.name == "object":
            try:
                if isinstance(df[col][0], str):
                    df[col] = df[col].astype("string")
            except KeyError:
                df[col] = df[col].astype("string")
    return df


def _convert_to_arrow(df, keep_index=False):
    if isinstance(df, (pd.Series, pl.Series)):
        df = df.to_frame()
    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    cols = df.column_names

    if DEFAULT_ARROW_INDEX_NAME in cols and not keep_index:
        index = df[DEFAULT_ARROW_INDEX_NAME]
        if _utils.is_rangeindex(index):
            cols.remove(DEFAULT_ARROW_INDEX_NAME)
    df = df.select(cols)
    return _utils.format_arrow_table(df)


def _convert_to_polars(df, as_series, keep_index=False):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        df = _convert_to_arrow(df, keep_index=keep_index)
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)
    return _utils.squeeze_df(df) if as_series else df
