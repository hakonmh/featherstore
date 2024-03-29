import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from . import _utils


def convert_table(df, *, to, index_name=None, as_series=True):
    if to == 'pandas':
        df = _convert_to_pandas(df, index_name=index_name, as_series=as_series)
    elif to == 'arrow':
        df = _convert_to_arrow(df)
    elif to == "polars":
        df = _convert_to_polars(df, as_series=as_series)
    return df


def _convert_to_pandas(df, index_name=None, as_series=True):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        return df
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

    if isinstance(df, pd.DataFrame) and as_series and __can_be_squeezed(df):
        df = df.squeeze(axis=1)
    return df


def __convert_object_cols_to_string(df):
    for col in df.columns:
        if df[col].dtype.name == 'object':
            try:
                if isinstance(df[col][0], str):
                    df[col] = df[col].astype('string')
            except KeyError:
                df[col] = df[col].astype('string')
    return df


def __can_be_squeezed(df):
    num_cols = df.shape[1]
    return num_cols == 1


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
    return df


def _convert_to_polars(df, as_series):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        df = _convert_to_arrow(df)
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)

    if isinstance(df, pl.DataFrame) and as_series and __can_be_squeezed(df):
        df = df.to_series()
    return df
