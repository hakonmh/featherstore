import pyarrow as pa
import polars as pl
import pandas as pd

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from . import _utils


def convert_table(df, *, to, index_name=None, as_series=True):
    if to == 'pandas':
        df = _convert_to_pandas(df, index_name=index_name, as_series=as_series)
    elif to == 'arrow':
        df = _convert_to_arrow(df)
    elif to == "polars":
        df = _convert_to_polars(df)
    return df


def _convert_to_pandas(df, index_name=None, as_series=True):
    if isinstance(df, (pa.Table, pl.DataFrame)):
        df = df.to_pandas(date_as_object=False)
        df = __convert_object_cols_to_string(df)

        if index_name and index_name in df.columns:
            df = df.set_index(index_name)
        elif DEFAULT_ARROW_INDEX_NAME in df.columns:
            df = df.set_index(DEFAULT_ARROW_INDEX_NAME)
        if df.index.name == DEFAULT_ARROW_INDEX_NAME:
            df.index.name = None

    if as_series and isinstance(df, pd.DataFrame):
        df = df.squeeze()
    if isinstance(df, pd.Series):
        df = df.to_frame()
    return df


def __convert_object_cols_to_string(df):
    for col in df.columns:
        if df[col].dtype.name == 'object':
            if isinstance(df[col][0], str):
                df[col] = df[col].astype('string')
    return df


def _convert_to_arrow(df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    cols = df.column_names

    if DEFAULT_ARROW_INDEX_NAME in cols:
        index = df[DEFAULT_ARROW_INDEX_NAME]
        if _utils.is_rangeindex(index):
            cols.remove(DEFAULT_ARROW_INDEX_NAME)
    df = df.select(cols)
    return df


def _convert_to_polars(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        df = _convert_to_arrow(df)
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)
    return df
