import random

import pandas as pd
import pyarrow as pa
import polars as pl

from featherstore.table import DEFAULT_PARTITION_SIZE
from . import _utils
from .convert_table import _convert_to_arrow


def shuffle_cols(df):
    cols = _utils.get_col_names(df, index=False)
    shuffled_cols = random.sample(tuple(cols), len(cols))
    if isinstance(df, pa.Table):
        df = df.select(shuffled_cols)
    else:
        df = df[shuffled_cols]
    return df


def sort_table(df, *, by=None):
    index_name = by
    if isinstance(df, (pd.DataFrame, pd.Series)):
        df = df.sort_index()
    elif isinstance(df, pa.Table):
        if index_name:
            sorted_index = pa.compute.sort_indices(df[index_name])
            df = df.take(sorted_index)
    elif isinstance(df, (pl.DataFrame, pl.Series)):
        if index_name:
            if isinstance(df, pl.Series):
                df = df.sort()
            else:
                df = df.sort(by=index_name)
    return df


def get_partition_size(df, num_partitions=5):
    if num_partitions is None:
        return DEFAULT_PARTITION_SIZE
    elif num_partitions < 0:
        return -1

    if isinstance(df, pd.DataFrame):
        byte_size = sum(df[col].nbytes for col in df.columns)
        byte_size += df.index.nbytes
    elif isinstance(df, pd.Series):
        byte_size = df.nbytes + df.index.nbytes
    elif isinstance(df, (pl.DataFrame, pl.Series)):
        if isinstance(df, pl.Series):
            df = df.to_frame()
        df = df.to_arrow()
    if isinstance(df, pa.Table):
        byte_size = df.nbytes
        if _has_rangeindex(df):
            byte_size += 6 * df.shape[0]
    partition_size = byte_size // num_partitions
    return partition_size


def _has_rangeindex(df):
    try:
        index_type = df.schema.pandas_metadata['index_columns'][0]['kind']
        return index_type == 'range'
    except TypeError:
        return False


def format_arrow_table(df):
    if __index_in_columns(df):
        df = _utils.make_index_first_column(df)
    return df


def __index_in_columns(df):
    index_name = df.schema.pandas_metadata["index_columns"][0]
    return index_name in df.column_names


def drop_default_index_if_exists(df):
    if not df_has_default_index(df):
        return df

    df = df.drop([_utils.DEFAULT_ARROW_INDEX_NAME])
    if isinstance(df, pl.DataFrame):
        if df.shape[1] == 1:
            df = df.to_series()
    return df


def df_has_default_index(df):
    # convert to arrow table
    df = _convert_to_arrow(df, keep_index=True)
    if _utils.DEFAULT_ARROW_INDEX_NAME not in df.column_names:
        return False

    index = df[_utils.DEFAULT_ARROW_INDEX_NAME]
    return _utils.is_rangeindex(index)
