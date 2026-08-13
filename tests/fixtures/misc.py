"""Miscellaneous fixture helpers that do not fit elsewhere.

Put small, standalone utilities here only when no other fixture module is
a better home.
"""

import os

import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore import _utils
from featherstore.table import DEFAULT_PARTITION_SIZE


class Paths:
    """Entry point for deleting files and trees via the production deletion stack."""

    def remove(self, path):
        _utils._remove_path(str(path))

    def rmtree(self, path):
        path = str(path)
        if os.path.exists(path):
            _utils.rmtree(path)


paths = Paths()


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
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    elif isinstance(df, pl.Series):
        df = df.to_frame().to_arrow()

    if isinstance(df, pa.Table):
        byte_size = df.nbytes
        if _has_rangeindex(df):
            byte_size += 6 * df.shape[0]
    partition_size = byte_size // num_partitions
    return partition_size


def _has_rangeindex(df):
    try:
        index_type = df.schema.pandas_metadata["index_columns"][0]["kind"]
        return index_type == "range"
    except TypeError:
        return False
