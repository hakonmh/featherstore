

import pyarrow as pa
import polars as pl
import pandas as pd
import numpy as np
from pandas._testing import rands_array

ROWS = 30


def make_table(index=None, cols=5, *, astype="arrow"):
    random_data = {f"c{x}": np.random.random(size=ROWS) for x in range(cols)}
    df = pd.DataFrame(random_data)
    if index is not None:
        df.index = index()

    if astype in ("arrow", "polars"):
        df = pa.Table.from_pandas(df)
    if astype == "polars":
        df = pl.from_arrow(df)
    return df


def sorted_string_index():
    str_length = 5
    index = rands_array(str_length, ROWS)
    index = np.sort(index)
    index = pd.Series(index)
    return index


def sorted_datetime_index():
    index = pd.date_range(start="2021-01-01", periods=ROWS, freq="D")
    index = pd.Series(index, name="Date")
    return index


def unsorted_int_index():
    index = pd.RangeIndex(ROWS)
    index = pd.Series(index)
    index = index.sample(frac=1)
    return index


def unsorted_string_index():
    str_length = 10
    index = set()
    while len(index) < ROWS:
        index = np.unique(rands_array(str_length, ROWS))
    index = pd.Series(index)
    return index


def unsorted_datetime_index():
    index = pd.date_range(start="2021-01-01", periods=ROWS, freq="D")
    index = pd.Series(index, name="Date")
    index = index.sample(frac=1)
    return index


def get_partition_size(df, num_partitions):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        byte_size = df.memory_usage(index=True).sum()
    elif isinstance(df, pl.DataFrame):
        byte_size = df.to_arrow().nbytes
    else:
        byte_size = df.nbytes
    partition_size = byte_size // num_partitions
    return partition_size
