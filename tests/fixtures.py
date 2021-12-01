import pyarrow as pa
import polars as pl
import pandas as pd
import numpy as np
from pandas._testing import rands_array
from featherstore._table._table_utils import _get_col_names

ROWS = 30


def get_index_name(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        index_name = None
    else:
        cols = _get_col_names(df, has_default_index=False)
        if 'Date' in cols:
            index_name = 'Date'
        elif 'index' in cols:
            index_name = 'index'
        elif 'Index' in cols:
            index_name = 'index'
        elif "__index_level_0__" in cols:
            index_name = "__index_level_0__"
        else:
            index_name = None
    return index_name


def make_table(index=None, rows=ROWS, cols=5, *, astype="arrow"):
    random_data = {f"c{x}": np.random.random(size=rows) for x in range(cols)}
    df = pd.DataFrame(random_data)
    if index is not None:
        df.index = index(rows)

    if astype in ("arrow", "polars"):
        df = pa.Table.from_pandas(df, )
        if not _is_default_index(df):
            df = _make_index_first_column(df)
    if astype == "polars":
        df = pl.from_arrow(df)
    return df


def _is_default_index(df):
    index_data = df.schema.pandas_metadata["index_columns"][0]
    try:
        if index_data["name"] is None and index_data["kind"] == "range":
            is_default_index = True
        else:
            is_default_index = False
    except Exception:
        is_default_index = False
    return is_default_index


def _make_index_first_column(df):
    index_name = df.schema.pandas_metadata["index_columns"]
    column_names = df.column_names[:-1]
    columns = index_name + column_names
    df = df.select(columns)
    return df


def sorted_string_index(rows):
    str_length = 5
    index = rands_array(str_length, rows)
    index = np.sort(index)
    index = pd.Series(index)
    return index


def sorted_datetime_index(rows):
    index = pd.date_range(start="2021-01-01", periods=rows, freq="D")
    index = pd.Series(index, name="Date")
    return index


def hardcoded_string_index(rows):
    index = []
    for x in range(rows):
        x = format(x, '05d')
        index.append(f"row{x}")
    index = pd.Series(index)
    return index


def unsorted_int_index(rows):
    index = pd.RangeIndex(rows)
    index = pd.Series(index)
    index = index.sample(frac=1)
    return index


def unsorted_string_index(rows):
    str_length = 10
    index = set()
    while len(index) < rows:
        index = np.unique(rands_array(str_length, rows))
    index = pd.Series(index)
    return index


def unsorted_datetime_index(rows):
    index = pd.date_range(start="2021-01-01", periods=rows, freq="D")
    index = pd.Series(index, name="Date")
    index = index.sample(frac=1)
    return index


def get_partition_size(df, num_partitions):
    if isinstance(df, pd.DataFrame):
        byte_size = df.memory_usage(index=True).sum()
    elif isinstance(df, pd.Series):
        byte_size = df.memory_usage(index=True)
    elif isinstance(df, pl.DataFrame):
        byte_size = df.to_arrow().nbytes
    else:
        byte_size = df.nbytes
    partition_size = byte_size // num_partitions
    return partition_size
