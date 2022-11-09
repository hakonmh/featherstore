import os
import itertools
from string import ascii_lowercase
from featherstore._table._table_utils import get_col_names
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME, DB_MARKER_NAME
from featherstore._table.write import __is_rangeindex
from featherstore.table import DEFAULT_PARTITION_SIZE

import pyarrow as pa
import polars as pl
import pandas as pd
import numpy as np
from pandas._testing import rands_array

DB_MARKER_NAME = DB_MARKER_NAME  # Remove flake8 warning
DB_PATH = os.path.join('tests', '_db')
STORE_NAME = "test_store"
TABLE_NAME = "table_name"
TABLE_PATH = os.path.join(DB_PATH, STORE_NAME, TABLE_NAME)
MD_NAME = 'db'


def make_table(index=None, rows=30, cols=5, *, astype="arrow", dtype=None, **kwargs):
    df = _make_df(rows, cols, dtype=dtype)
    if index is not None:
        df.index = index(rows, **kwargs)
    df = _convert_df_to(df, to=astype)
    return df


def _make_df(rows, cols, dtype=None):
    col_dtypes = {
        'string': __make_string_col,
        'float': __make_float_col,
        'int': __make_int_col,
        'datetime': __make_datetime_col,
        'bool': __make_bool_column,
        'uint': __make_uint_col,
    }

    if dtype:
        col_dtypes = itertools.cycle([col_dtypes[dtype]])
    else:
        col_dtypes = itertools.cycle(col_dtypes.values())

    data = dict()
    for col in range(cols):
        col_dtype = next(col_dtypes)
        data[f"c{col}"] = col_dtype(rows)
    df = pd.DataFrame.from_dict(data)
    return df


def __make_float_col(rows):
    return np.random.random(size=rows)


def __make_uint_col(rows):
    return np.random.randint(0, 200000, size=rows)


def __make_int_col(rows):
    return np.random.randint(-100000, 100000, size=rows)


def __make_datetime_col(rows):
    start = -852076800  # 1943-01-01 in seconds relative to epoch
    end = 1640995200  # 2022-01-01 in seconds relative to epoch
    times_since_epoch = __random_unique_numbers(start, end, rows)
    dtime = times_since_epoch.astype('datetime64[s]')
    return dtime


def __random_unique_numbers(start, end, rows):
    df = np.random.randint(start, end, size=round(rows * 1.25), dtype=np.int32)
    df = np.unique(df)[:rows]
    return df


def __make_string_col(rows):
    str_length = 5
    df = rands_array(str_length, rows)
    df = pd.Series(df, dtype='string')
    return df


def __make_bool_column(rows):
    return np.random.randint(0, 2, size=rows, dtype=bool)


def _convert_df_to(df, *, to):
    astype = to
    if astype in ("arrow", "polars"):
        df = pa.Table.from_pandas(df)
        if not __is_default_index(df):
            df = __make_index_first_column(df)
    if astype == "polars":
        df = pl.from_arrow(df)
    return df


def __make_index_first_column(df):
    index_name = df.schema.pandas_metadata["index_columns"][0]
    cols = df.column_names
    cols.remove(index_name)
    cols.insert(0, index_name)
    df = df.select(cols)
    return df


def __is_default_index(df):
    index_data = df.schema.pandas_metadata["index_columns"][0]
    try:
        if index_data["name"] is None and index_data["kind"] == "range":
            is_default_index = True
        else:
            is_default_index = False
    except Exception:
        is_default_index = False
    return is_default_index


def default_index(rows):
    index = pd.RangeIndex(rows)
    return index


def sorted_string_index(rows):
    index = unsorted_string_index(rows)
    return index.sort_values()


def sorted_datetime_index(rows):
    index = __make_datetime_col(rows)
    index = pd.Index(index)
    index.name = 'Date'
    return index.sort_values()


def continuous_datetime_index(rows):
    index = pd.date_range(start="2021-01-01", periods=rows, freq="D")
    index = pd.Index(index)
    index.name = 'Date'
    return index


def continuous_string_index(rows, size=2):
    values = _make_continuous_alphabetic_str(size)
    index = []
    for idx, value in enumerate(values):
        if idx == rows:
            break
        index.append(value)
    return pd.Index(index)


def _make_continuous_alphabetic_str(size):
    for letters in itertools.product(ascii_lowercase, repeat=size):
        yield "".join(letters)


def unsorted_int_index(rows):
    index = np.random.default_rng().permutation(rows)
    index = pd.Index(index)
    return index


def unsorted_string_index(rows):
    index = __make_string_col(rows)
    index = np.unique(index)
    while len(index) < rows:
        new_rows = rows - len(index.unique())
        new_elements = __make_string_col(new_rows)
        index = np.append(index, new_elements)
        index = np.unique(index)
    return pd.Index(index)


def unsorted_datetime_index(rows):
    index = __make_datetime_col(rows)
    index = pd.Index(index)
    index.name = 'Date'
    return index


def get_index_name(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        index_name = None
    else:
        cols = get_col_names(df, has_default_index=False)
        if 'Date' in cols:
            index_name = 'Date'
        elif 'index' in cols:
            index_name = 'index'
        elif 'Index' in cols:
            index_name = 'index'
        elif DEFAULT_ARROW_INDEX_NAME in cols:
            index_name = DEFAULT_ARROW_INDEX_NAME
        else:
            index_name = None
    return index_name


def get_partition_size(df, num_partitions=5):
    if num_partitions == 0:
        return DEFAULT_PARTITION_SIZE
    elif num_partitions < 0:
        return -1

    if isinstance(df, pd.DataFrame):
        byte_size = df.memory_usage(index=True).sum()
    elif isinstance(df, pd.Series):
        byte_size = df.memory_usage(index=True)
    elif isinstance(df, pl.DataFrame):
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
        df = df.to_pandas()

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
        if __is_rangeindex(index):
            cols.remove(DEFAULT_ARROW_INDEX_NAME)
    df = df.select(cols)
    return df


def _convert_to_polars(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        df = _convert_to_arrow(df)
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)
    return df


def format_arrow_table(df):
    if __index_in_columns(df):
        df = __make_index_first_column(df)
    return df


def __index_in_columns(df):
    index_name = df.schema.pandas_metadata["index_columns"][0]
    return index_name in df.column_names
