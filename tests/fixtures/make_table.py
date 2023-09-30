import itertools
from string import ascii_letters, ascii_lowercase

import pandas as pd
import polars as pl
import pyarrow as pa
import numpy as np

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from . import _utils

RANDS_CHARS = np.array(list(ascii_letters + ' '))


def make_table(index=None, rows=30, cols=5, *, astype="arrow", dtype=None, **kwargs):
    df = _make_df(rows, cols, dtype=dtype)
    df = pd.DataFrame.from_dict(df)
    df = _utils.convert_object_cols_to_string(df)
    if index == default_index:
        index = None
    if index is not None:
        df.index = index(rows, **kwargs)
    df = _convert_df_to(df, to=astype)
    return df


def _make_df(rows, cols, dtype=None):
    col_dtypes = get_col_dtypes()

    if dtype:
        col_dtypes = itertools.cycle([col_dtypes[dtype]])
    else:
        col_dtypes = itertools.cycle(col_dtypes.values())

    data = dict()
    for col in range(cols):
        col_dtype = next(col_dtypes)
        data[f"c{col}"] = col_dtype(rows)

    return data


def get_col_dtypes():
    COL_DTYPES = {
        'string': _make_string_col,
        'float': _make_float_col,
        'int': _make_int_col,
        'datetime': _make_datetime_col,
        'bool': _make_bool_column,
        'uint': _make_uint_col,
    }
    return COL_DTYPES


def _make_float_col(rows):
    return np.random.random(size=rows)


def _make_uint_col(rows):
    return np.random.randint(0, 200000, size=rows)


def _make_int_col(rows):
    return np.random.randint(-100000, 100000, size=rows)


def _make_datetime_col(rows):
    start = -852076800  # 1943-01-01 in seconds relative to epoch
    end = 1640995200  # 2022-01-01 in seconds relative to epoch
    times_since_epoch = np.random.randint(start, end, size=rows, dtype=np.int32)
    dtime = times_since_epoch.astype('datetime64[ns]')
    return dtime


def _make_string_col(rows):
    STR_LENGTH = 5
    df = (np.random.choice(RANDS_CHARS, size=STR_LENGTH * rows)
          .view((np.str_, STR_LENGTH)).reshape(rows))
    return df


def _make_bool_column(rows):
    return np.random.randint(0, 2, size=rows, dtype=bool)


def _convert_df_to(df, *, to):
    astype = to
    if not astype.startswith('pandas'):
        df = pa.Table.from_pandas(df)
        if not __is_default_index(df):
            df = _utils.make_index_first_column(df)
    if astype.startswith("polars"):
        df = pl.from_arrow(df)

    if "[series]" in astype:
        df = _squeeze_df(df)
    return df


def _squeeze_df(df):
    if isinstance(df, pl.DataFrame):
        df = df.to_series()
    elif isinstance(df, pd.DataFrame):
        df = df.squeeze(axis=1)
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
    pass


def fake_default_index(rows):
    index = pd.Index(list(range(rows)))
    index.name = DEFAULT_ARROW_INDEX_NAME
    return index


def sorted_string_index(rows):
    index = unsorted_string_index(rows)
    return index.sort_values()


def sorted_datetime_index(rows):
    index = __make_unique_datetime_col(rows)
    index = pd.Index(index)
    index.name = 'Date'
    return index.sort_values()


def __make_unique_datetime_col(rows):
    start = -852076800  # 1943-01-01 in seconds relative to epoch
    end = 1640995200  # 2022-01-01 in seconds relative to epoch
    times_since_epoch = __random_unique_numbers(start, end, rows)
    dtime = times_since_epoch.astype('datetime64[s]')
    return dtime


def __random_unique_numbers(start, end, rows):
    df = np.random.randint(start, end, size=round(rows * 1.25), dtype=np.int32)
    df = np.unique(df)[:rows]
    return df


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
    index = _make_string_col(rows)
    index = np.unique(index)
    while len(index) < rows:
        new_rows = rows - len(index.unique())
        new_elements = _make_string_col(new_rows)
        index = np.append(index, new_elements)
        index = np.unique(index)
    return pd.Index(index)


def unsorted_datetime_index(rows):
    index = __make_unique_datetime_col(rows)
    index = pd.Index(index)
    index.name = 'Date'
    return index
