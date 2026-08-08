import itertools
from decimal import Decimal
from string import ascii_letters, ascii_lowercase

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

from . import _utils

RANDS_CHARS = np.array(list(ascii_letters + " "))
TIME32_INDEX_NAME = "Time"


def make_table(
    index=None, rows=30, cols=5, *, astype="arrow", dtype=None, seed=None, **kwargs
):
    rng = np.random.default_rng(seed)
    df = _make_df(rows, cols, dtype=dtype, rng=rng)
    df = pd.DataFrame.from_dict(df)
    df = _utils.convert_object_cols_to_string(df)
    if index == default_index:
        index = None
    if index is not None:
        df.index = index(rows, **kwargs)
    df = _convert_df_to(df, to=astype)
    return df


def _make_df(rows, cols, dtype=None, rng=None):
    col_dtypes = get_col_dtypes()

    if dtype:
        col_dtypes = itertools.cycle([col_dtypes[dtype]])
    else:
        col_dtypes = itertools.cycle(col_dtypes.values())

    data = {}
    for col in range(cols):
        col_dtype = next(col_dtypes)
        data[f"c{col}"] = col_dtype(rows, rng)

    return data


def get_col_dtypes():
    COL_DTYPES = {
        "string": _make_string_col,
        "float": _make_float_col,
        "int": _make_int_col,
        "datetime": _make_datetime_col,
        "bool": _make_bool_column,
        "uint": _make_uint_col,
        "categorical": _make_categorical_cols,
    }
    return COL_DTYPES


def _make_float_col(rows, rng=None):
    rng = np.random.default_rng() if rng is None else rng
    return rng.random(size=rows)


def _make_uint_col(rows, rng=None):
    rng = np.random.default_rng() if rng is None else rng
    return rng.integers(0, 200000, size=rows)


def _make_int_col(rows, rng=None):
    rng = np.random.default_rng() if rng is None else rng
    return rng.integers(-100000, 100000, size=rows)


def _make_categorical_cols(rows, rng=None):
    rng = np.random.default_rng() if rng is None else rng
    return pd.cut(
        rng.random(size=rows),
        (-np.inf, -0.5, 0.5, np.inf),
        labels=["low", "med", "high"],
    )


def _make_datetime_col(rows, rng=None):
    rng = np.random.default_rng() if rng is None else rng
    start = -852076800  # 1943-01-01 in seconds relative to epoch
    end = 1640995200  # 2022-01-01 in seconds relative to epoch
    times_since_epoch = rng.integers(start, end, size=rows, dtype=np.int32)
    dtime = times_since_epoch.astype("datetime64[ns]")
    return dtime


def _make_string_col(rows, rng=None):
    rng = np.random.default_rng() if rng is None else rng
    STR_LENGTH = 5
    df = (
        rng.choice(RANDS_CHARS, size=STR_LENGTH * rows)
        .view((np.str_, STR_LENGTH))
        .reshape(rows)
    )
    return df


def _make_bool_column(rows, rng=None):
    rng = np.random.default_rng() if rng is None else rng
    return rng.integers(0, 2, size=rows, dtype=bool)


def _convert_df_to(df, *, to):
    backend, as_series = _utils.parse_astype(to)
    if backend != "pandas":
        df = pa.Table.from_pandas(df)
        df = _utils.format_arrow_table(df)
        df = _cast_time32_index_if_needed(df)
    if backend == "polars":
        df = pl.from_arrow(df)
    if as_series:
        df = _utils.squeeze_df(df)
    return df


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
    index.name = "Date"
    return index.sort_values()


def __make_unique_datetime_col(rows):
    start = -852076800  # 1943-01-01 in seconds relative to epoch
    end = 1640995200  # 2022-01-01 in seconds relative to epoch
    times_since_epoch = __random_unique_numbers(start, end, rows)
    dtime = times_since_epoch.astype("datetime64[s]")
    return dtime


def __random_unique_numbers(start, end, rows):
    df = np.random.randint(start, end, size=round(rows * 1.25), dtype=np.int32)
    df = np.unique(df)[:rows]
    return df


def continuous_datetime_index(rows):
    index = pd.date_range(start="2021-01-01", periods=rows, freq="D")
    index = pd.Index(index)
    index.name = "Date"
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
    index = pd.Index(index, dtype=np.int64)
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
    index.name = "Date"
    return index


def sorted_timedelta_index(rows):
    index = pd.timedelta_range("1 day", periods=rows, freq="D")
    index = pd.Index(index, name="Timedelta")
    return index.sort_values()


def sorted_time32_index(rows):
    milliseconds = [(idx + 1) * 3_600_000 for idx in range(rows)]
    index = pd.Index(
        [pd.to_datetime(ms, unit="ms").time() for ms in milliseconds],
        name=TIME32_INDEX_NAME,
    )
    return index.sort_values()


def sorted_date32_index(rows):
    index = pd.date_range("2021-01-01", periods=rows, freq="D").date
    index = pd.Index(index, name="Date")
    return index.sort_values()


def sorted_float_index(rows):
    index = pd.Index(np.arange(rows, dtype=float) + 0.5, name="Float")
    return index.sort_values()


def sorted_uint_index(rows):
    index = pd.Index(np.arange(rows, dtype=np.uint32) + 100, name="UInt")
    return index.sort_values()


def sorted_decimal_index(rows):
    index = pd.Index([Decimal(f"{idx + 1}.1") for idx in range(rows)], name="Decimal")
    return index.sort_values()


def sorted_binary_index(rows):
    index = pd.Index(
        [bytes(f"{idx:02d}", "ascii") for idx in range(rows)], name="Binary"
    )
    return index.sort_values()


def sorted_large_string_index(rows):
    index = pd.Index(
        pd.array([f"idx_{idx:04d}" for idx in range(rows)], dtype="string"),
        name="LargeString",
    )
    return index.sort_values()


def _cast_time32_index_if_needed(df):
    index_name = df.schema.pandas_metadata["index_columns"][0]
    if index_name != TIME32_INDEX_NAME:
        return df

    col_idx = df.column_names.index(index_name)
    index_col = df[index_name].cast(pa.time32("ms"))
    return df.set_column(col_idx, index_name, index_col)
