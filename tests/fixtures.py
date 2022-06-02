import os
from featherstore._table._table_utils import get_col_names

import pyarrow as pa
import polars as pl
import pandas as pd
import numpy as np
from pandas._testing import rands_array

DB_PATH = os.path.join('tests', '_db')
STORE_NAME = "test_store"
TABLE_NAME = "table_name"
TABLE_PATH = os.path.join(DB_PATH, STORE_NAME, TABLE_NAME)


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
        elif "__index_level_0__" in cols:
            index_name = "__index_level_0__"
        else:
            index_name = None
    return index_name


def make_table(index=None, rows=30, cols=5, *, astype="arrow", dtype=None):
    df = _make_df(rows, cols, dtype=dtype)
    if index is not None:
        df.index = index(rows)
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
    if not dtype:
        col_dtypes = tuple(col_dtypes.values())

    random_data = dict()
    for col in range(cols):
        if not dtype:
            idx = col % len(col_dtypes)
        else:
            idx = dtype
        random_data[f"c{col}"] = col_dtypes[idx](rows)

    df = pd.DataFrame(random_data)
    return df


def __make_float_col(rows):
    return np.random.random(size=rows)


def __make_uint_col(rows):
    df = __make_int_col(rows)
    return np.abs(df)


def __make_int_col(rows):
    return np.random.randint(-100000, 100000, rows)


def __make_datetime_col(rows):
    start = pd.to_datetime('1943-01-01')
    start = start.value // 10**9

    end = pd.to_datetime('2022-01-01')
    end = end.value // 10**9

    return pd.to_datetime(__random_unique_randint(start, end, rows), unit='s')


def __random_unique_randint(start, end, rows):
    df = pd.Series(dtype=int)
    while len(df) < rows:
        new_rows = rows - len(df)
        df1 = np.random.randint(start, end, size=new_rows)
        df1 = pd.Series(df1)
        df = df.append(df1)
        df = pd.Series(df.unique())
    return df.to_numpy()


def __make_string_col(rows):
    str_length = 5
    df = rands_array(str_length, rows)
    df = pd.Series(df).astype("string")
    return df


def _convert_df_to(df, *, to):
    astype = to
    if astype in ("arrow", "polars"):
        df = pa.Table.from_pandas(df)
        if not __is_default_index(df):
            df = __make_index_first_column(df)
    if astype == "polars":
        df = pl.from_arrow(df)
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


def __make_index_first_column(df):
    index_name = df.schema.pandas_metadata["index_columns"]
    column_names = df.column_names[:-1]
    columns = index_name + column_names
    df = df.select(columns)
    return df


def __make_bool_column(rows):
    df = np.random.randint(0, 2, size=rows)
    df = pd.Series(df)
    df = df.astype(bool)
    return df


def default_index(rows):
    index = pd.RangeIndex(rows)
    return index


def sorted_string_index(rows):
    index = unsorted_string_index(rows)
    return index.sort_values()


def sorted_datetime_index(rows):
    index = __make_datetime_col(rows)
    index = pd.Series(index, name="Date")
    return index.sort_values()


def hardcoded_datetime_index(rows):
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
    index = default_index(rows)
    index = pd.Series(index)
    index = index.sample(frac=1)
    return index


def unsorted_string_index(rows):
    index = __make_string_col(rows)
    while len(index.unique()) < rows:
        new_rows = rows - len(index.unique())
        new_elements = __make_string_col(new_rows)
        index = index.append(new_elements)
    index = index.unique()
    index = pd.Series(index)
    return index


def unsorted_datetime_index(rows):
    index = __make_datetime_col(rows)
    return pd.Series(index, name="Date")


def get_partition_size(df, num_partitions=5):
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
    if isinstance(df, pd.DataFrame):
        pd_df = df
    elif isinstance(df, pd.Series):
        pd_df = df.to_frame()
    elif isinstance(df, (pa.Table, pl.DataFrame)):
        pd_df = df.to_pandas()

    pd_df = pd_df.convert_dtypes()  # TODO: Change so it only converts object to string
    if index_name and index_name in pd_df.columns:
        pd_df = pd_df.set_index(index_name)
    elif "__index_level_0__" in pd_df.columns:
        pd_df = pd_df.set_index("__index_level_0__")
    if pd_df.index.name == "__index_level_0__":
        pd_df.index.name = None

    if as_series:
        pd_df = pd_df.squeeze()
    return pd_df


def _convert_to_arrow(df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    cols = df.column_names
    if '__index_level_0__' in cols:
        first_field = df['__index_level_0__'][0].as_py()
        if isinstance(first_field, int):
            cols.remove('__index_level_0__')
    df = df.select(cols)
    return df


def _convert_to_polars(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        df = _convert_to_arrow(df)
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)
    return df
