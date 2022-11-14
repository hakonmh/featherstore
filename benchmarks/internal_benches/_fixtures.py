import itertools
import string
import copy
import os

import pyarrow as pa
import polars as pl
import pandas as pd
import numpy as np

import featherstore as fs
from featherstore._utils import (
    filter_items_like_pattern,
    delete_folder_tree,
    DB_MARKER_NAME
)
from featherstore._table._indexers import RowIndexer
from featherstore._table._table_utils import filter_arrow_table
from featherstore._table.common import _format_pd_metadata
from featherstore.table import DEFAULT_PARTITION_SIZE

RANDS_CHARS = np.array(list(string.ascii_lowercase + ' '))


def delete_db():
    db_path = fs.current_db()
    items = os.listdir(db_path)
    if DB_MARKER_NAME in items:
        delete_folder_tree(db_path, db_path)
    else:
        raise RuntimeError("Not a database!")


def __make_float_col(rows):
    return np.random.random(size=rows)


def __make_uint_col(rows):
    return np.random.randint(0, 200000, size=rows)


def __make_int_col(rows):
    return np.random.randint(-100000, 100000, size=rows)


def __make_datetime_col(rows):
    start = -852076800  # 1943-01-01 in seconds relative to epoch
    end = 1640995200  # 2022-01-01 in seconds relative to epoch
    times_since_epoch = np.random.randint(start, end, size=rows, dtype=np.int32)
    dtime = times_since_epoch.astype('datetime64[s]')
    return dtime


def __make_string_col(rows):
    str_length = 5
    df = (
        np.random.choice(RANDS_CHARS, size=str_length * rows)
        .view((np.str_, str_length))
        .reshape(rows)
    )
    return df


def __make_bool_column(rows):
    return np.random.randint(0, 2, size=rows, dtype=bool)


COL_DTYPES = {
    'int': __make_int_col,
    'string': __make_string_col,
    'float': __make_float_col,
    'datetime': __make_datetime_col,
    'bool': __make_bool_column,
    'uint': __make_uint_col,
}


def make_table(shape=(30, 5), *, sorted=True, astype="arrow", dtype=None):
    rows, cols = shape

    if dtype:
        col_dtypes = itertools.cycle([COL_DTYPES[dtype]])
    else:
        col_dtypes = itertools.cycle(COL_DTYPES.values())

    data = dict()
    if sorted:
        data['index'] = pd.RangeIndex(rows)
    else:
        data['index'] = np.random.default_rng().permutation(rows)

    for col in range(cols):
        col_dtype = next(col_dtypes)
        data[f"c{col}"] = col_dtype(rows)
    if astype == 'pandas':
        df = pd.DataFrame.from_dict(data)
        df.set_index('index', inplace=True)
    elif astype in {'arrow', 'polars'}:
        df = pa.Table.from_pydict(data)
        df = _format_pd_metadata(df, 'index')
        if astype == 'polars':
            df = pl.from_arrow(df)
    return df


def get_partition_size(df, num_partitions=0):
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


def split_table(df, rows=None, cols=None, index_name='index', keep_index=False):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        df, other = split_pandas(df, rows, cols)
    elif isinstance(df, pa.Table):
        df, other = split_arrow(df, rows, cols, index_name, keep_index)
    elif isinstance(df, (pl.DataFrame, pl.Series)):
        df, other = split_polars(df, rows, cols, index_name, keep_index)
    return df, other


def split_pandas(df, rows, cols):
    both_rows_and_cols_are_provided = cols is not None and rows is not None
    if both_rows_and_cols_are_provided:
        raise AttributeError('Both cols and rows provided')
    elif rows is not None:
        df, other = _split_pd_rows(df, rows)
    elif cols is not None:
        df, other = _split_cols(df, cols)
    return df, other


def _split_pd_rows(df, rows):
    other = pa.Table.from_pandas(df, preserve_index=True)
    other = filter_arrow_table(other, RowIndexer(rows), df.index.name)
    other = other.to_pandas()

    mask = df.index.isin(other.index)
    df = df[~mask]
    return df, other


def split_arrow(df, rows, cols, index_name, keep_index):
    both_rows_and_cols_are_provided = cols is not None and rows is not None
    if both_rows_and_cols_are_provided:
        raise AttributeError('Both cols and rows provided')
    elif rows is not None:
        df, other = _split_pa_rows(df, rows, index_name)
    elif cols is not None:
        df, other = _split_cols(df, cols, index_name, keep_index)
    return df, other


def _split_pa_rows(df, rows, index_name):
    other = filter_arrow_table(df, RowIndexer(rows), index_name)

    mask = pa.compute.is_in(df[index_name], value_set=other[index_name])
    mask = pa.compute.invert(mask)
    df = df.filter(mask)
    return df, other


def split_polars(df, rows, cols, index_name, keep_index):
    both_rows_and_cols_are_provided = cols is not None and rows is not None
    if both_rows_and_cols_are_provided:
        raise AttributeError('Both cols and rows provided')
    elif rows is not None:
        df, other = _split_pl_rows(df, rows, index_name)
    elif cols is not None:
        df, other = _split_cols(df, cols, index_name, keep_index)
    return df, other


def _split_pl_rows(df, rows, index_name):
    other = df.to_arrow()
    other = filter_arrow_table(other, RowIndexer(rows), index_name)
    other = pl.DataFrame(other)

    mask = df[index_name].is_in(other[index_name])
    df = df.filter(~mask)
    return df, other


def _split_cols(df, cols, index_name=None, keep_index=False):
    df_cols = __get_cols(df)

    if isinstance(cols, dict):
        pattern = cols['like']
        cols = filter_items_like_pattern(df_cols, like=pattern)
    not_cols = [c for c in df_cols if c not in cols]
    if keep_index and not isinstance(df, (pd.DataFrame, pd.Series)):
        if index_name not in cols:
            cols.insert(0, index_name)
        if index_name not in not_cols:
            not_cols.insert(0, index_name)

    df, other = __split_cols(df, cols, not_cols)
    return df, other


def __get_cols(df):
    if isinstance(df, pa.Table):
        df_cols = df.column_names
    else:
        df_cols = df.columns
    return df_cols


def __split_cols(df, cols, not_cols):
    if isinstance(df, pa.Table):
        other = df.select(cols)
        df = df.select(not_cols)
    else:
        other = df[cols]
        df = df[not_cols]
    return df, other


def update_values(df, index_name='index'):
    df = copy.copy(df)
    col_names = df.column_names if isinstance(df, pa.Table) else df.columns
    for col_name in col_names:
        if col_name == index_name:
            continue
        col = df[col_name].to_numpy()
        col = _update_col(col)

        if isinstance(df, pa.Table):
            col_idx = col_names.index(col_name)
            df = df.set_column(col_idx, col_name, pa.array(col))
        elif isinstance(df, pl.DataFrame):
            col = pl.Series(col_name, col)
            df = df.with_column(col)
        elif isinstance(df, (pd.DataFrame, pd.Series)):
            df[col_name] = col
    return df


def _update_col(df):
    dtype = __get_dtype(df)
    make_col = COL_DTYPES[dtype]
    rows = df.shape[0]
    return make_col(rows)


def __get_dtype(df):
    dtype = df.dtype.kind

    if dtype == 'i':
        if df.min() < 0:
            dtype = 'int'
        else:
            dtype = 'uint'
    elif dtype == 'f':
        dtype = 'float'
    elif dtype == 'b':
        dtype = 'bool'
    elif dtype == 'O':
        dtype = 'string'
    elif dtype == 'M':
        dtype = 'datetime'

    return dtype


def change_dtype(df, to, index_name='index', cols=None):
    arrow_dtype = _convert_to_pa_dtype(to)
    schema = df.schema
    cols = cols if cols else schema.names
    for idx, field in enumerate(schema):
        if field.name in cols and field.name != index_name:
            field = field.with_type(arrow_dtype)
            schema = schema.set(idx, field)
    df = df.cast(schema)
    return df


def _convert_to_pa_dtype(dtype):
    if __is_valid_dtype(dtype):
        dtype = pa.from_numpy_dtype(dtype)
    return dtype


def __is_valid_dtype(item):
    try:
        pa.from_numpy_dtype(item)
        return True
    except Exception:
        return False
