from numbers import Integral

import pyarrow as pa
import pandas as pd
import polars as pl

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

PARTITION_NAME_LENGTH = 14
INSERTION_BUFFER_LENGTH = 10**6


def _get_cols(df, has_default_index):
    if isinstance(df, pd.DataFrame):
        cols = df.columns.tolist()
        if df.index.name is not None:
            cols.append(df.index.name)
        else:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pd.Series):
        cols = [df.name]
        if df.index.name is not None:
            cols.append(df.index.name)
        else:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pa.Table):
        cols = df.column_names
        if has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pl.DataFrame):
        cols = df.columns
        if has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    return cols


def _rows_dtype_matches_index(rows, index_dtype):
    try:
        _convert_row(rows[-1], to=index_dtype)
        row_type_matches = True
    except TypeError:
        row_type_matches = False
    return row_type_matches


def _convert_row(row, *, to):
    if to == "datetime64":
        try:
            row = pd.to_datetime(row)
        except Exception:
            raise TypeError("'row' dtype doesn't match index dtype")
    elif to == "string" or to == "unicode":
        if not isinstance(row, str):
            raise TypeError("'row' dtype doesn't match index dtype")
        row = str(row)
    elif to == "int64":
        if not isinstance(row, Integral):
            raise TypeError("'row' dtype doesn't match index dtype")
        row = int(row)
    return row


def _coerce_column_dtypes(df, *, to):
    cols = df.columns
    dtypes = to[cols].dtypes
    try:
        df = df.astype(dtypes)
    except ValueError:
        raise TypeError("New and old column dtypes do not match")
    return df


def _convert_to_partition_id(partition_id):
    partition_id = int(partition_id * INSERTION_BUFFER_LENGTH)
    format_string = f'0{PARTITION_NAME_LENGTH}d'
    partition_id = format(partition_id, format_string)
    return partition_id


def _convert_partition_id_to_int(partition_id):
    return int(partition_id) // INSERTION_BUFFER_LENGTH


def _get_index_dtype(df):
    schema = df[0].schema
    # TODO: A better solution for when format_table is refactored:
    # str(df[0].field(index_position).type)
    index_dtype = schema.pandas_metadata["columns"][-1]["pandas_type"]
    if index_dtype == "datetime":
        index_dtype = "datetime64"
    return index_dtype


def _convert_to_pyarrow_table(df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    return df
