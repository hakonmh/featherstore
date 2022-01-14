import os
from numbers import Integral

import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._metadata import METADATA_FOLDER_NAME, Metadata
from featherstore._table import common
from featherstore._table import _table_utils


def table_not_exists(table_path):
    table_name = table_path.rsplit('/')[-1]
    if not os.path.exists(table_path):
        raise FileNotFoundError(f"Table {table_name} doesn't exist")


def table_already_exists(table_path):
    table_name = table_path.rsplit('/')[-1]
    if os.path.exists(table_path):
        raise OSError(f"A table with name {table_name} already exists")


def table_name_is_not_str(table_name):
    if not isinstance(table_name, str):
        raise TypeError(
            f"'table_name' must be a str, is type {type(table_name)}")


def table_name_is_forbidden(table_name):
    if table_name == METADATA_FOLDER_NAME:
        raise ValueError(f"Table name {METADATA_FOLDER_NAME} is forbidden")


def df_is_not_supported_table_dtype(df):
    if not isinstance(df, (pd.DataFrame, pd.Series, pl.DataFrame, pa.Table)):
        raise TypeError(f"'df' must be a supported DataFrame dtype (is type {type(df)})")


def df_is_not_pandas_table(df):
    if not isinstance(df, (pd.DataFrame, pd.Series)):
        raise TypeError(
            f"'df' must be a pd.DataFrame or pd.Series (is type {type(df)})")


def cols_argument_is_not_supported_dtype(cols):
    is_valid_col_format = isinstance(cols, (list, type(None)))
    if not is_valid_col_format:
        raise TypeError("'cols' must be either list or None")


def cols_argument_items_is_not_str(cols):
    col_elements_are_str = all(isinstance(item, str) for item in cols)
    if not col_elements_are_str:
        raise TypeError("Elements in 'cols' must be of type str")


def cols_does_not_match(df, table_path):
    stored_data_cols = Metadata(table_path, "table")["columns"]
    has_default_index = Metadata(table_path, "table")["has_default_index"]
    new_data_cols = _table_utils.get_col_names(df, has_default_index)

    if sorted(new_data_cols) != sorted(stored_data_cols):
        raise ValueError("New and old columns doesn't match")


def cols_not_in_table(cols, table_path):
    table_metadata = Metadata(table_path, 'table')
    stored_cols = table_metadata["columns"]

    cols = common.filter_cols_if_like_provided(cols, stored_cols)
    some_cols_not_in_stored_cols = set(cols) - set(stored_cols)
    if some_cols_not_in_stored_cols:
        raise IndexError("Trying to access a column not found in table")


def rows_argument_is_not_supported_dtype(rows):
    is_valid_row_format = isinstance(rows, (list, pd.Index, type(None)))
    if not is_valid_row_format:
        raise TypeError("'rows' must be either List, pd.Index or None")


def rows_argument_items_dtype_not_same_as_index(rows, table_path):
    index_dtype = Metadata(table_path, "table")["index_dtype"]
    if rows is not None and not _rows_dtype_matches_index(rows, index_dtype):
        raise TypeError("'rows' dtype doesn't match table index dtype")


def _rows_dtype_matches_index(rows, index_dtype):
    row = rows[-1]
    matches_dtime_idx = _check_if_row_and_index_is_temporal(row, index_dtype)
    matches_str_idx = _check_if_row_and_index_is_str(row, index_dtype)
    matches_int_idx = _check_if_row_and_index_is_int(row, index_dtype)

    row_type_matches_idx = matches_dtime_idx or matches_str_idx or matches_int_idx
    return row_type_matches_idx


def _check_if_row_and_index_is_temporal(row, index_dtype):
    if _table_utils.str_is_temporal_dtype(index_dtype):
        return _isinstance_temporal(row)
    return False


def _check_if_row_and_index_is_str(row, index_dtype):
    if _table_utils.str_is_string_dtype(index_dtype):
        return _isinstance_str(row)
    return False


def _check_if_row_and_index_is_int(row, index_dtype):
    if _table_utils.str_is_int_dtype(index_dtype):
        return _isinstance_int(row)
    return False


def _isinstance_temporal(obj):
    try:
        _ = pd.to_datetime(obj)
        is_temporal = True
    except Exception:
        is_temporal = False
    return is_temporal


def _isinstance_str(obj):
    try:
        is_str = pa.types.is_string(obj) or pa.types.is_large_string(obj)
    except AttributeError:
        is_str = isinstance(obj, str)
    return is_str


def _isinstance_int(obj):
    try:
        is_int = pa.types.is_integer(obj)
    except AttributeError:
        is_int = isinstance(obj, Integral)
    return is_int


def index_dtype_not_same_as_stored_index(df, table_path):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        index_type = str(pa.Array.from_pandas(df.index).type)
        stored_index_type = Metadata(table_path, "table")["index_dtype"]
        if index_type != stored_index_type:
            raise TypeError("New and old index types do not match")


def col_names_contains_duplicates(cols):
    cols = pd.Index(cols)
    if cols.has_duplicates:
        raise IndexError("Column names must be unique")


def col_names_are_forbidden(cols):
    cols = pd.Index(cols)
    if "like" in cols.str.lower():
        raise ValueError("df contains invalid column name 'like'")


def index_values_contains_duplicates(index):
    if index.has_duplicates:
        raise IndexError("Values in Table.index must be unique")


def index_is_not_supported_dtype(index):
    index_type = index.inferred_type
    if index_type not in {"integer", "datetime64", "string"}:
        raise TypeError("Table.index type must be either int, str or datetime")
