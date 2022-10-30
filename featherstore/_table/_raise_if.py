import os
from numbers import Integral

import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._metadata import METADATA_FOLDER_NAME
from featherstore._table import _table_utils
from featherstore._table._indexers import ColIndexer


def table_not_exists(table):
    if not table.exists():
        raise FileNotFoundError(f"Table '{table.name}' not found")


def table_already_exists(table_path):
    table_name = table_path.rsplit('/')[-1]
    if os.path.exists(table_path):
        raise FileExistsError(f"A table with name '{table_name}' already exists")


def table_name_is_not_str(table_name):
    if not isinstance(table_name, str):
        raise TypeError(
            f"'table_name' must be a str (is type {type(table_name)})")


def table_name_is_forbidden(table_name):
    if table_name == METADATA_FOLDER_NAME:
        raise ValueError(f"Table name '{METADATA_FOLDER_NAME}' is forbidden")


def df_is_not_supported_table_type(df):
    if not isinstance(df, (pd.DataFrame, pd.Series, pl.DataFrame, pa.Table)):
        raise TypeError(f"'df' must be a supported DataFrame type (is type {type(df)})")


def df_is_not_pandas_table(df):
    if not isinstance(df, (pd.DataFrame, pd.Series)):
        raise TypeError(f"'df' must be a pd.DataFrame or pd.Series (is type {type(df)})")


def rows_argument_is_not_collection(rows):
    is_collection_or_none = _table_utils.is_collection(rows)
    if not is_collection_or_none:
        raise TypeError(f"'rows' must be a collection (is type {type(rows)})")


def rows_argument_is_not_collection_or_none(rows):
    is_collection_or_none = _table_utils.is_collection(rows) or rows is None
    if not is_collection_or_none:
        raise TypeError(f"'rows' must be a collection or None (is type {type(rows)})")


def to_argument_is_not_list_like(to):
    is_list_like = _table_utils.is_list_like(to)
    if not is_list_like:
        raise TypeError(f"'to' must be list like (is type {type(to)})")


def cols_argument_is_not_list_like(cols):
    is_list_like = _table_utils.is_list_like(cols)
    if not is_list_like:
        raise TypeError(f"'cols' must be list like (is type {type(cols)})")


def cols_argument_is_not_collection(cols):
    is_collection = _table_utils.is_collection(cols)
    if not is_collection:
        raise TypeError(f"'cols' must be a collection (is type {type(cols)})")


def cols_argument_is_not_collection_or_none(cols):
    is_collection_or_none = _table_utils.is_collection(cols) or cols is None
    if not is_collection_or_none:
        raise TypeError(f"'cols' must be a collection or None (is type {type(cols)})")


def cols_argument_items_is_not_str(cols):
    if isinstance(cols, dict):
        col_elements_are_str = all(isinstance(item, str) for item in cols.keys())
    else:
        col_elements_are_str = all(isinstance(item, str) for item in cols)
    if not col_elements_are_str:
        raise TypeError("Elements in 'cols' must be of type str")


def length_of_cols_and_to_doesnt_match(cols, to):
    if len(cols) != len(to):
        raise ValueError(f"Length of 'cols' != length of 'to' ({len(cols)} != {len(to)})")


def cols_does_not_match(df, table_data):
    stored_data_cols = table_data["columns"]
    has_default_index = table_data["has_default_index"]
    new_data_cols = _table_utils.get_col_names(df, has_default_index)

    if sorted(new_data_cols) != sorted(stored_data_cols):
        raise ValueError("New and old columns doesn't match")


def cols_not_in_table(cols, table_data):
    stored_cols = table_data["columns"]
    if not isinstance(cols, ColIndexer):
        cols = ColIndexer(cols)

    cols = cols.like(stored_cols)
    some_cols_not_in_stored_cols = set(cols) - set(stored_cols)
    if some_cols_not_in_stored_cols:
        raise IndexError("Trying to access a column not found in table")


def to_is_provided_twice(cols, to):
    cols_is_dict = isinstance(cols, dict)
    to_is_provided = to is not None
    if cols_is_dict and to_is_provided:
        raise AttributeError(r"'to' is provided twice, use either 'cols={<COL>: <TO>, ...}"
                             "to=None' or 'cols=[<COL>, ...], to=[<TO>, ...]'")


def to_not_provided(cols, to):
    cols_is_dict = isinstance(cols, dict)
    to_is_provided = to is not None
    if not cols_is_dict and not to_is_provided:
        raise AttributeError("'to' is not provided")


def rows_items_not_all_same_type(rows):
    try:
        rows = rows.values()
        if rows is not None:
            pa.array(rows)
    except Exception:
        raise TypeError("'rows' items not all of same type")


def rows_argument_items_type_not_same_as_index(rows, table_data):
    index_dtype = table_data["index_dtype"]
    if rows:
        if not _rows_type_matches_index(rows, index_dtype):
            raise TypeError("'rows' type doesn't match table index dtype")


def _rows_type_matches_index(rows, index_dtype):
    row = rows[0]
    matches_dtime_idx = _check_if_row_and_index_is_temporal(row, index_dtype)
    matches_str_idx = _check_if_row_and_index_is_str(row, index_dtype)
    matches_int_idx = _check_if_row_and_index_is_int(row, index_dtype)

    row_type_matches_idx = matches_dtime_idx or matches_str_idx or matches_int_idx
    return row_type_matches_idx


def _check_if_row_and_index_is_temporal(row, index_dtype):
    if _table_utils.typestring_is_temporal(index_dtype):
        return _isinstance_temporal(row)
    return False


def _check_if_row_and_index_is_str(row, index_dtype):
    if _table_utils.typestring_is_string(index_dtype):
        return _isinstance_str(row)
    return False


def _check_if_row_and_index_is_int(row, index_dtype):
    if _table_utils.typestring_is_int(index_dtype):
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


def index_type_not_same_as_stored_index(df, table_data):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        index_type = str(pa.Array.from_pandas(df.index).type)
        stored_index_type = table_data["index_dtype"]
        if index_type != stored_index_type:
            raise TypeError("New and old index types do not match")


def index_name_not_same_as_stored_index(df, table_data):
    stored_index_name = table_data['index_name']
    has_default_index = table_data["has_default_index"]
    cols = _table_utils.get_col_names(df, has_default_index=has_default_index)
    if stored_index_name not in cols:
        raise ValueError("New and old index names do not match")


def index_in_cols(cols, table_data):
    index_name = table_data["index_name"]
    if index_name in cols:
        raise ValueError("Index name in 'cols'")


def col_names_contains_duplicates(cols):
    cols = pd.Index(cols)
    if cols.has_duplicates:
        raise IndexError("Column names must be unique")


def index_values_contains_duplicates(index):
    if index is not None:
        if index.has_duplicates:
            raise IndexError("Index values must be unique")
