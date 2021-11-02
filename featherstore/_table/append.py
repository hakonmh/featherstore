import os

import pyarrow as pa
import pandas as pd
import polars as pl

from .._metadata import Metadata
from .. import _utils
from .._utils import DEFAULT_ARROW_INDEX_NAME
from .common import _get_cols


def can_append_table(
    df,
    warnings,
    table_path,
    table_exists,
):
    _utils.check_if_arg_warnings_is_valid(warnings)

    if not table_exists:
        raise FileNotFoundError()

    if not isinstance(df, (pd.DataFrame, pd.Series, pl.DataFrame, pa.Table)):
        raise TypeError(f"'df' must be a DataFrame (is type {type(df)})")

    stored_data_cols = Metadata(table_path, "table")["columns"]
    has_default_index = Metadata(table_path, "table")["has_default_index"]
    append_data_cols = _get_cols(df, has_default_index)
    if sorted(append_data_cols) != sorted(stored_data_cols):
        raise ValueError("New and old columns doesn't match")

    append_data_start = _get_first_append_value(df, table_path, has_default_index)
    stored_data_end = _get_last_stored_value(table_path)
    if append_data_start <= stored_data_end:
        raise ValueError(
            f"New_data.index can't be <= old_data.index[-1] ({append_data_start}"
            f" <= {stored_data_end})"
        )


def format_default_index(df, table_path):
    """Formats the appended data's index to continue from where the stored
    data's index stops
    """
    first_value = _get_first_append_value(df, table_path, has_default_index=True)
    index_col = df[DEFAULT_ARROW_INDEX_NAME]
    formatted_index_col = pa.compute.add(index_col, first_value)

    index_col_position = Metadata(table_path, "table")["index_column_position"]
    df = df.remove_column(index_col_position)
    df = df.add_column(
        index_col_position, DEFAULT_ARROW_INDEX_NAME, formatted_index_col
    )
    return df


def _get_first_append_value(df, table_path, has_default_index):
    if has_default_index:
        stored_data_end = _get_last_stored_value(table_path)
        append_data_start = int(stored_data_end) + 1
    else:
        index_col = Metadata(table_path, "table")["index_name"]
        index_dtype = Metadata(table_path, "table")["index_dtype"]
        append_data_start = df[index_col][0].as_py()
        append_data_start = _format_index_value(append_data_start, index_dtype)
    return append_data_start


def _get_last_stored_value(table_path):
    stored_data_end = Metadata(table_path, "partition")["max"][-1]
    index_dtype = Metadata(table_path, "table")["index_dtype"]
    stored_data_end = _format_index_value(stored_data_end, index_dtype)
    return stored_data_end


def _format_index_value(value, index_dtype):
    if index_dtype == "datetime64":
        value = pd.Timestamp(value)
    elif index_dtype == "int64":
        value = int(value)
    return value


def sort_columns(df, table_metadata):
    columns = table_metadata["columns"]
    columns_not_sorted = df.column_names != columns
    if columns_not_sorted:
        df = df.select(columns)
    return df


def delete_last_partition(table_path, last_partition_name):
    partition_path = f"{table_path}/{last_partition_name}.feather"
    os.remove(partition_path)
