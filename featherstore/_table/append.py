import os

import pyarrow as pa
import pandas as pd
import polars as pl

from featherstore import _metadata
from featherstore._metadata import Metadata
from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from featherstore._table.common import (
    _get_cols,
    _convert_to_partition_id,
    _convert_partition_id_to_int
)


def can_append_table(
    df,
    warnings,
    table_path,
    table_exists,
):
    _utils.check_if_arg_warnings_is_valid(warnings)

    if not table_exists:
        raise FileNotFoundError("Table doesn't exist")

    if not isinstance(df, (pd.DataFrame, pd.Series, pl.DataFrame, pa.Table)):
        raise TypeError(f"'df' must be a DataFrame (is type {type(df)})")

    stored_data_cols = Metadata(table_path, "table")["columns"]
    has_default_index = Metadata(table_path, "table")["has_default_index"]
    append_data_cols = _get_cols(df, has_default_index)
    if sorted(append_data_cols) != sorted(stored_data_cols):
        raise ValueError("New and old columns doesn't match")

    append_data_start = _get_first_append_value(df, table_path, has_default_index)
    stored_data_end = _metadata.get_partition_attr(table_path, 'min')[-1]
    if append_data_start <= stored_data_end:
        raise ValueError(
            f"New_data.index can't be <= old_data.index[-1] ({append_data_start}"
            f" <= {stored_data_end})")


def format_default_index(df, table_path):
    """Formats the appended data's index to continue from where the stored
    data's index stops
    """
    first_value = _get_first_append_value(df,
                                          table_path,
                                          has_default_index=True)
    index_col = df[DEFAULT_ARROW_INDEX_NAME]
    formatted_index_col = pa.compute.add(index_col, first_value)

    index_col_position = Metadata(table_path, "table")["index_column_position"]
    df = df.remove_column(index_col_position)
    df = df.add_column(index_col_position, DEFAULT_ARROW_INDEX_NAME,
                       formatted_index_col)
    return df


def _get_first_append_value(df, table_path, has_default_index):
    if has_default_index:
        stored_data_end = _metadata.get_partition_attr(table_path, 'min')[-1]
        append_data_start = int(stored_data_end) + 1
    else:
        index_col = Metadata(table_path, "table")["index_name"]
        index_dtype = Metadata(table_path, "table")["index_dtype"]
        first_row = df[:1]

        # To enable selecting index using column selection
        if isinstance(first_row, (pd.DataFrame, pd.Series)):
            if first_row.index.name is None:
                first_row.index.name = DEFAULT_ARROW_INDEX_NAME
            first_row = first_row.reset_index()

        append_data_start = first_row[index_col]
        append_data_start = _extract_value(append_data_start)
        append_data_start = _format_index_value(append_data_start, index_dtype)
    return append_data_start


def _extract_value(value):
    if isinstance(value, pa.ChunkedArray):
        value, = value.to_pylist()
    elif isinstance(value, (pl.Series, pd.Series)):
        value, = value.to_list()
    else:
        raise TypeError("Couldn't convert first_append_value")
    return value


def _format_index_value(value, index_dtype):
    if hasattr(value, 'as_py'):
        value = value.as_py()
    if index_dtype == "datetime64":
        value = pd.Timestamp(value)
    return value


def sort_columns(df, columns):
    columns_not_sorted = df.column_names != columns
    if columns_not_sorted:
        df = df.select(columns)
    return df


def append_new_partition_ids(num_partitions, last_partition_id):
    partition_ids = [last_partition_id]
    range_start = _convert_partition_id_to_int(last_partition_id) + 1
    range_end = range_start + num_partitions
    for partition_num in range(range_start, range_end):
        partition_id = _convert_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids
