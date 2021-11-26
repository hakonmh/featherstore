import os

import pyarrow as pa
from pyarrow import feather
import pandas as pd
import polars as pl

from featherstore._metadata import Metadata, get_partition_attr
from featherstore._table.common import _rows_dtype_matches_index, format_cols


def can_read_table(cols, rows, table_exists, table_metadata):
    if not table_exists:
        raise FileNotFoundError("Table doesn't exist")

    is_valid_col_format = isinstance(cols, (list, type(None)))
    if not is_valid_col_format:
        raise TypeError("'cols' must be either list or None")

    stored_columns = table_metadata["columns"]
    cols_are_provided = isinstance(cols, list)
    if cols_are_provided:
        col_elements_are_str = all(isinstance(item, str) for item in cols)
        if not col_elements_are_str:
            raise TypeError("Elements in 'cols' must be of type str")

        cols = format_cols(cols, stored_columns)
        some_cols_not_in_stored_cols = set(cols) - set(stored_columns)
        if some_cols_not_in_stored_cols:
            raise IndexError("Trying to read a column not found in table")

    is_valid_row_format = isinstance(rows, (list, pd.Index, type(None)))
    if not is_valid_row_format:
        raise TypeError("'rows' must be either List, or None")

    index_dtype = table_metadata["index_dtype"]
    if rows and not _rows_dtype_matches_index(rows, index_dtype):
        raise TypeError("'rows' type doesn't match table index")


def get_partition_names(rows, table_path):
    if rows:
        partition_names = _predicate_filtering(rows, table_path)
    else:
        partition_names = Metadata(table_path, 'partition').keys()
    return partition_names


def _predicate_filtering(rows, table_path):
    partition_stats = _get_partition_stats(table_path)
    keyword = str(rows[0]).lower()
    if keyword == "before":
        mask = rows[1] >= partition_stats
        mask = mask.max(axis=1)
    elif keyword == "after":
        mask = rows[1] <= partition_stats
        mask = mask.max(axis=1)
    elif keyword == "between":
        lower_bound = rows[1] <= partition_stats["max"]
        higher_bound = rows[2] >= partition_stats["min"]
        mask = higher_bound & lower_bound
    else:  # When a list of rows is provided
        max_ = max(rows)
        min_ = min(rows)
        lower_bound = min_ <= partition_stats["max"]
        higher_bound = max_ >= partition_stats["min"]
        mask = higher_bound & lower_bound

    filtered_partition_stats = partition_stats[mask]
    partition_names = filtered_partition_stats.index.tolist()
    return partition_names


def _get_partition_stats(table_path):
    index_type = Metadata(table_path, "table")["index_dtype"]

    partition_stats = dict()
    partition_stats["name"] = Metadata(table_path, 'partition').keys()
    partition_stats["min"] = get_partition_attr(table_path, 'min')
    partition_stats["max"] = get_partition_attr(table_path, 'max')

    partition_stats = pd.DataFrame(partition_stats)
    partition_stats.set_index("name", inplace=True)
    for col in partition_stats:
        partition_stats[col] = partition_stats[col].astype(index_type)
    return partition_stats


def read_partitions(partition_names, table_path, columns):
    if columns is not None:
        index_col = Metadata(table_path, "table")["index_name"]
        columns = columns.copy()
        columns.append(index_col)

    partitions = []
    for partition_name in partition_names:
        partition_path = os.path.join(table_path, f"{partition_name}.feather")
        partition = feather.read_table(partition_path,
                                       columns=columns,
                                       memory_map=True)
        partitions.append(partition)
    return partitions


def filter_table_rows(df, rows, index_col_name):
    should_be_filtered = rows is not None
    if should_be_filtered:
        df = _filter_arrow_table(df, index_col_name, rows)
    return df


def _filter_arrow_table(df, index_col_name, rows):
    keyword = str(rows[0]).lower()
    index = df[index_col_name]
    if keyword == 'before':
        upper_bound = _compute_row_index(rows[1], index)
        df = df[:upper_bound + 1]
    elif keyword == 'after':
        lower_bound = _compute_row_index(rows[1], index)
        df = df[lower_bound:]
    elif keyword == 'between':
        lower_bound = _compute_row_index(rows[1], index)
        upper_bound = _compute_row_index(rows[2], index)
        df = df[lower_bound:upper_bound + 1]
    else:  # When a list of rows is provided
        rows_indices = pa.compute.index_in(rows, value_set=index)
        df = df.take(rows_indices)
    return df


def _compute_row_index(row, index):
    row_idx = pa.compute.index_in(row, value_set=index)
    row_idx = row_idx.as_py()
    no_row_is_matching = row_idx is None
    if no_row_is_matching:
        row_idx = _fetch_closest_row(row, index)
    return row_idx


def _fetch_closest_row(row, index):
    TRUE = 1
    mask = pa.compute.less_equal(row, index)
    row_idx = pa.compute.index_in(TRUE, value_set=mask)
    row_idx = row_idx.as_py()

    row_not_in_index = row_idx is None
    if row_not_in_index:
        # Set row_idx to the end of index
        row_idx = len(index)

    return row_idx - 1


def drop_default_index(df, index_col_name):
    df = df.drop([index_col_name])
    return df


def can_be_converted_to_series(df):
    num_cols = df.shape[1]
    return num_cols == 1


def can_be_converted_to_rangeindex(df):
    is_already_rangeindex = isinstance(df.index, pd.RangeIndex)
    if is_already_rangeindex:
        can_be_converted = False
    else:
        corresponding_rangeindex = pd.RangeIndex(start=0, stop=len(df.index))
        is_equal_to_rangeindex = df.index.equals(corresponding_rangeindex)
        if is_equal_to_rangeindex:
            can_be_converted = True
        else:
            can_be_converted = False
    return can_be_converted


def convert_to_rangeindex(df):
    index_name = df.index.name
    df = df.reset_index(drop=True)
    df.index.name = index_name
    return df


def convert_table_to_polars(df):
    partitions = df.to_batches()
    for idx, partition in enumerate(partitions):
        partition = pa.Table.from_batches([partition])
        partitions[idx] = pl.from_arrow(partition, rechunk=False)
    # Rechunking introduces a significant performance penalty
    full_table = pl.concat(partitions, rechunk=False)
    return full_table
