import os

import pyarrow as pa
from pyarrow import feather
import pandas as pd
import polars as pl

from featherstore.connection import Connection
from featherstore import _metadata
from featherstore._metadata import Metadata
from featherstore._table import _raise_if


def can_read_table(cols, rows, table_path):
    Connection.is_connected()
    _raise_if.table_not_exists(table_path)

    _raise_if.rows_argument_is_not_supported_dtype(rows)
    _raise_if.rows_argument_items_dtype_not_same_as_index(rows, table_path)

    _raise_if.cols_argument_is_not_supported_dtype(cols)
    cols_are_provided = isinstance(cols, list)
    if cols_are_provided:
        _raise_if.cols_argument_items_is_not_str(cols)
        _raise_if.cols_not_in_table(cols, table_path)


def get_partition_names(rows, table_path):
    if rows:
        partition_names = _predicate_filtering(rows, table_path)
    else:
        partition_names = Metadata(table_path, 'partition').keys()
    return partition_names


def _predicate_filtering(rows, table_path):
    # TODO: Rework, make more effective (Binary search)
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
    # TODO: Rework or replace
    partition_stats = dict()
    partition_stats["name"] = Metadata(table_path, 'partition').keys()
    partition_stats["min"] = _metadata.get_partition_attr(table_path, 'min')
    partition_stats["max"] = _metadata.get_partition_attr(table_path, 'max')

    partition_stats = pd.DataFrame(partition_stats)
    partition_stats.set_index("name", inplace=True)
    return partition_stats


def read_partitions(partition_names, table_path, cols):
    if cols is not None:
        cols = _add_index_to_cols(cols, table_path)

    partitions = []
    for partition_name in partition_names:
        partition_path = os.path.join(table_path, f"{partition_name}.feather")
        partition = feather.read_table(partition_path,
                                       columns=cols,
                                       memory_map=True)
        partitions.append(partition)
    return partitions


def _add_index_to_cols(cols, table_path):
    index_col = Metadata(table_path, "table")["index_name"]
    cols = cols.copy()
    cols.append(index_col)
    return cols


def filter_table_rows(df, rows, index_col_name):
    should_be_filtered = rows is not None
    if should_be_filtered:
        df = _filter_arrow_table(df, rows, index_col_name)
    return df


def _filter_arrow_table(df, rows, index_col_name):
    keyword = str(rows[0]).lower()
    index = df[index_col_name]
    if keyword not in ('before', 'after', 'between'):
        df = _fetch_rows_in_list(df, index, rows)
    elif keyword == 'before':
        df = _fetch_rows_before(df, index, rows[1])
    elif keyword == 'after':
        df = _fetch_rows_after(df, index, rows[1])
    elif keyword == 'between':
        df = _fetch_rows_between(df, index, low=rows[1], high=rows[2])
    return df


def _fetch_rows_in_list(df, index, rows):
    rows_indices = pa.compute.index_in(rows, value_set=index)
    df = df.take(rows_indices)
    return df


def _fetch_rows_before(df, index, row):
    upper_bound = _compute_upper_bound(row, index)
    df = df[:upper_bound]
    return df


def _fetch_rows_after(df, index, row):
    lower_bound = _compute_lower_bound(row, index)
    df = df[lower_bound:]
    return df


def _fetch_rows_between(df, index, low, high):
    lower_bound = _compute_lower_bound(low, index)
    upper_bound = _compute_upper_bound(high, index)
    df = df[lower_bound:upper_bound]
    return df


def _compute_lower_bound(row, index):
    lower_bound = _fetch_row_idx(row, index) - 1
    return lower_bound


def _compute_upper_bound(row, index):
    upper_bound = _fetch_row_idx(row, index)
    return upper_bound


def _fetch_row_idx(row, index):
    row_idx = _fetch_exact_row_idx(row, index)

    no_row_idx_found = row_idx is None
    if no_row_idx_found:
        row_idx = _fetch_closest_row_idx(row, index)

    no_close_row_idx_found = row_idx is None
    if no_close_row_idx_found:
        row_idx = _fetch_last_row_idx(index)
    return row_idx


def _fetch_exact_row_idx(row, index):
    row_idx = pa.compute.index_in(row, value_set=index)
    row_idx = row_idx.as_py()
    if row_idx is not None:
        row_idx += 1
    return row_idx


def _fetch_closest_row_idx(row, index):
    TRUE = 1
    mask = pa.compute.less_equal(row, index)
    row_idx = pa.compute.index_in(TRUE, value_set=mask)
    row_idx = row_idx.as_py()
    return row_idx


def _fetch_last_row_idx(index):
    return len(index)


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
        corresponding_rangeindex = pd.RangeIndex(start=0, stop=len(df))
        is_equal_to_rangeindex = df.index.equals(corresponding_rangeindex)
        if is_equal_to_rangeindex:
            can_be_converted = True
        else:
            can_be_converted = False
    return can_be_converted


def make_rangeindex(df):
    index = pd.RangeIndex(len(df))
    index.name = df.index.name
    return index


def convert_table_to_polars(df):
    partitions = df.to_batches()
    for idx, partition in enumerate(partitions):
        partition = pa.Table.from_batches([partition])
        partitions[idx] = pl.from_arrow(partition, rechunk=False)
    # Rechunking introduces a significant performance penalty
    full_table = pl.concat(partitions, rechunk=False)
    return full_table
