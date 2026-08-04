import os
import platform

import pyarrow as pa
from pyarrow import ipc
import pandas as pd
import polars as pl

from featherstore.connection import Connection
from featherstore._metadata import Metadata
from featherstore._table import _raise_if
from featherstore._table import _table_utils
from featherstore._table._indexers import ColIndexer, RowIndexer


def can_read_table(table, cols, rows, mmap):
    Connection._raise_if_not_connected()
    _raise_if.table_not_exists(table)

    _raise_if_mmap_is_not_bool_or_none(mmap)

    _raise_if.rows_argument_is_not_collection_or_none(rows)
    rows = RowIndexer(rows)
    _raise_if.rows_items_not_all_same_type(rows)
    _raise_if.rows_argument_items_type_not_same_as_index(rows, table._table_data)

    _raise_if.cols_argument_is_not_collection_or_none(cols)
    cols = ColIndexer(cols)
    if cols:
        _raise_if.cols_argument_items_is_not_str_or_none(cols.values())
        _raise_if.cols_not_in_table(cols, table._table_data)


def _raise_if_mmap_is_not_bool_or_none(mmap):
    is_bool_or_none = isinstance(mmap, bool) or mmap is None
    if not is_bool_or_none:
        raise ValueError(f"'mmap' must be a bool or None (is {type(mmap)})")


def get_partition_names(table, rows):
    partition_data = table._partition_data
    if rows is None:
        rows = RowIndexer(None)

    partition_names = partition_data.keys()
    if rows.values():
        partition_names = _predicate_filtering(rows, partition_names, partition_data)
    return partition_names


def _predicate_filtering(rows, partition_names, partition_data):
    if rows.keyword == "before":
        start = 0
        target = rows[0]
        end = _binary_search(target, partition_names, partition_data)
    elif rows.keyword == "after":
        target = rows[0]
        start = _binary_search(target, partition_names, partition_data)
        end = len(partition_names)
    elif rows.keyword == "between":
        target_start = rows[0]
        target_end = rows[1]
        start = _binary_search(target_start, partition_names, partition_data)
        end = _binary_search(target_end, partition_names, partition_data)
    else:  # When a list of rows is provided
        start = _binary_search(min(rows.values()), partition_names, partition_data)
        end = _binary_search(max(rows.values()), partition_names, partition_data)

    partition_names = partition_names[start:end + 1]
    return partition_names


def _binary_search(target, partition_names, partition_data):
    possible_partition_names = partition_names

    while len(possible_partition_names) > 1:
        mid = len(possible_partition_names) // 2
        candidate_name = possible_partition_names[mid]
        candidate = partition_data[candidate_name]

        if _row_inside_candidate(target, candidate):
            break
        if _row_before_candidate(target, candidate):
            possible_partition_names = possible_partition_names[mid:]
        elif _row_after_candidate(target, candidate):
            possible_partition_names = possible_partition_names[:mid]
    else:
        candidate_name = possible_partition_names[0]

    return partition_names.index(candidate_name)


def _row_inside_candidate(target, candidate):
    candidate_min = candidate['min']
    candidate_max = candidate['max']
    return target <= candidate_max and target >= candidate_min


def _row_before_candidate(target, candidate):
    return target >= candidate['max']


def _row_after_candidate(target, candidate):
    return target <= candidate['min']


def read_table(table, partition_names, cols=ColIndexer(None),
               rows=RowIndexer(None), mmap=None):
    index_name = table._table_data["index_name"]
    if cols.values() is None:
        cols = ColIndexer(table._table_data["columns"])
    dfs = _read_partitions(partition_names, table._table_path, cols, mmap)
    df = _combine_partitions(dfs)
    df = _filter_table_rows(df, rows, index_name)
    return df


def _read_partitions(partition_names, table_path, cols, mmap):
    cols = __add_index_to_cols(cols, table_path)

    partitions = []
    for partition_name in partition_names:
        partition_path = os.path.join(table_path, f"{partition_name}.feather")
        partition = __read_feather(partition_path, cols, mmap)
        partitions.append(partition)
    return partitions


def __add_index_to_cols(cols, table_path):
    index_col = Metadata(table_path, "table")["index_name"]
    if index_col not in cols:
        cols.insert(0, index_col)
    return cols


def __read_feather(path, cols, mmap):
    use_mmap = mmap if mmap is not None else platform.system() != "Windows"
    table = _read_ipc_table(path, use_mmap)
    return table.select(cols.values())


def _read_ipc_table(path, use_mmap):
    if use_mmap:
        source = pa.memory_map(path, 'r')
    else:
        source = pa.OSFile(path, 'r')
    try:
        return ipc.open_file(source).read_all()
    finally:
        source.close()


def _combine_partitions(partitions):
    full_table = pa.concat_tables(partitions)
    return full_table


def _filter_table_rows(df, rows, index_col_name):
    should_be_filtered = rows.values() is not None
    if should_be_filtered:
        df = _table_utils.filter_arrow_table(df, rows, index_col_name)
    return df


def drop_default_index(df, index_col_name):
    df = df.drop([index_col_name])
    return df


def convert_table_to_pandas(df):
    was_transposed = _table_utils.is_transposed(df)
    df = df.to_pandas(date_as_object=False)
    if was_transposed:
        df = df.T

    if _can_be_converted_to_series(df):
        df = df.squeeze(axis=1)
        if df.name == '0':
            df.name = None

    index = df.index
    if _can_be_converted_to_rangeindex(index):
        df.index = _make_rangeindex(df)
    elif isinstance(index, pd.DatetimeIndex):
        df.index.freq = index.inferred_freq
    return df


def _can_be_converted_to_series(df):
    num_cols = df.shape[1]
    return num_cols == 1


def _can_be_converted_to_rangeindex(index):
    if isinstance(index, pd.RangeIndex):
        return False
    corresponding_rangeindex = pd.RangeIndex(start=0, stop=len(index))
    return index.equals(corresponding_rangeindex)


def _make_rangeindex(df):
    index = pd.RangeIndex(len(df))
    index.name = df.index.name
    return index


def convert_table_to_polars(df):
    full_table = pl.from_arrow(df, rechunk=False)
    num_cols = full_table.shape[1]
    if num_cols == 1:
        full_table = full_table.to_series()
        series_name = full_table.name
        if series_name == '0':
            full_table = pl.Series('', full_table)
    return full_table
