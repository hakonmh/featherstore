import os
import json
from numbers import Integral

import pyarrow as pa
import pandas as pd
from pyarrow import feather

from featherstore.connection import Connection
from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from featherstore._table import _raise_if
from featherstore._table import _table_utils
from featherstore._table._table_utils import _convert_int_to_partition_id


def can_write_table(df, table_path, index_name, partition_size, errors, warnings):
    Connection.is_connected()
    _utils.raise_if_errors_argument_is_not_valid(errors)
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    if errors == 'raise':
        _raise_if.table_already_exists(table_path)

    _raise_if.df_is_not_supported_table_dtype(df)
    _raise_if_partition_size_is_not_int(partition_size)

    cols = _table_utils._get_col_names(df, has_default_index=False)
    _raise_if_index_argument_is_not_supported_dtype(index_name)
    _raise_if_provided_index_not_in_cols(index_name, cols)
    _raise_if.column_names_are_forbidden(cols)

    pd_index = _table_utils._get_index_col_as_pd_index(df, index_name)
    index_is_provided = pd_index is not None
    if index_is_provided:
        _raise_if.index_is_not_supported_dtype(pd_index)
        _raise_if.index_values_contains_duplicates(pd_index)


def _raise_if_partition_size_is_not_int(partition_size):
    if not isinstance(partition_size, (Integral, type(None))):
        dtype = type(partition_size)
        raise TypeError(f"'partition_size' must be a int (is type {dtype})")


def _raise_if_index_argument_is_not_supported_dtype(index):
    if not isinstance(index, (str, type(None))):
        raise TypeError(
            f"'index' must be a str or None (is type {type(index)})")


def _raise_if_provided_index_not_in_cols(index, cols):
    if isinstance(index, str) and index not in cols:
        raise IndexError("'index' not in table columns")


def calculate_rows_per_partition(df, target_size):
    number_of_rows = df.shape[0]
    table_size_in_bytes = df.nbytes
    row_group_size = int(number_of_rows * target_size / table_size_in_bytes)
    return row_group_size


def make_partitions(df, partition_size):
    df = df.combine_chunks()
    partitions = df.to_batches(partition_size)
    partitions = _combine_small_partitions(partitions, partition_size)
    return partitions


def _combine_small_partitions(partitions, partition_size):
    has_multiple_partitions = len(partitions) > 1
    size_of_last_partition = partitions[-1].num_rows
    min_partition_size = partition_size * 0.5

    if has_multiple_partitions and size_of_last_partition < min_partition_size:
        new_last_partition = _combine_last_two_partitions(partitions)
        partitions = _replace_last_two_partitions(new_last_partition,
                                                  partitions)
    return partitions


def _combine_last_two_partitions(partitions):
    last_partition = pa.Table.from_batches(partitions[-2:])
    last_partition = last_partition.combine_chunks()
    return last_partition.to_batches()


def _replace_last_two_partitions(new_last_partition, partitions):
    partitions = partitions[:-2]
    partitions.extend(new_last_partition)
    return partitions


def make_partition_ids(partitioned_df):
    num_partitions = len(partitioned_df)
    partition_ids = list()
    for partition_num in range(1, num_partitions + 1):
        partition_id = _convert_int_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids


def make_table_metadata(df, collected_data):
    df = tuple(df.values())
    partition_byte_size, partition_size_in_rows = collected_data

    index_name = _table_utils._get_index_name(df[0])

    metadata = {
        "num_rows": _get_num_rows(df),
        "columns": _get_col_names(df),
        "num_columns": _get_num_cols(df),
        "num_partitions": len(df),
        "rows_per_partition": partition_size_in_rows,
        "partition_byte_size": int(partition_byte_size),
        "index_name": index_name,
        "index_column_position": _get_index_position(df, index_name),
        "index_dtype": _table_utils._get_index_dtype(df[0]),
        "has_default_index": _has_default_index(df, index_name),
    }
    return metadata


def _get_col_names(df):
    schema = df[0].schema
    cols = schema.names
    return cols


def _get_num_rows(df):
    num_rows = 0
    for partition in df:
        num_rows += partition.num_rows
    return num_rows


def _get_num_cols(df):
    num_cols = df[0].num_columns
    return num_cols


def _get_index_position(df, index_name):
    schema = df[0].schema
    index_position = schema.get_field_index(index_name)
    return index_position


def _has_default_index(df, index_name):
    has_index_name = index_name != DEFAULT_ARROW_INDEX_NAME
    if has_index_name or _index_was_sorted(df):
        has_default_index = False
    else:
        index = pa.Table.from_batches(df)[index_name]
        if _is_rangeindex(index):
            has_default_index = True
        else:
            has_default_index = False

    return has_default_index


def _index_was_sorted(df):
    featherstore_metadata = df[0].schema.metadata[b"featherstore"]
    metadata_dict = json.loads(featherstore_metadata)
    was_sorted = metadata_dict["sorted"]
    return was_sorted


def _is_rangeindex(index):
    """Compares the index against a equivalent range index"""
    rangeindex = _make_rangeindex(index)
    TYPES_NOT_MATCHING = pa.lib.ArrowNotImplementedError
    try:
        is_rangeindex = pa.compute.equal(index, rangeindex)
        is_rangeindex = pa.compute.all(is_rangeindex).as_py()
    except TYPES_NOT_MATCHING:
        is_rangeindex = False

    return is_rangeindex


def _make_rangeindex(index):
    """Makes a rangeindex with equal length to 'index'"""
    return pa.array(pd.RangeIndex(len(index)))


def write_partitions(partitions, table_path):
    for file_name, partition in partitions.items():
        partition = pa.Table.from_batches([partition])
        file_path = os.path.join(table_path, f"{file_name}.feather")
        _write_feather(partition, file_path)


def _write_feather(df, file_path):
    CHUNKSIZE = 128 * 1024**2  # bytes
    feather.write_feather(df,
                          file_path,
                          compression="uncompressed",
                          chunksize=CHUNKSIZE)
