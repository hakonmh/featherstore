import os
import json
from numbers import Integral

import pyarrow as pa
import pandas as pd
from pyarrow import feather

from featherstore.connection import Connection
from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from featherstore._table import common
from featherstore._table import _raise_if
from featherstore._table import _table_utils


def can_write_table(table, df, index_name, partition_size, errors, warnings):
    Connection._raise_if_not_connected()
    _utils.raise_if_errors_argument_is_not_valid(errors)
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    if errors == 'raise':
        _raise_if.table_already_exists(table._table_path)

    _raise_if.df_is_not_supported_table_type(df)
    _raise_if_partition_size_is_not_int(partition_size)

    cols = _table_utils.get_col_names(df, has_default_index=False)
    _raise_if_index_argument_is_not_supported_dtype(index_name)
    _raise_if_provided_index_not_in_cols(index_name, cols)
    _raise_if.cols_argument_items_is_not_str(cols)
    _raise_if.col_names_contains_duplicates(cols)

    pd_index = _table_utils.get_pd_index_if_exists(df, index_name)
    _raise_if_index_is_not_supported_type(pd_index)
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


def _raise_if_index_is_not_supported_type(index):
    if index is not None:
        index_type = index.inferred_type
        if index_type not in {"integer", "datetime64", "string"}:
            raise TypeError(f"Table.index type must be either int, str or datetime "
                            f"(is type {index_type})")


def create_partitions(df, rows_per_partition, partition_names=None):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    if partition_names is None:
        partition_names = _make_partition_ids(partitions)
    partitions = _table_utils.assign_ids_to_partitions(partitions, partition_names)
    return partitions


def _make_partition_ids(partitioned_df):
    num_partitions = len(partitioned_df)
    partition_ids = list()
    for partition_num in range(1, num_partitions + 1):
        partition_id = _table_utils.convert_int_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids


def generate_metadata(df, partition_size, rows_per_partition):
    table_metadata = _make_table_metadata(df, partition_size, rows_per_partition)
    partition_metadata = common._make_partition_metadata(df)
    return table_metadata, partition_metadata


def _make_table_metadata(df, partition_size, rows_per_partition):
    df = tuple(df.values())

    metadata = {
        "num_rows": _get_num_rows(df),
        "num_columns": _get_num_cols(df),
        "num_partitions": len(df),
        "columns": _get_partitioned_df_col_names(df),
        "index_name": _table_utils.get_index_name(df[0]),
        "index_dtype": _table_utils.get_index_dtype(df[0]),
        "has_default_index": _has_default_index(df),
        "partition_size": int(partition_size),
        "rows_per_partition": rows_per_partition,
    }
    return metadata


def _get_partitioned_df_col_names(df):
    cols = df[0].schema.names
    return cols


def _get_num_rows(df):
    num_rows = 0
    for partition in df:
        num_rows += partition.num_rows
    return num_rows


def _get_num_cols(df):
    num_cols = df[0].num_columns
    return num_cols


def _has_default_index(df):
    index_name = _table_utils.get_index_name(df[0])
    has_index_name = index_name != DEFAULT_ARROW_INDEX_NAME
    if has_index_name or __index_was_sorted(df):
        has_default_index = False
    else:
        index = [batch[index_name] for batch in df]
        index = pa.concat_arrays(index)
        if __is_rangeindex(index):
            has_default_index = True
        else:
            has_default_index = False

    return has_default_index


def __index_was_sorted(df):
    featherstore_metadata = df[0].schema.metadata[b"featherstore"]
    metadata_dict = json.loads(featherstore_metadata)
    was_sorted = metadata_dict["sorted"]
    return was_sorted


def __is_rangeindex(index):
    """Compares the index against a equivalent range index"""
    rangeindex = __make_rangeindex(index)
    TYPES_NOT_MATCHING = pa.lib.ArrowNotImplementedError
    try:
        is_rangeindex = pa.compute.equal(index, rangeindex)
        is_rangeindex = pa.compute.all(is_rangeindex).as_py()
    except TYPES_NOT_MATCHING:
        is_rangeindex = False

    return is_rangeindex


def __make_rangeindex(index):
    """Makes a rangeindex with equal length to 'index'"""
    return pa.array(pd.RangeIndex(len(index)))


def write_metadata(table, metadata):
    table_metadata, partition_metadata = metadata
    table._table_data.write(table_metadata)
    table._partition_data.write(partition_metadata)


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
