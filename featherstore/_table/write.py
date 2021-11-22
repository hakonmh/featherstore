import os
import json
from numbers import Integral
from polars.lazy.functions import last

import pyarrow as pa
from pyarrow import feather
import pandas as pd
import polars as pl

from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from featherstore._table.common import (
    _get_cols,
    _check_column_constraints,
    _convert_to_partition_id,
    _get_index_dtype
)


def can_write_table(df, index, errors, warnings, partition_size, table_exists,
                    table_name):
    _utils.check_if_arg_errors_is_valid(errors)
    _utils.check_if_arg_warnings_is_valid(warnings)

    if not isinstance(df, (pd.DataFrame, pd.Series, pl.DataFrame, pa.Table)):
        raise TypeError(f"'df' must be a DataFrame (is type {type(df)})")

    if not isinstance(index, (str, type(None))):
        raise TypeError(
            f"'index' must be a str or None (is type {type(index)})")

    cols = _get_cols(df, has_default_index=False)
    if isinstance(index, str) and index not in cols:
        raise IndexError("'index' not in table columns")

    _check_column_constraints(cols)

    if not isinstance(partition_size, (Integral, type(None))):
        raise ValueError(
            f"'partition_size' must be int (is type {type(partition_size)})")

    if table_exists and errors == "raise":
        raise FileExistsError(f"{table_name} already exists")


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
        partitions = _replace_last_two_partitions(new_last_partition, partitions)
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
        partition_id = _convert_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids


def make_table_metadata(df, collected_data):
    df = tuple(df.values())
    partition_byte_size, partition_size_in_rows = collected_data
    index_name = _get_index_name(df)

    metadata = {
        "num_rows": _get_num_rows(df),
        "columns": _get_column_names(df),
        "num_columns": _get_num_cols(df),
        "num_partitions": len(df),
        "rows_per_partition": partition_size_in_rows,
        "partition_byte_size": int(partition_byte_size),
        "index_name": index_name,
        "index_column_position": _get_index_position(df, index_name),
        "index_dtype": _get_index_dtype(df),
        "has_default_index": _has_default_index(df, index_name),
    }
    return metadata


def _get_index_name(df):
    if isinstance(df, dict):
        partition = tuple(df.values())[0]
    else:
        partition = df[0]
    schema = partition.schema
    index_name, = schema.pandas_metadata["index_columns"]
    no_index_name = not isinstance(index_name, str)
    if no_index_name:
        index_name = "index"
    return index_name


def _get_column_names(df):
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
        rangeindex = pa.compute.sort_indices(index)
        IS_NOT_THE_SAME_TYPE = pa.lib.ArrowNotImplementedError
        try:
            is_rangeindex = all(pa.compute.equal(index, rangeindex))
        except IS_NOT_THE_SAME_TYPE:
            is_rangeindex = False

        if is_rangeindex:
            has_default_index = True
        else:
            has_default_index = False

    return has_default_index


def _index_was_sorted(df):
    featherstore_metadata = df[0].schema.metadata[b"featherstore"]
    metadata_dict = json.loads(featherstore_metadata)
    was_sorted = metadata_dict["sorted"]
    return was_sorted


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
