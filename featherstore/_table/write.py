import os
from numbers import Integral

import pyarrow as pa
from pyarrow import feather
import pandas as pd
import polars as pl

from featherstore import _utils
from featherstore._table.common import (
    _get_cols,
    _check_column_constraints,
    _convert_to_partition_id,
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
    return partitions


def make_partition_ids(num_partitions):
    partition_ids = list()
    for partition_num in range(1, num_partitions + 1):
        partition_id = _convert_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids


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
