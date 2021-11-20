import pandas as pd
import pyarrow as pa

from featherstore._metadata import Metadata
from featherstore._table.common import (
    _check_index_constraints,
    _check_column_constraints,
    _coerce_column_dtypes,
    _convert_to_partition_id,
    _convert_partition_id_to_int,
    _get_index_dtype
)


def can_insert_table(df, table_path, table_exists):
    if not table_exists:
        raise FileNotFoundError("Table doesn't exist")

    if not isinstance(df, (pd.DataFrame, pd.Series)):
        raise TypeError(
            f"'df' must be a pd.DataFrame or pd.Series (is type {type(df)})")

    _check_index_constraints(df.index)

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns

    index_name = Metadata(table_path, "table")['index_name']
    stored_data_cols = Metadata(table_path, "table")["columns"]
    stored_data_cols.remove(index_name)
    if sorted(cols) != sorted(stored_data_cols):
        raise ValueError("New and old columns doesn't match")

    # Take one row and converts it to pa.Table to check its arrow datatype
    first_row = df.head(1)
    if isinstance(first_row, pd.Series):
        first_row = first_row.to_frame()
    arrow_df = pa.Table.from_pandas(first_row, preserve_index=True)
    index_type = _get_index_dtype([arrow_df])
    stored_index_type = Metadata(table_path, "table")["index_dtype"]
    if index_type != stored_index_type:
        raise TypeError("Index types do not match")


def insert_data(old_df, *, to):
    if isinstance(to, pd.Series):
        new_data = to.to_frame()
    else:
        new_data = to
    old_df = old_df.to_pandas()
    _check_if_rows_not_in_old_data(old_df, new_data)
    new_data = new_data[old_df.columns]
    new_data = _coerce_column_dtypes(new_data, to=old_df)
    df = old_df.append(new_data)
    df = df.sort_index()
    return df


def _check_if_rows_not_in_old_data(old_df, df):
    index = df.index
    old_index = old_df.index
    rows_in_old_df = any(index.isin(old_index))
    if rows_in_old_df:
        raise ValueError(f"Some rows already in stored table")


def insert_new_partition_ids(num_partitions, preceding_id, ensuing_id):
    partition_ids = [preceding_id]

    preceding_id = _convert_partition_id_to_int(preceding_id)
    ensuing_id = _convert_partition_id_to_int(ensuing_id)
    insertion_range = ensuing_id - preceding_id
    num_partitions += 1
    increment = insertion_range / num_partitions

    for partition_num in range(1, num_partitions):
        partition_id = preceding_id + increment * partition_num
        partition_id = _convert_to_partition_id(partition_num)
        partition_ids.append(partition_id)

    partition_ids.append(ensuing_id)
    return partition_ids
