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

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns

    _check_index_constraints(df.index)
    _check_column_constraints(cols)

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


def insert_new_partition_ids(partitioned_df, partition_names):
    INSERTION_ID_RANGE = 1
    num_partitions = len(partitioned_df)
    num_partition_names = len(partition_names)

    number_of_new_names_to_make = num_partitions - num_partition_names + 1
    increment = INSERTION_ID_RANGE / number_of_new_names_to_make
    last_partition_id = _convert_partition_id_to_int(partition_names[-1])

    new_partition_names = partition_names.copy()
    for partition_num in range(1, number_of_new_names_to_make):
        partition_id = last_partition_id + increment * partition_num
        partition_id = _convert_to_partition_id(partition_id)
        new_partition_names.append(partition_id)

    return sorted(new_partition_names)
