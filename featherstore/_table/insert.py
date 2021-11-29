import pandas as pd

from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._table import _table_utils


def can_insert_table(df, table_path):
    Connection.is_connected()

    _raise_if.table_not_exists(table_path)
    _raise_if.df_is_not_pandas_table(df)

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns.tolist()

    _raise_if.index_values_contains_duplicates(df.index)
    _raise_if.index_is_not_supported_dtype(df.index)
    _raise_if.column_names_are_forbidden(cols)
    _raise_if.index_dtype_not_same_as_index(df, table_path)
    _raise_if.columns_does_not_match(df, table_path)


def insert_data(old_df, *, to):
    if isinstance(to, pd.Series):
        new_data = to.to_frame()
    else:
        new_data = to
    old_df = old_df.to_pandas()
    _raise_if_rows_in_old_data(old_df, new_data)
    new_data = new_data[old_df.columns]
    new_data = _table_utils._coerce_column_dtypes(new_data, to=old_df)
    df = old_df.append(new_data)
    df = df.sort_index()
    return df


def _raise_if_rows_in_old_data(old_df, df):
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
    last_partition_id = _table_utils._convert_partition_id_to_int(partition_names[-1])

    new_partition_names = partition_names.copy()
    for partition_num in range(1, number_of_new_names_to_make):
        partition_id = last_partition_id + increment * partition_num
        partition_id = _table_utils._convert_to_partition_id(partition_id)
        new_partition_names.append(partition_id)

    return sorted(new_partition_names)
